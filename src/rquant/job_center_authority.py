"""Content-bound authority manifest for the formal Strategy Lab runtime."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path
from typing import Literal, Self, TypeAlias
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class JobCenterAuthorityIntegrityError(RuntimeError):
    """The installed authority cannot prove its configured physical generation."""


class _AuthorityModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, revalidate_instances="always")


class AuthorityPathIdentity(_AuthorityModel):
    path: Path
    kind: Literal["directory", "file"]
    device: int = Field(ge=0)
    inode: int = Field(gt=0)
    mode: int

    @field_validator("path")
    @classmethod
    def validate_path(cls, value: Path) -> Path:
        return _absolute_path(value, label="authority")


AuthorityName: TypeAlias = Literal[
    "job_center",
    "definition_registry",
    "experiment_registry",
    "artifact_catalog",
]


class AuthorityGenerationBinding(_AuthorityModel):
    name: AuthorityName
    identities: tuple[AuthorityPathIdentity, ...]
    generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_generation(self) -> Self:
        if not self.identities:
            raise ValueError("authority generation requires physical identities")
        expected = _canonical_hash(
            {
                "contract": "job-center-authority-generation/v1",
                "name": self.name,
                "identities": [identity.model_dump(mode="json") for identity in self.identities],
            }
        )
        if self.generation_id != expected:
            raise ValueError("authority generation hash does not match physical identities")
        return self


class JobCenterAuthorityManifest(_AuthorityModel):
    """Installed, content-addressed authority for Dashboard and daemon composition."""

    schema_version: Literal[4] = 4
    code_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    deployment_profile_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    deployment_generation_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    runtime_deployment_root: Path
    runtime_root: Path
    lab_jobs_path: Path
    command_spool_path: Path
    final_artifact_root: Path
    definition_registry_root: Path
    experiment_registry_path: Path
    dataset_authority_path: Path
    catalog_authority_root: Path
    catalog_authority_receipt_path: Path
    authorities: tuple[AuthorityGenerationBinding, ...]
    manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator(
        "runtime_deployment_root",
        "runtime_root",
        "lab_jobs_path",
        "command_spool_path",
        "final_artifact_root",
        "definition_registry_root",
        "experiment_registry_path",
        "dataset_authority_path",
        "catalog_authority_root",
        "catalog_authority_receipt_path",
    )
    @classmethod
    def validate_paths(cls, value: Path) -> Path:
        return _absolute_path(value, label="Job Center authority")

    @model_validator(mode="after")
    def validate_manifest(self) -> Self:
        names = tuple(binding.name for binding in self.authorities)
        if names != (
            "artifact_catalog",
            "definition_registry",
            "experiment_registry",
            "job_center",
        ):
            raise ValueError("authority bindings must be complete, unique, and ordered")
        expected = _canonical_hash(self.model_dump(mode="json", exclude={"manifest_hash"}))
        if self.manifest_hash != expected:
            raise ValueError("authority manifest hash does not match canonical content")
        return self

    @property
    def research_root(self) -> Path:
        """Compatibility name for existing Job Center callers."""

        return self.runtime_root


class JobCenterAuthorityDeploymentBinding(_AuthorityModel):
    """Exact Job Center paths named by one current deployment profile generation."""

    code_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    deployment_profile_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    deployment_generation_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    runtime_deployment_root: Path
    runtime_root: Path
    lab_jobs_path: Path
    command_spool_path: Path
    final_artifact_root: Path
    definition_registry_root: Path
    experiment_registry_path: Path
    dataset_authority_path: Path
    catalog_authority_root: Path
    catalog_authority_receipt_path: Path
    runtime_mode: Literal["local-test", "linux-production"]
    lab_highwater: object | None = None

    @field_validator(
        "runtime_deployment_root",
        "runtime_root",
        "lab_jobs_path",
        "command_spool_path",
        "final_artifact_root",
        "definition_registry_root",
        "experiment_registry_path",
        "dataset_authority_path",
        "catalog_authority_root",
        "catalog_authority_receipt_path",
    )
    @classmethod
    def validate_binding_path(cls, value: Path) -> Path:
        return _absolute_path(value, label="deployment authority binding")


def _absolute_path(path: Path, *, label: str) -> Path:
    candidate = Path(path)
    normalized = Path(os.path.abspath(os.fspath(candidate)))
    if not candidate.is_absolute() or candidate != normalized:
        raise ValueError(f"{label} path must be absolute and normalized")
    return candidate


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _canonical_hash(value: object) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _validate_directory(path: Path, *, label: str) -> os.stat_result:
    candidate = _absolute_path(path, label=label)
    try:
        observed = candidate.lstat()
    except FileNotFoundError as exc:
        raise JobCenterAuthorityIntegrityError(f"{label} does not exist") from exc
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISDIR(observed.st_mode)
        or observed.st_uid != os.geteuid()
        or stat.S_IMODE(observed.st_mode) != 0o700
    ):
        raise JobCenterAuthorityIntegrityError(
            f"{label} must be an owned physical directory with mode 0700"
        )
    return observed


def _open_verified_file(path: Path, *, label: str) -> tuple[int, os.stat_result]:
    candidate = _absolute_path(path, label=label)
    try:
        before = candidate.lstat()
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(candidate, flags)
    except (FileNotFoundError, OSError) as exc:
        raise JobCenterAuthorityIntegrityError(f"{label} cannot be opened safely") from exc
    try:
        opened = os.fstat(descriptor)
        after = candidate.lstat()
        identities = (
            (before.st_dev, before.st_ino),
            (opened.st_dev, opened.st_ino),
            (after.st_dev, after.st_ino),
        )
        if len(set(identities)) != 1:
            raise JobCenterAuthorityIntegrityError(f"{label} identity changed while opening")
        if (
            stat.S_ISLNK(opened.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.geteuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o600
        ):
            raise JobCenterAuthorityIntegrityError(
                f"{label} must be an owned, singly linked physical file with mode 0600"
            )
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor, opened


def _file_identity(path: Path, *, label: str) -> AuthorityPathIdentity:
    descriptor, observed = _open_verified_file(path, label=label)
    os.close(descriptor)
    return AuthorityPathIdentity(
        path=path,
        kind="file",
        device=observed.st_dev,
        inode=observed.st_ino,
        mode=stat.S_IMODE(observed.st_mode),
    )


def _directory_identity(path: Path, *, label: str) -> AuthorityPathIdentity:
    observed = _validate_directory(path, label=label)
    return AuthorityPathIdentity(
        path=path,
        kind="directory",
        device=observed.st_dev,
        inode=observed.st_ino,
        mode=stat.S_IMODE(observed.st_mode),
    )


def _binding(
    name: AuthorityName,
    identities: tuple[AuthorityPathIdentity, ...],
) -> AuthorityGenerationBinding:
    payload = {
        "contract": "job-center-authority-generation/v1",
        "name": name,
        "identities": [identity.model_dump(mode="json") for identity in identities],
    }
    return AuthorityGenerationBinding(
        name=name,
        identities=identities,
        generation_id=_canonical_hash(payload),
    )


def _build_manifest(
    *,
    code_sha: str,
    deployment_profile_id: str,
    deployment_generation_hash: str,
    runtime_deployment_root: Path,
    runtime_root: Path,
    lab_jobs_path: Path,
    command_spool_path: Path,
    final_artifact_root: Path,
    definition_registry_root: Path,
    experiment_registry_path: Path,
    dataset_authority_path: Path,
    catalog_authority_root: Path,
    catalog_authority_receipt_path: Path,
) -> JobCenterAuthorityManifest:
    paths = {
        "runtime_deployment_root": _absolute_path(
            runtime_deployment_root,
            label="runtime deployment root",
        ),
        "runtime_root": _absolute_path(runtime_root, label="runtime root"),
        "lab_jobs_path": _absolute_path(lab_jobs_path, label="lab jobs"),
        "command_spool_path": _absolute_path(command_spool_path, label="command spool"),
        "final_artifact_root": _absolute_path(final_artifact_root, label="final artifact root"),
        "definition_registry_root": _absolute_path(
            definition_registry_root,
            label="definition registry",
        ),
        "experiment_registry_path": _absolute_path(
            experiment_registry_path,
            label="experiment registry",
        ),
        "dataset_authority_path": _absolute_path(
            dataset_authority_path,
            label="dataset authority",
        ),
        "catalog_authority_root": _absolute_path(
            catalog_authority_root,
            label="artifact catalog",
        ),
        "catalog_authority_receipt_path": _absolute_path(
            catalog_authority_receipt_path,
            label="artifact catalog receipt",
        ),
    }
    root = paths["runtime_root"]
    deployment_root = paths["runtime_deployment_root"]
    if not root.is_relative_to(deployment_root):
        raise JobCenterAuthorityIntegrityError(
            "Job Center runtime root is outside runtime deployment root"
        )
    _validate_directory(root, label="runtime root")
    for key in (
        "lab_jobs_path",
        "command_spool_path",
        "final_artifact_root",
        "experiment_registry_path",
        "dataset_authority_path",
        "catalog_authority_root",
        "catalog_authority_receipt_path",
    ):
        if not paths[key].is_relative_to(root):
            raise JobCenterAuthorityIntegrityError(f"{key} is outside runtime root")
    from rquant.artifact_retention_catalog_authority import load_retention_catalog_authority

    catalog_root = paths["catalog_authority_root"]
    catalog_receipt = paths["catalog_authority_receipt_path"]
    if catalog_receipt != catalog_root / "current.json":
        raise JobCenterAuthorityIntegrityError(
            "artifact catalog receipt must be the retention authority current receipt"
        )
    try:
        catalog_authority = load_retention_catalog_authority(
            catalog_root,
            expected_producer_commit=None,
            expected_reference_store_path=catalog_root.parent / "references.sqlite3",
        )
    except (OSError, ValueError, RuntimeError) as exc:
        raise JobCenterAuthorityIntegrityError(
            "retention catalog authority is unavailable or invalid"
        ) from exc
    authorities = (
        _binding(
            "artifact_catalog",
            (
                _directory_identity(
                    catalog_root,
                    label="retention catalog authority root",
                ),
                _file_identity(
                    catalog_authority.current_receipt_path,
                    label="retention catalog receipt",
                ),
                _file_identity(
                    catalog_root / "snapshots" / catalog_authority.receipt.snapshot_file,
                    label="retention catalog snapshot",
                ),
            ),
        ),
        _binding(
            "definition_registry",
            (
                _directory_identity(
                    paths["definition_registry_root"],
                    label="definition registry root",
                ),
            ),
        ),
        _binding(
            "experiment_registry",
            (
                _file_identity(
                    paths["experiment_registry_path"],
                    label="experiment registry",
                ),
            ),
        ),
        _binding(
            "job_center",
            (
                _directory_identity(root, label="runtime root"),
                _file_identity(paths["lab_jobs_path"], label="lab jobs"),
                _directory_identity(paths["command_spool_path"], label="command spool"),
                _directory_identity(paths["final_artifact_root"], label="final artifact root"),
                _file_identity(paths["dataset_authority_path"], label="dataset authority"),
            ),
        ),
    )
    values = {
        "code_sha": code_sha,
        "deployment_profile_id": deployment_profile_id,
        "deployment_generation_hash": deployment_generation_hash,
        **paths,
        "authorities": authorities,
    }
    manifest_hash = _canonical_hash(
        JobCenterAuthorityManifest.model_construct(
            schema_version=4,
            manifest_hash="0" * 64,
            **values,
        ).model_dump(mode="json", exclude={"manifest_hash"})
    )
    return JobCenterAuthorityManifest(**values, manifest_hash=manifest_hash)


def _manifest_bytes(manifest: JobCenterAuthorityManifest) -> bytes:
    return _canonical_bytes(manifest.model_dump(mode="json")) + b"\n"


def _read_manifest(path: Path) -> JobCenterAuthorityManifest:
    descriptor, observed = _open_verified_file(path, label="Job Center authority manifest")
    try:
        if observed.st_size > 1024 * 1024:
            raise JobCenterAuthorityIntegrityError("Job Center authority manifest is too large")
        payload = os.read(descriptor, observed.st_size + 1)
        if len(payload) != observed.st_size or not payload.endswith(b"\n"):
            raise JobCenterAuthorityIntegrityError("Job Center authority manifest is not canonical")
    finally:
        os.close(descriptor)
    try:
        parsed = json.loads(payload)
        manifest = JobCenterAuthorityManifest.model_validate(parsed)
    except (TypeError, ValueError) as exc:
        raise JobCenterAuthorityIntegrityError(
            "Job Center authority manifest hash or schema is invalid"
        ) from exc
    if payload != _manifest_bytes(manifest):
        raise JobCenterAuthorityIntegrityError("Job Center authority manifest is not canonical")
    return manifest


def _write_atomic(path: Path, payload: bytes) -> Path:
    parent = path.parent
    _validate_directory(parent, label="authority manifest parent")
    temporary = parent / f".{path.name}.{uuid4().hex}.tmp"
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0),
        0o600,
    )
    try:
        with os.fdopen(descriptor, "wb", closefd=True) as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        _atomic_replace(temporary, path)
        directory_fd = os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except BaseException:
        with suppress(FileNotFoundError):
            temporary.unlink()
        raise
    return path


def _atomic_replace(source: Path, target: Path) -> None:
    os.replace(source, target)


def _remove_atomic(path: Path) -> None:
    parent = path.parent
    _validate_directory(parent, label="authority manifest parent")
    path.unlink()
    directory_fd = os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _profile_setting_path(value: object, *, label: str) -> Path:
    if not isinstance(value, str) or not value:
        raise JobCenterAuthorityIntegrityError(f"deployment profile {label} is missing")
    try:
        return _absolute_path(Path(value), label=f"deployment profile {label}")
    except ValueError as exc:
        raise JobCenterAuthorityIntegrityError(
            f"deployment profile {label} is not an absolute normalized path"
        ) from exc


def resolve_current_job_center_authority_binding(
    runtime_deployment_root: Path,
    *,
    expected_code_sha: str,
    runtime_root: Path,
    lab_jobs_path: Path,
    command_spool_path: Path,
    final_artifact_root: Path,
) -> JobCenterAuthorityDeploymentBinding:
    """Resolve Job Center ownership only from the installed deployment profile."""

    from rquant.runtime_deployment_bundle import (
        load_current_runtime_deployment_receipt_unbound,
    )
    from rquant.runtime_deployment_profile import (
        load_current_runtime_deployment_profile,
    )
    from rquant.runtime_service_entrypoint import RuntimeServiceKind

    deployment_root = _absolute_path(
        runtime_deployment_root,
        label="runtime deployment root",
    )
    selected_runtime_root = _absolute_path(runtime_root, label="Job Center runtime root")
    selected_jobs = _absolute_path(lab_jobs_path, label="Lab jobs")
    selected_commands = _absolute_path(command_spool_path, label="Lab command spool")
    selected_artifacts = _absolute_path(final_artifact_root, label="Lab final artifacts")
    try:
        receipt = load_current_runtime_deployment_receipt_unbound(deployment_root)
        profile = load_current_runtime_deployment_profile(deployment_root)
    except (OSError, ValueError) as exc:
        raise JobCenterAuthorityIntegrityError(
            "current runtime deployment profile is unavailable or invalid"
        ) from exc
    if receipt.producer_commit != expected_code_sha or profile.producer_commit != expected_code_sha:
        raise JobCenterAuthorityIntegrityError(
            "current runtime deployment profile code SHA is stale"
        )
    if profile.profile_id is None or receipt.deployment_profile_id != profile.profile_id:
        raise JobCenterAuthorityIntegrityError(
            "current runtime deployment profile identity is incomplete"
        )
    if profile.production_runtime_root != str(deployment_root):
        raise JobCenterAuthorityIntegrityError(
            "current runtime deployment profile root is not authoritative"
        )

    jobs_manifests = tuple(
        manifest
        for manifest in profile.manifests
        if manifest.service_kind is RuntimeServiceKind.LAB_JOBS_PUBLISHER
    )
    catalog_manifests = tuple(
        manifest
        for manifest in profile.manifests
        if manifest.service_kind is RuntimeServiceKind.LAB_ARTIFACT_CATALOG
    )
    retention_manifests = tuple(
        manifest
        for manifest in profile.manifests
        if manifest.service_kind is RuntimeServiceKind.ARTIFACT_RETENTION
    )
    if len(jobs_manifests) != 1 or len(catalog_manifests) != 1 or len(retention_manifests) != 1:
        raise JobCenterAuthorityIntegrityError(
            "deployment profile must contain one Lab jobs, artifact catalog, and retention owner"
        )
    jobs_settings = jobs_manifests[0].settings
    catalog_settings = catalog_manifests[0].settings
    retention_settings = retention_manifests[0].settings
    profile_jobs = _profile_setting_path(
        jobs_settings.get("lab_jobs_path"),
        label="lab_jobs_path",
    )
    catalog_jobs = _profile_setting_path(
        catalog_settings.get("lab_jobs_path"),
        label="catalog lab_jobs_path",
    )
    profile_artifacts = _profile_setting_path(
        catalog_settings.get("artifact_root"),
        label="artifact_root",
    )
    experiments = _profile_setting_path(
        catalog_settings.get("experiment_registry_path"),
        label="experiment_registry_path",
    )
    dataset = _profile_setting_path(
        catalog_settings.get("dataset_authority_path"),
        label="dataset_authority_path",
    )
    catalog_state_root = _profile_setting_path(
        catalog_settings.get("state_root"),
        label="catalog state_root",
    )
    retention_state_root = _profile_setting_path(
        retention_settings.get("state_root"),
        label="retention state_root",
    )
    retention_references = _profile_setting_path(
        retention_settings.get("reference_store_path"),
        label="retention reference_store_path",
    )
    catalog = _profile_setting_path(
        retention_settings.get("catalog_authority_root"),
        label="retention catalog_authority_root",
    )
    definition_roots = {
        _profile_setting_path(
            manifest.settings["definition_registry_root"],
            label="definition_registry_root",
        )
        for manifest in profile.manifests
        if "definition_registry_root" in manifest.settings
    }
    if len(definition_roots) != 1:
        raise JobCenterAuthorityIntegrityError(
            "deployment profile does not name one exact Definition Registry"
        )
    definitions = next(iter(definition_roots))
    if (
        profile_jobs != selected_jobs
        or catalog_jobs != selected_jobs
        or profile_artifacts != selected_artifacts
        or selected_jobs.parent != selected_runtime_root
        or selected_commands.parent != selected_runtime_root
        or selected_artifacts.parent != selected_runtime_root
        or experiments.parent != selected_runtime_root
        or dataset.parent != selected_runtime_root
        or not catalog_state_root.is_relative_to(selected_runtime_root)
        or retention_state_root != catalog.parent
        or retention_references != retention_state_root / "references.sqlite3"
        or catalog != retention_state_root / "catalog-authority"
    ):
        raise JobCenterAuthorityIntegrityError(
            "controlled Lab runtime paths differ from the current deployment profile"
        )
    from rquant.artifact_retention_catalog_authority import (
        load_retention_catalog_authority,
        quarantine_legacy_catalog_reference_store,
    )

    legacy_catalog_references = catalog_state_root / "references.sqlite3"
    if legacy_catalog_references.exists():
        quarantine_legacy_catalog_reference_store(
            legacy_path=legacy_catalog_references,
            retention_state_root=retention_state_root,
            reason="job-center-authority-v4",
        )
    try:
        catalog_authority = load_retention_catalog_authority(
            catalog,
            expected_producer_commit=expected_code_sha,
            expected_reference_store_path=retention_references,
        )
    except (OSError, ValueError, RuntimeError) as exc:
        raise JobCenterAuthorityIntegrityError(
            "retention catalog authority must be initialized before Job Center binding"
        ) from exc
    return JobCenterAuthorityDeploymentBinding(
        code_sha=expected_code_sha,
        deployment_profile_id=str(profile.profile_id),
        deployment_generation_hash=receipt.generation_hash,
        runtime_deployment_root=deployment_root,
        runtime_root=selected_runtime_root,
        lab_jobs_path=selected_jobs,
        command_spool_path=selected_commands,
        final_artifact_root=selected_artifacts,
        definition_registry_root=definitions,
        experiment_registry_path=experiments,
        dataset_authority_path=dataset,
        catalog_authority_root=catalog,
        catalog_authority_receipt_path=catalog_authority.current_receipt_path,
        runtime_mode=profile.runtime_mode,
        lab_highwater=profile.lab_highwater,
    )


def publish_job_center_authority_candidate(
    path: Path,
    **authorities: object,
) -> Path:
    """Publish one canonical candidate after re-reading all physical authorities."""

    manifest = _build_manifest(**authorities)  # type: ignore[arg-type]
    candidate_path = _absolute_path(path, label="candidate manifest")
    return _write_atomic(candidate_path, _manifest_bytes(manifest))


def _verify_expected(
    manifest: JobCenterAuthorityManifest,
    *,
    expected_code_sha: str,
    expected_runtime_root: Path,
    expected_runtime_deployment_root: Path | None = None,
    expected_deployment_profile_id: str | None = None,
    expected_deployment_generation_hash: str | None = None,
    expected_paths: dict[str, Path] | None = None,
) -> None:
    if manifest.code_sha != expected_code_sha:
        raise JobCenterAuthorityIntegrityError("Job Center authority code SHA is stale")
    if manifest.runtime_root != _absolute_path(expected_runtime_root, label="expected root"):
        raise JobCenterAuthorityIntegrityError("Job Center authority runtime root mismatch")
    if (
        expected_runtime_deployment_root is not None
        and manifest.runtime_deployment_root
        != _absolute_path(expected_runtime_deployment_root, label="expected deployment root")
    ):
        raise JobCenterAuthorityIntegrityError(
            "Job Center authority runtime deployment root mismatch"
        )
    if (
        expected_deployment_profile_id is not None
        and manifest.deployment_profile_id != expected_deployment_profile_id
    ):
        raise JobCenterAuthorityIntegrityError("Job Center authority deployment profile is stale")
    if (
        expected_deployment_generation_hash is not None
        and manifest.deployment_generation_hash != expected_deployment_generation_hash
    ):
        raise JobCenterAuthorityIntegrityError(
            "Job Center authority deployment generation is stale"
        )
    for field_name, expected in (expected_paths or {}).items():
        if getattr(manifest, field_name) != _absolute_path(expected, label=field_name):
            raise JobCenterAuthorityIntegrityError(
                f"Job Center authority {field_name} path mismatch"
            )
    rebuilt = _build_manifest(
        code_sha=manifest.code_sha,
        deployment_profile_id=manifest.deployment_profile_id,
        deployment_generation_hash=manifest.deployment_generation_hash,
        runtime_deployment_root=manifest.runtime_deployment_root,
        runtime_root=manifest.runtime_root,
        lab_jobs_path=manifest.lab_jobs_path,
        command_spool_path=manifest.command_spool_path,
        final_artifact_root=manifest.final_artifact_root,
        definition_registry_root=manifest.definition_registry_root,
        experiment_registry_path=manifest.experiment_registry_path,
        dataset_authority_path=manifest.dataset_authority_path,
        catalog_authority_root=manifest.catalog_authority_root,
        catalog_authority_receipt_path=manifest.catalog_authority_receipt_path,
    )
    if rebuilt != manifest:
        raise JobCenterAuthorityIntegrityError(
            "Job Center authority physical identity or generation has changed"
        )


def install_job_center_authority(
    candidate: Path,
    *,
    target: Path,
    expected_code_sha: str,
    expected_runtime_root: Path,
    expected_runtime_deployment_root: Path,
    expected_deployment_profile_id: str,
    expected_deployment_generation_hash: str,
) -> Path:
    """Atomically install an already published candidate without weakening identity checks."""

    manifest = _read_manifest(candidate)
    _verify_expected(
        manifest,
        expected_code_sha=expected_code_sha,
        expected_runtime_root=expected_runtime_root,
        expected_runtime_deployment_root=expected_runtime_deployment_root,
        expected_deployment_profile_id=expected_deployment_profile_id,
        expected_deployment_generation_hash=expected_deployment_generation_hash,
    )
    selected_target = _absolute_path(target, label="installed authority")
    if selected_target != manifest.runtime_root / "job-center-authority.json":
        raise JobCenterAuthorityIntegrityError("installed authority target is not canonical")
    return _write_atomic(selected_target, _manifest_bytes(manifest))


def load_job_center_authority(
    path: Path,
    *,
    expected_code_sha: str,
    runtime_root: Path,
    runtime_deployment_root: Path | None = None,
    deployment_profile_id: str | None = None,
    deployment_generation_hash: str | None = None,
    lab_jobs_path: Path | None = None,
    command_spool_path: Path | None = None,
    final_artifact_root: Path | None = None,
    definition_registry_root: Path | None = None,
    experiment_registry_path: Path | None = None,
    dataset_authority_path: Path | None = None,
    catalog_authority_root: Path | None = None,
    catalog_authority_receipt_path: Path | None = None,
) -> JobCenterAuthorityManifest:
    """Load an installed authority and compare it to controlled runtime settings."""

    manifest = _read_manifest(path)
    _verify_expected(
        manifest,
        expected_code_sha=expected_code_sha,
        expected_runtime_root=runtime_root,
        expected_runtime_deployment_root=runtime_deployment_root,
        expected_deployment_profile_id=deployment_profile_id,
        expected_deployment_generation_hash=deployment_generation_hash,
        expected_paths={
            **({"lab_jobs_path": lab_jobs_path} if lab_jobs_path is not None else {}),
            **(
                {"command_spool_path": command_spool_path} if command_spool_path is not None else {}
            ),
            **(
                {"final_artifact_root": final_artifact_root}
                if final_artifact_root is not None
                else {}
            ),
            **(
                {"definition_registry_root": definition_registry_root}
                if definition_registry_root is not None
                else {}
            ),
            **(
                {"experiment_registry_path": experiment_registry_path}
                if experiment_registry_path is not None
                else {}
            ),
            **(
                {"dataset_authority_path": dataset_authority_path}
                if dataset_authority_path is not None
                else {}
            ),
            **(
                {"catalog_authority_root": catalog_authority_root}
                if catalog_authority_root is not None
                else {}
            ),
            **(
                {"catalog_authority_receipt_path": catalog_authority_receipt_path}
                if catalog_authority_receipt_path is not None
                else {}
            ),
        },
    )
    return manifest


def publish_install_current_job_center_authority(
    *,
    code_sha: str,
    deployment_profile_id: str,
    deployment_generation_hash: str,
    runtime_deployment_root: Path,
    current_code_sha: Callable[[], str],
    runtime_root: Path,
    lab_jobs_path: Path,
    command_spool_path: Path,
    final_artifact_root: Path,
    definition_registry_root: Path,
    experiment_registry_path: Path,
    dataset_authority_path: Path,
    catalog_authority_root: Path,
    catalog_authority_receipt_path: Path,
) -> JobCenterAuthorityManifest:
    """Publish, install, and reload one authority under a continuously checked SHA."""

    def verify_current_sha() -> None:
        if current_code_sha() != code_sha:
            raise JobCenterAuthorityIntegrityError(
                "Job Center authority current code SHA changed during publication"
            )

    root = _absolute_path(runtime_root, label="runtime root")
    candidate = root / ".job-center-authority.candidate.json"
    installed = root / "job-center-authority.json"
    authorities = {
        "runtime_deployment_root": runtime_deployment_root,
        "runtime_root": root,
        "lab_jobs_path": lab_jobs_path,
        "command_spool_path": command_spool_path,
        "final_artifact_root": final_artifact_root,
        "definition_registry_root": definition_registry_root,
        "experiment_registry_path": experiment_registry_path,
        "dataset_authority_path": dataset_authority_path,
        "catalog_authority_root": catalog_authority_root,
        "catalog_authority_receipt_path": catalog_authority_receipt_path,
    }
    verify_current_sha()
    previous_payload: bytes | None = None
    if installed.exists():
        previous_payload = _manifest_bytes(_read_manifest(installed))
    installed_identity: tuple[int, int] | None = None
    try:
        publish_job_center_authority_candidate(
            candidate,
            code_sha=code_sha,
            deployment_profile_id=deployment_profile_id,
            deployment_generation_hash=deployment_generation_hash,
            **authorities,
        )
        verify_current_sha()
        install_job_center_authority(
            candidate,
            target=installed,
            expected_code_sha=code_sha,
            expected_runtime_root=root,
            expected_runtime_deployment_root=runtime_deployment_root,
            expected_deployment_profile_id=deployment_profile_id,
            expected_deployment_generation_hash=deployment_generation_hash,
        )
        observed_installed = installed.lstat()
        installed_identity = (observed_installed.st_dev, observed_installed.st_ino)
        verify_current_sha()
        return load_job_center_authority(
            installed,
            expected_code_sha=code_sha,
            deployment_profile_id=deployment_profile_id,
            deployment_generation_hash=deployment_generation_hash,
            **authorities,
        )
    except BaseException:
        if installed_identity is not None:
            try:
                active = installed.lstat()
                if (active.st_dev, active.st_ino) != installed_identity:
                    raise JobCenterAuthorityIntegrityError(
                        "installed Job Center authority changed before rollback"
                    )
                if previous_payload is None:
                    _remove_atomic(installed)
                else:
                    _write_atomic(installed, previous_payload)
            except BaseException as rollback_error:
                raise JobCenterAuthorityIntegrityError(
                    "Job Center authority publication failed and current rollback failed"
                ) from rollback_error
        raise
    finally:
        with suppress(FileNotFoundError):
            candidate.unlink()
