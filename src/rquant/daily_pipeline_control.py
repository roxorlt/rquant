"""Immutable control-plane plan for the daily-close DAG CLI.

This module deliberately contains no SQLite or stage implementation.  It is
the small boundary shared by ``preview`` and ``apply`` so an operator cannot
silently execute a plan whose run identity, DAG, mode, or command manifest
changed after preview.
"""

from __future__ import annotations

import os
import stat
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Literal, Self

from pydantic import Field, model_validator

from rquant.daily_pipeline_ledger import (
    DailyPipelineMode,
    DailyPipelineStorageProfile,
    DailyRunSpec,
)
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256

if TYPE_CHECKING:
    from rquant.runtime_deployment_profile import RuntimeDeploymentProfile

Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class DailyPipelineProductionProfileError(ValueError):
    """The fixed production profile is present, unsafe, or invalid."""


_DIRECTORY_OPEN_FLAGS = (
    os.O_RDONLY
    | getattr(os, "O_DIRECTORY", 0)
    | getattr(os, "O_NOFOLLOW", 0)
    | getattr(os, "O_CLOEXEC", 0)
)


def _assert_component_verified_absent(
    component: str,
    *,
    dir_fd: int,
    open_error: OSError,
    final: bool,
) -> None:
    """Fail-closed triage of one failed component open.

    Returning means the *final* component was independently confirmed absent
    with fstatat(NOFOLLOW) against the pinned parent descriptor.  Every other
    outcome — a symlink (platforms disagree on ELOOP/ENOTDIR/ENOENT here), a
    non-directory, a missing ancestor, or an open/stat errno mismatch — must
    reject rather than be inferred from the openat errno alone.
    """
    try:
        observed = os.stat(component, dir_fd=dir_fd, follow_symlinks=False)
    except FileNotFoundError as exc:
        if not final:
            raise DailyPipelineProductionProfileError(
                "fixed production runtime ancestor is missing"
            ) from exc
        if not isinstance(open_error, FileNotFoundError):
            raise DailyPipelineProductionProfileError(
                "fixed production runtime path changed while validating"
            ) from open_error
        return
    except OSError as exc:
        raise DailyPipelineProductionProfileError(
            "fixed production runtime path cannot be verified"
        ) from exc
    if stat.S_ISLNK(observed.st_mode):
        raise DailyPipelineProductionProfileError(
            "fixed production runtime path contains a symlink"
        ) from open_error
    if not stat.S_ISDIR(observed.st_mode):
        raise DailyPipelineProductionProfileError(
            "fixed production runtime path contains a non-directory"
        ) from open_error
    raise DailyPipelineProductionProfileError(
        "fixed production runtime path is unsafe or was replaced"
    ) from open_error


@dataclass(frozen=True)
class _BoundDirectoryIdentity:
    path: Path
    descriptor: int
    device: int
    inode: int
    owner: int
    mode: int

    @classmethod
    def capture(
        cls,
        *,
        path: Path,
        descriptor: int,
    ) -> _BoundDirectoryIdentity:
        observed = os.fstat(descriptor)
        if not stat.S_ISDIR(observed.st_mode):
            raise DailyPipelineProductionProfileError(
                "fixed production runtime path contains a non-directory"
            )
        return cls(
            path=path,
            descriptor=descriptor,
            device=observed.st_dev,
            inode=observed.st_ino,
            owner=observed.st_uid,
            mode=observed.st_mode,
        )

    def assert_current(self) -> None:
        try:
            path_state = self.path.lstat()
            descriptor_state = os.fstat(self.descriptor)
        except OSError as exc:
            raise DailyPipelineProductionProfileError(
                "fixed production runtime path changed while validating"
            ) from exc
        expected = (self.device, self.inode, self.owner, self.mode)
        if (
            stat.S_ISLNK(path_state.st_mode)
            or not stat.S_ISDIR(path_state.st_mode)
            or expected
            != (
                path_state.st_dev,
                path_state.st_ino,
                path_state.st_uid,
                path_state.st_mode,
            )
            or expected
            != (
                descriptor_state.st_dev,
                descriptor_state.st_ino,
                descriptor_state.st_uid,
                descriptor_state.st_mode,
            )
        ):
            raise DailyPipelineProductionProfileError(
                "fixed production runtime path was replaced"
            )


class _FixedProductionRuntimeBinding:
    def __init__(
        self,
        identities: tuple[_BoundDirectoryIdentity, ...],
        *,
        root: Path,
        final_name: str,
        final_absent: bool,
    ) -> None:
        self.identities = identities
        self.root = root
        self.final_name = final_name
        self.final_absent = final_absent
        self._closed = False

    @classmethod
    def open(cls, root: Path) -> _FixedProductionRuntimeBinding:
        candidate = Path(root)
        if (
            not candidate.is_absolute()
            or candidate != Path(os.path.abspath(candidate))
            or candidate.anchor != "/"
            or len(candidate.parts) < 2
        ):
            raise DailyPipelineProductionProfileError(
                "fixed production runtime root is not canonical"
            )
        descriptors: list[int] = []
        identities: list[_BoundDirectoryIdentity] = []
        binding: _FixedProductionRuntimeBinding | None = None
        try:
            parent = os.open("/", _DIRECTORY_OPEN_FLAGS)
            descriptors.append(parent)
            current = Path("/")
            identities.append(
                _BoundDirectoryIdentity.capture(path=current, descriptor=parent)
            )
            components = candidate.parts[1:]
            for index, component in enumerate(components):
                current /= component
                try:
                    parent = os.open(
                        component,
                        _DIRECTORY_OPEN_FLAGS,
                        dir_fd=parent,
                    )
                except OSError as open_error:
                    _assert_component_verified_absent(
                        component,
                        dir_fd=parent,
                        open_error=open_error,
                        final=index == len(components) - 1,
                    )
                    binding = cls(
                        tuple(identities),
                        root=candidate,
                        final_name=component,
                        final_absent=True,
                    )
                    binding.assert_current()
                    return binding
                descriptors.append(parent)
                identities.append(
                    _BoundDirectoryIdentity.capture(path=current, descriptor=parent)
                )
            final = identities[-1]
            if final.owner != os.geteuid() or stat.S_IMODE(final.mode) != 0o700:
                raise DailyPipelineProductionProfileError(
                    "fixed production runtime root has unsafe owner or mode"
                )
            binding = cls(
                tuple(identities),
                root=candidate,
                final_name=components[-1],
                final_absent=False,
            )
            binding.assert_current()
            return binding
        except (OSError, DailyPipelineProductionProfileError) as exc:
            if binding is not None:
                binding.close()
            else:
                for descriptor in reversed(descriptors):
                    with suppress(OSError):
                        os.close(descriptor)
            if isinstance(exc, DailyPipelineProductionProfileError):
                raise
            raise DailyPipelineProductionProfileError(
                "fixed production runtime path is unsafe or was replaced"
            ) from exc

    def assert_current(self) -> None:
        if self._closed:
            raise DailyPipelineProductionProfileError(
                "fixed production runtime binding is closed"
            )
        for identity in self.identities:
            identity.assert_current()
        if self.final_absent:
            parent = self.identities[-1]
            try:
                os.stat(
                    self.final_name,
                    dir_fd=parent.descriptor,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                return
            except OSError as exc:
                raise DailyPipelineProductionProfileError(
                    "fixed production runtime final component cannot be verified absent"
                ) from exc
            raise DailyPipelineProductionProfileError(
                "fixed production runtime appeared after absence verification"
            )

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for identity in reversed(self.identities):
            with suppress(OSError):
                os.close(identity.descriptor)

    def __enter__(self) -> _FixedProductionRuntimeBinding:
        return self

    def __exit__(self, *_error: object) -> None:
        self.close()

    def __del__(self) -> None:
        if not getattr(self, "_closed", True):
            with suppress(OSError):
                self.close()


class DailyDagDevProductionAbsenceGuard:
    """Held parent-dir capability proving the fixed production root stays absent."""

    def __init__(self, binding: _FixedProductionRuntimeBinding) -> None:
        if not binding.final_absent:
            raise DailyPipelineProductionProfileError(
                "daily dev absence guard requires an absent production root"
            )
        self._binding = binding

    def assert_still_absent(self) -> None:
        self._binding.assert_current()

    def close(self) -> None:
        self._binding.close()

    def __enter__(self) -> DailyDagDevProductionAbsenceGuard:
        return self

    def __exit__(self, *_error: object) -> None:
        self.close()

    def __del__(self) -> None:
        if hasattr(self, "_binding"):
            with suppress(OSError):
                self.close()


class DailyPipelineControlPlan(RuntimeContractModel):
    """Canonical operator intent for exactly one immutable daily run."""

    contract: Literal["daily-pipeline-control-plan/v3"] = "daily-pipeline-control-plan/v3"
    mode: DailyPipelineMode
    run_spec: DailyRunSpec
    command_manifest_hash: Sha256
    storage_profile: DailyPipelineStorageProfile
    plan_hash: Sha256 | None = None

    @model_validator(mode="after")
    def bind_plan_hash(self) -> Self:
        if self.command_manifest_hash != self.run_spec.command_manifest_hash:
            raise ValueError("daily control plan command manifest does not match its immutable run")
        if (
            self.mode is not self.run_spec.mode
            or self.mode is not self.storage_profile.mode
            or self.run_spec.profile_hash != self.storage_profile.profile_hash
        ):
            raise ValueError("daily control plan storage profile does not match its immutable run")
        expected = canonical_sha256(self.model_dump(mode="json", exclude={"plan_hash"}))
        if self.plan_hash is None:
            object.__setattr__(self, "plan_hash", expected)
        elif self.plan_hash != expected:
            raise ValueError("daily pipeline control plan hash does not match canonical content")
        return self

    @classmethod
    def create(
        cls,
        *,
        mode: DailyPipelineMode,
        run_spec: DailyRunSpec,
        command_manifest_hash: str,
        storage_profile: DailyPipelineStorageProfile,
    ) -> DailyPipelineControlPlan:
        return cls(
            mode=mode,
            run_spec=DailyRunSpec.model_validate(run_spec),
            command_manifest_hash=command_manifest_hash,
            storage_profile=DailyPipelineStorageProfile.model_validate(storage_profile),
        )


def resolve_production_daily_storage_profile(
    *,
    expected_code_commit: str,
    expected_profile_hash: str,
) -> DailyPipelineStorageProfile:
    """Resolve production storage only through the installed immutable profile."""
    deployment = _load_fixed_production_runtime_profile()
    if deployment is None:
        raise DailyPipelineProductionProfileError(
            "fixed production runtime profile is not installed"
        )
    if deployment.producer_commit != expected_code_commit:
        raise ValueError("daily production code commit differs from the immutable profile")
    if deployment.profile_id != expected_profile_hash:
        raise ValueError("daily production profile hash differs from the immutable profile")
    return DailyPipelineStorageProfile.create(
        root=Path(deployment.production_runtime_root),
        mode=DailyPipelineMode.PRODUCTION,
        profile_hash=expected_profile_hash,
    )


def assert_daily_dag_dev_allowed() -> DailyDagDevProductionAbsenceGuard:
    """Permit dev mode only when the fixed managed production root is absent."""
    from rquant.runtime_deployment_profile import LINUX_PRODUCTION_RUNTIME_ROOT

    binding = _FixedProductionRuntimeBinding.open(LINUX_PRODUCTION_RUNTIME_ROOT)
    if binding.final_absent:
        return DailyDagDevProductionAbsenceGuard(binding)
    try:
        _load_profile_from_binding(
            binding,
            runtime_root=LINUX_PRODUCTION_RUNTIME_ROOT,
        )
    finally:
        binding.close()
    raise DailyPipelineProductionProfileError(
        "daily-dag-dev is disabled while a verified production profile is installed"
    )


def _load_fixed_production_runtime_profile() -> RuntimeDeploymentProfile | None:
    from rquant.runtime_deployment_profile import (
        LINUX_PRODUCTION_RUNTIME_ROOT,
        load_current_runtime_deployment_profile,
    )

    runtime_root = LINUX_PRODUCTION_RUNTIME_ROOT
    binding = _FixedProductionRuntimeBinding.open(runtime_root)
    if binding.final_absent:
        binding.close()
        return None
    with binding:
        deployment = _load_profile_from_binding(
            binding,
            runtime_root=runtime_root,
            loader=load_current_runtime_deployment_profile,
        )
    return deployment


def _load_profile_from_binding(
    binding: _FixedProductionRuntimeBinding,
    *,
    runtime_root: Path,
    loader=None,
) -> RuntimeDeploymentProfile:
    if binding.final_absent:
        raise DailyPipelineProductionProfileError(
            "fixed production runtime profile is absent"
        )
    if loader is None:
        from rquant.runtime_deployment_profile import (
            load_current_runtime_deployment_profile as loader,
        )

    try:
        binding.assert_current()
        deployment = loader(runtime_root)
        binding.assert_current()
    except DailyPipelineProductionProfileError:
        raise
    except (OSError, RuntimeError, ValueError) as exc:
        raise DailyPipelineProductionProfileError(
            "fixed production runtime profile is present but invalid"
        ) from exc
    if deployment.runtime_mode != "linux-production":
        raise DailyPipelineProductionProfileError(
            "fixed production runtime profile is not a valid Linux production profile"
        )
    if deployment.production_runtime_root != str(runtime_root):
        raise DailyPipelineProductionProfileError(
            "production profile differs from the fixed runtime root"
        )
    return deployment


__all__ = [
    "DailyPipelineControlPlan",
    "DailyDagDevProductionAbsenceGuard",
    "DailyPipelineProductionProfileError",
    "assert_daily_dag_dev_allowed",
    "resolve_production_daily_storage_profile",
]
