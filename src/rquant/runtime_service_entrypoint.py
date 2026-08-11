"""Typed, allow-listed entrypoint for isolated runtime service processes."""

from __future__ import annotations

import os
import re
import stat
from collections.abc import Callable, Mapping
from datetime import timedelta
from enum import StrEnum
from pathlib import Path
from threading import Event
from types import MappingProxyType
from typing import TYPE_CHECKING, Annotated, Literal

from pydantic import Field, JsonValue, StringConstraints, field_serializer, field_validator

from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.runtime_service_control import (
    Clock,
    RuntimeServiceControl,
    RuntimeServiceHeartbeat,
    RuntimeServicePlane,
    RuntimeServiceSpec,
    RuntimeStepResult,
    run_service_loop,
)

if TYPE_CHECKING:
    from rquant.runtime_artifact_terminal_lifecycle import ProductionArtifactTerminalLifecycle

CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
_GENERATION_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_SECRET_KEY_PARTS = frozenset(
    {"api_key", "credential", "password", "private_key", "secret", "token"}
)


def _freeze_setting(value: object) -> object:
    if isinstance(value, Mapping):
        frozen: dict[str, object] = {}
        for key, item in sorted(value.items()):
            normalized = str(key).strip().lower()
            if not normalized:
                raise ValueError("runtime service setting names cannot be empty")
            if any(part in normalized for part in _SECRET_KEY_PARTS):
                raise ValueError(f"runtime service manifest cannot contain secret setting: {key}")
            frozen[str(key)] = _freeze_setting(item)
        return MappingProxyType(frozen)
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_setting(item) for item in value)
    return value


def _thaw_setting(value: object) -> object:
    if isinstance(value, Mapping):
        return {key: _thaw_setting(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_setting(item) for item in value]
    return value


class RuntimeServiceKind(StrEnum):
    REFERENCE_SLOW_SOURCE = "reference_slow_source"
    REFERENCE_SLOW_PUBLISHER = "reference_slow_publisher"
    AUCTION_UNIVERSE_PUBLISHER = "auction_universe_publisher"
    AUCTION_MATCH_SOURCE = "auction_match_source"
    MARKET_MINUTE_SOURCE = "market_minute_source"
    WATCHLIST_QUOTE_SOURCE = "watchlist_quote_source"
    DAILY_CLOSE_SOURCE = "daily_close_source"
    SHADOW_SESSION = "shadow_session"
    DAILY_PIPELINE_ORCHESTRATOR = "daily_pipeline_orchestrator"
    CANDIDATE_PUBLISHER = "candidate_publisher"
    FEATURE_LIVE = "feature_live"
    STRATEGY_LIVE = "strategy_live"
    SIGNAL_ROUTER = "signal_router"
    NOTIFIER = "notifier"
    PAPER_CONSUMER = "paper_consumer"
    PAPER_CONSTRAINT_PUBLISHER = "paper_constraint_publisher"
    PAPER_BROKER = "paper_broker"
    RUNTIME_HEALTH_PUBLISHER = "runtime_health_publisher"
    LAB_JOBS_PUBLISHER = "lab_jobs_publisher"
    LAB_ARTIFACT_CATALOG = "lab_artifact_catalog"
    ARTIFACT_RETENTION = "artifact_retention"
    PROMOTIONS_PUBLISHER = "promotions_publisher"
    SERVING_PUBLISHER = "serving_publisher"


class RuntimeServiceManifest(RuntimeContractModel):
    schema_version: Literal[1, 2] = 2
    service_id: str = Field(min_length=1)
    service_kind: RuntimeServiceKind
    plane: RuntimeServicePlane
    interval_seconds: float = Field(ge=0)
    stale_after_seconds: float = Field(gt=0)
    producer_commit: CommitSha
    settings: Mapping[str, JsonValue]

    @field_validator("settings")
    @classmethod
    def freeze_settings(cls, value: Mapping[str, JsonValue]) -> Mapping[str, JsonValue]:
        frozen = _freeze_setting(value)
        if not isinstance(frozen, Mapping):
            raise TypeError("runtime service settings must be a mapping")
        return frozen  # type: ignore[return-value]

    @field_serializer("settings")
    def serialize_settings(self, value: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
        thawed = _thaw_setting(value)
        if not isinstance(thawed, dict):
            raise TypeError("runtime service settings must serialize as a mapping")
        return thawed  # type: ignore[return-value]

    @property
    def manifest_fingerprint(self) -> str:
        return canonical_sha256(self)

    @property
    def service_spec(self) -> RuntimeServiceSpec:
        return RuntimeServiceSpec(
            service_id=self.service_id,
            plane=self.plane,
            stale_after=timedelta(seconds=self.stale_after_seconds),
            producer_commit=self.producer_commit,
        )


RuntimeServiceStep = Callable[[], RuntimeStepResult]
RuntimeServiceBuilder = Callable[[RuntimeServiceManifest], RuntimeServiceStep]


class ArtifactTerminalOwnerStep:
    """Keep one terminal-owner lifecycle alive for a runtime step's lifetime."""

    def __init__(
        self,
        *,
        step: RuntimeServiceStep,
        artifact_terminal_lifecycle: ProductionArtifactTerminalLifecycle,
        resource_closer: Callable[[], None] | None = None,
    ) -> None:
        self._step = step
        self.artifact_terminal_lifecycle = artifact_terminal_lifecycle
        self._resource_closer = resource_closer
        self._closed = False

    def __call__(self) -> RuntimeStepResult:
        return self._step()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        errors: list[BaseException] = []
        if self._resource_closer is not None:
            try:
                self._resource_closer()
            except BaseException as exc:
                errors.append(exc)
        try:
            self.artifact_terminal_lifecycle.close()
        except BaseException as exc:
            errors.append(exc)
        if len(errors) == 1:
            raise errors[0]
        if errors:
            raise BaseExceptionGroup("runtime owner cleanup failed", errors)


class RuntimeServiceRegistry:
    def __init__(
        self,
        *,
        artifact_terminal_lifecycle_factory: (
            Callable[[], ProductionArtifactTerminalLifecycle] | None
        ) = None,
    ) -> None:
        self._builders: dict[RuntimeServiceKind, RuntimeServiceBuilder] = {}
        self._artifact_terminal_lifecycle_factory = artifact_terminal_lifecycle_factory

    def open_artifact_terminal_lifecycle(self) -> ProductionArtifactTerminalLifecycle:
        """Open the production-only terminal-owner composition on demand.

        Builders that create audit/snapshot/experiment terminal owners obtain the
        shared hook composition here.  Readers and unrelated services never open
        the lifecycle's writable authorities.
        """

        if self._artifact_terminal_lifecycle_factory is None:
            raise RuntimeError("artifact terminal lifecycle is unavailable in this runtime")
        return self._artifact_terminal_lifecycle_factory()

    def register(
        self,
        kind: RuntimeServiceKind,
        builder: RuntimeServiceBuilder,
    ) -> None:
        if kind in self._builders:
            raise ValueError(f"runtime service builder already registered: {kind.value}")
        self._builders[kind] = builder

    @property
    def registered_kinds(self) -> tuple[RuntimeServiceKind, ...]:
        return tuple(self._builders)

    def build(self, manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        try:
            builder = self._builders[manifest.service_kind]
        except KeyError:
            raise ValueError(
                "runtime service configuration error: builder is not registered for "
                f"service kind {manifest.service_kind.value}"
            ) from None
        return builder(manifest)


def _read_owned_manifest(path: Path) -> bytes:
    directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    no_follow = getattr(os, "O_NOFOLLOW", 0)
    directory_descriptor = -1
    manifest_descriptor = -1
    try:
        directory_descriptor = os.open(path.anchor, directory_flags | no_follow)
        for component in path.parts[1:-1]:
            child_descriptor = os.open(
                component,
                directory_flags | no_follow,
                dir_fd=directory_descriptor,
            )
            os.close(directory_descriptor)
            directory_descriptor = child_descriptor
        manifest_descriptor = os.open(
            path.name,
            os.O_RDONLY | no_follow,
            dir_fd=directory_descriptor,
        )
        observed = os.fstat(manifest_descriptor)
        if not stat.S_ISREG(observed.st_mode) or observed.st_uid != os.getuid():
            raise ValueError("runtime service manifest must be an owned regular file")
        if stat.S_IMODE(observed.st_mode) != 0o600:
            raise ValueError("runtime service manifest must have mode 0600")
        with os.fdopen(manifest_descriptor, "rb", closefd=True) as stream:
            manifest_descriptor = -1
            return stream.read()
    except OSError as exc:
        raise ValueError("runtime service manifest is unavailable or contains a symlink") from exc
    finally:
        if manifest_descriptor >= 0:
            os.close(manifest_descriptor)
        if directory_descriptor >= 0:
            os.close(directory_descriptor)


def _resolve_runtime_current_pointer(
    path: Path,
    *,
    expected_generation: str | None,
) -> Path:
    try:
        index = path.parts[1:-1].index("current") + 1
    except ValueError:
        if expected_generation is not None:
            raise ValueError(
                "runtime service manifest must use the current generation pointer"
            ) from None
        return path
    current = Path(path.anchor, *path.parts[1 : index + 1])
    try:
        observed = current.lstat()
    except FileNotFoundError:
        if expected_generation is not None:
            raise ValueError("runtime service manifest current pointer is unavailable") from None
        return path
    if not stat.S_ISLNK(observed.st_mode):
        if expected_generation is not None:
            raise ValueError("runtime service manifest current pointer must be a symlink")
        return path
    target = Path(os.readlink(current))
    if (
        target.is_absolute()
        or len(target.parts) != 2
        or target.parts[0] != "generations"
        or _GENERATION_PATTERN.fullmatch(target.parts[1]) is None
    ):
        raise ValueError("runtime service manifest current pointer is invalid")
    if expected_generation is not None and target.parts[1] != expected_generation:
        raise ValueError("runtime service manifest generation does not match runtime environment")
    remainder = Path(*path.parts[index + 1 :])
    return current.parent / target / remainder


def load_runtime_service_manifest(
    path: Path,
    *,
    expected_commit: str,
    expected_generation: str | None = None,
) -> RuntimeServiceManifest:
    if re.fullmatch(r"[0-9a-f]{40}", expected_commit) is None:
        raise ValueError("expected commit must be a full lowercase Git SHA")
    if (
        expected_generation is not None
        and re.fullmatch(r"[0-9a-f]{64}", expected_generation) is None
    ):
        raise ValueError("expected generation must be a full lowercase SHA-256")
    manifest_path = _resolve_runtime_current_pointer(
        Path(os.path.abspath(path)),
        expected_generation=expected_generation,
    )
    try:
        manifest = RuntimeServiceManifest.model_validate_json(_read_owned_manifest(manifest_path))
    except ValueError as exc:
        if str(exc).startswith("runtime service manifest"):
            raise
        raise ValueError("invalid runtime service manifest") from exc
    if manifest.producer_commit != expected_commit:
        raise ValueError("runtime service manifest commit does not match running code")
    return manifest


def run_runtime_service_manifest(
    manifest: RuntimeServiceManifest,
    *,
    registry: RuntimeServiceRegistry,
    control_root: Path,
    stop_event: Event,
    max_iterations: int | None = None,
    clock: Clock | None = None,
) -> RuntimeServiceHeartbeat:
    step = registry.build(manifest)
    control = RuntimeServiceControl(
        control_root,
        spec=manifest.service_spec,
        clock=clock,
    )
    try:
        return run_service_loop(
            control,
            step=step,
            stop_event=stop_event,
            interval_seconds=manifest.interval_seconds,
            max_iterations=max_iterations,
        )
    finally:
        close = getattr(step, "close", None)
        if callable(close):
            close()


__all__ = [
    "RuntimeServiceBuilder",
    "RuntimeServiceKind",
    "RuntimeServiceManifest",
    "RuntimeServiceRegistry",
    "RuntimeServiceStep",
    "ArtifactTerminalOwnerStep",
    "load_runtime_service_manifest",
    "run_runtime_service_manifest",
]
