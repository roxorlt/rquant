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
from typing import Annotated

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

CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
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
    MARKET_MINUTE_SOURCE = "market_minute_source"
    FEATURE_LIVE = "feature_live"
    STRATEGY_LIVE = "strategy_live"
    SIGNAL_ROUTER = "signal_router"
    NOTIFIER = "notifier"
    PAPER_CONSUMER = "paper_consumer"
    PAPER_BROKER = "paper_broker"
    SERVING_PUBLISHER = "serving_publisher"


class RuntimeServiceManifest(RuntimeContractModel):
    schema_version: int = Field(default=1, ge=1)
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


class RuntimeServiceRegistry:
    def __init__(self) -> None:
        self._builders: dict[RuntimeServiceKind, RuntimeServiceBuilder] = {}

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
        except KeyError as exc:
            raise KeyError(
                f"runtime service builder is not registered: {manifest.service_kind.value}"
            ) from exc
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


def load_runtime_service_manifest(
    path: Path,
    *,
    expected_commit: str,
) -> RuntimeServiceManifest:
    if re.fullmatch(r"[0-9a-f]{40}", expected_commit) is None:
        raise ValueError("expected commit must be a full lowercase Git SHA")
    manifest_path = Path(os.path.abspath(path))
    try:
        manifest = RuntimeServiceManifest.model_validate_json(
            _read_owned_manifest(manifest_path)
        )
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
    return run_service_loop(
        control,
        step=step,
        stop_event=stop_event,
        interval_seconds=manifest.interval_seconds,
        max_iterations=max_iterations,
    )


__all__ = [
    "RuntimeServiceBuilder",
    "RuntimeServiceKind",
    "RuntimeServiceManifest",
    "RuntimeServiceRegistry",
    "RuntimeServiceStep",
    "load_runtime_service_manifest",
    "run_runtime_service_manifest",
]
