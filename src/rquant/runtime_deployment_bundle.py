"""Atomic, least-privilege deployment bundles for isolated runtime services."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import stat
import tempfile
from collections.abc import Mapping
from contextlib import suppress
from pathlib import Path
from types import MappingProxyType
from typing import Annotated

from pydantic import StringConstraints, field_serializer, field_validator, model_validator

from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import (
    RuntimeServiceKind,
    RuntimeServiceManifest,
)

CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
GenerationHash = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
InstanceName = Annotated[
    str,
    StringConstraints(pattern=r"^svc-[0-9a-f]{64}$"),
]
SystemdUnitName = Annotated[
    str,
    StringConstraints(
        pattern=r"^rquant-runtime-(?:live|serving|research|candidate)@svc-[0-9a-f]{64}\.service$"
    ),
]

_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_GENERATION_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_LIVE_KINDS = frozenset(
    {
        RuntimeServiceKind.MARKET_MINUTE_SOURCE,
        RuntimeServiceKind.CANDIDATE_PUBLISHER,
        RuntimeServiceKind.FEATURE_LIVE,
        RuntimeServiceKind.STRATEGY_LIVE,
        RuntimeServiceKind.SIGNAL_ROUTER,
        RuntimeServiceKind.NOTIFIER,
        RuntimeServiceKind.PAPER_CONSUMER,
        RuntimeServiceKind.PAPER_BROKER,
    }
)
_EXPECTED_PLANE = {
    **{kind: RuntimeServicePlane.LIVE for kind in _LIVE_KINDS},
    RuntimeServiceKind.SERVING_PUBLISHER: RuntimeServicePlane.SERVING,
}
_CAPABILITY_KEYS = {
    RuntimeServiceKind.MARKET_MINUTE_SOURCE: frozenset(
        {"TUSHARE_TOKEN_MAIN", "TUSHARE_TOKEN_BACKUP"}
    ),
    RuntimeServiceKind.NOTIFIER: frozenset(
        {
            "PUSHDEER_KEYS",
            "PUSHPLUS_TOKENS",
            "PUSHDEER_ENDPOINT",
            "PUSHPLUS_ENDPOINT",
        }
    ),
}
_SECRET_CAPABILITY_KEYS = frozenset(
    {
        "TUSHARE_TOKEN_MAIN",
        "TUSHARE_TOKEN_BACKUP",
        "PUSHDEER_KEYS",
        "PUSHPLUS_TOKENS",
    }
)
_WRITABLE_PATH_SETTINGS = {
    RuntimeServiceKind.MARKET_MINUTE_SOURCE: ("spool_root", "quota_path"),
    RuntimeServiceKind.CANDIDATE_PUBLISHER: ("snapshot_root",),
    RuntimeServiceKind.FEATURE_LIVE: ("raw_spool_root", "feature_spool_root"),
    RuntimeServiceKind.STRATEGY_LIVE: (
        "feature_spool_root",
        "runner_state_path",
    ),
    RuntimeServiceKind.SIGNAL_ROUTER: ("signal_bus_path",),
    RuntimeServiceKind.NOTIFIER: ("signal_bus_path",),
    RuntimeServiceKind.PAPER_CONSUMER: (
        "signal_bus_path",
        "queue_path",
        "consumer_state_path",
        "broker_path",
    ),
    RuntimeServiceKind.PAPER_BROKER: (
        "signal_bus_path",
        "queue_path",
        "consumer_state_path",
        "broker_path",
    ),
    RuntimeServiceKind.SERVING_PUBLISHER: ("serving_root",),
}
_READONLY_PATH_SETTINGS = {
    RuntimeServiceKind.CANDIDATE_PUBLISHER: ("candidate_input_path",),
}


class RuntimeDeploymentReceipt(RuntimeContractModel):
    runtime_root: Path
    producer_commit: CommitSha
    generation_hash: GenerationHash
    instance_mapping: Mapping[str, InstanceName]
    unit_mapping: Mapping[str, SystemdUnitName]

    @field_validator("instance_mapping")
    @classmethod
    def freeze_instance_mapping(cls, value: Mapping[str, str]) -> Mapping[str, str]:
        if len(value) != len(set(value.values())):
            raise ValueError("runtime instance names must be unique")
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("instance_mapping")
    def serialize_instance_mapping(self, value: Mapping[str, str]) -> dict[str, str]:
        return dict(value)

    @field_validator("unit_mapping")
    @classmethod
    def freeze_unit_mapping(cls, value: Mapping[str, str]) -> Mapping[str, str]:
        if len(value) != len(set(value.values())):
            raise ValueError("runtime systemd unit names must be unique")
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("unit_mapping")
    def serialize_unit_mapping(self, value: Mapping[str, str]) -> dict[str, str]:
        return dict(value)

    @model_validator(mode="after")
    def validate_mapping_keys(self) -> RuntimeDeploymentReceipt:
        if set(self.instance_mapping) != set(self.unit_mapping):
            raise ValueError("runtime instance and unit mappings must have identical service ids")
        return self


def _instance_name(service_id: str) -> str:
    digest = hashlib.sha256(service_id.encode("utf-8")).hexdigest()
    return f"svc-{digest}"


def _systemd_unit_name(manifest: RuntimeServiceManifest, instance: str) -> str:
    template = (
        "candidate"
        if manifest.service_kind is RuntimeServiceKind.CANDIDATE_PUBLISHER
        else manifest.plane.value
    )
    return f"rquant-runtime-{template}@{instance}.service"


def _canonical_manifest(manifest: RuntimeServiceManifest) -> tuple[RuntimeServiceManifest, bytes]:
    if not isinstance(manifest, RuntimeServiceManifest):
        raise TypeError("manifests must contain RuntimeServiceManifest values")
    payload = json.dumps(
        manifest.model_dump(mode="json"),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    try:
        validated = RuntimeServiceManifest.model_validate_json(payload)
    except ValueError as exc:
        raise ValueError("runtime service manifest is invalid") from exc
    return validated, payload


def _absolute_runtime_root(root: Path) -> Path:
    candidate = Path(root)
    if not candidate.is_absolute():
        raise ValueError("runtime root must be absolute")
    normalized = Path(os.path.abspath(candidate))
    if candidate != normalized:
        raise ValueError("runtime root must not contain path traversal")
    current = Path(candidate.anchor)
    for component in candidate.parts[1:]:
        current /= component
        try:
            observed = current.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(observed.st_mode):
            raise ValueError(f"runtime root contains a symlink parent: {current}")
    return candidate


def _ensure_owned_directory(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    observed = path.lstat()
    if (
        not stat.S_ISDIR(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.getuid()
    ):
        raise ValueError(f"runtime deployment directory is not safely owned: {path}")
    path.chmod(0o700)


def _ensure_owned_descendant(root: Path, path: Path) -> None:
    relative = path.relative_to(root)
    current = root
    for component in relative.parts:
        current /= component
        current.mkdir(mode=0o700, exist_ok=True)
        observed = current.lstat()
        if (
            not stat.S_ISDIR(observed.st_mode)
            or stat.S_ISLNK(observed.st_mode)
            or observed.st_uid != os.getuid()
        ):
            raise ValueError(f"runtime deployment directory is not safely owned: {current}")
        current.chmod(0o700)


def _require_owned_plane_path(
    value: object,
    *,
    runtime_root: Path,
    plane: RuntimeServicePlane,
    setting_name: str,
) -> None:
    if not isinstance(value, str):
        raise ValueError(f"runtime path setting {setting_name} must be a string")
    candidate = Path(value)
    if not candidate.is_absolute() or candidate != Path(os.path.abspath(candidate)):
        raise ValueError(f"runtime path setting {setting_name} must be absolute and normalized")
    owner_root = runtime_root / plane.value
    try:
        relative = candidate.relative_to(owner_root)
    except ValueError as exc:
        raise ValueError(
            f"runtime path setting {setting_name} must be owned by the {plane.value} plane"
        ) from exc
    current = runtime_root
    for component in (plane.value, *relative.parts):
        current /= component
        try:
            observed = current.lstat()
        except FileNotFoundError:
            break
        if stat.S_ISLNK(observed.st_mode):
            raise ValueError(f"runtime path setting {setting_name} contains a symlink: {current}")


def _require_external_readonly_path(
    value: object,
    *,
    runtime_root: Path,
    plane: RuntimeServicePlane,
    setting_name: str,
) -> None:
    if not isinstance(value, str):
        raise ValueError(f"runtime path setting {setting_name} must be a string")
    candidate = Path(value)
    if not candidate.is_absolute() or candidate != Path(os.path.abspath(candidate)):
        raise ValueError(f"runtime path setting {setting_name} must be absolute and normalized")
    writable_roots = {
        plane.value: runtime_root / plane.value,
        "control": runtime_root / "control",
    }
    for root_name, writable_root in writable_roots.items():
        try:
            candidate.relative_to(writable_root)
        except ValueError:
            pass
        else:
            raise ValueError(
                f"read-only runtime path setting {setting_name} must not be inside "
                f"the {root_name} writable owner root"
            )
        try:
            writable_root.relative_to(candidate)
        except ValueError:
            pass
        else:
            raise ValueError(
                f"read-only runtime path setting {setting_name} must not contain "
                f"the {root_name} writable owner root"
            )


def _validate_manifest_authority(
    manifest: RuntimeServiceManifest,
    *,
    producer_commit: str,
    runtime_root: Path,
) -> None:
    if manifest.producer_commit != producer_commit:
        raise ValueError(
            f"runtime manifest {manifest.service_id} producer commit does not match bundle"
        )
    expected_plane = _EXPECTED_PLANE[manifest.service_kind]
    if manifest.plane is not expected_plane:
        raise ValueError(
            f"runtime service {manifest.service_id} must use the {expected_plane.value} plane"
        )
    for setting_name in _WRITABLE_PATH_SETTINGS[manifest.service_kind]:
        if setting_name not in manifest.settings:
            continue
        _require_owned_plane_path(
            manifest.settings[setting_name],
            runtime_root=runtime_root,
            plane=manifest.plane,
            setting_name=setting_name,
        )
    for setting_name in _READONLY_PATH_SETTINGS.get(manifest.service_kind, ()):
        if setting_name not in manifest.settings:
            continue
        _require_external_readonly_path(
            manifest.settings[setting_name],
            runtime_root=runtime_root,
            plane=manifest.plane,
            setting_name=setting_name,
        )
    if manifest.service_kind is RuntimeServiceKind.CANDIDATE_PUBLISHER:
        expected_root = (
            runtime_root
            / RuntimeServicePlane.LIVE.value
            / "candidates"
            / _instance_name(manifest.service_id)
        )
        if Path(str(manifest.settings.get("snapshot_root", ""))) != expected_root:
            raise ValueError(
                "candidate snapshot_root must equal its exclusive systemd instance root"
            )


def _validate_capability_value(name: str, value: object) -> str:
    if not isinstance(value, str):
        raise ValueError(f"capability environment {name} must be a string")
    if not value or any(character in value for character in ("\x00", "\n", "\r")):
        raise ValueError(f"capability environment {name} has an unsafe value")
    return value


def _validate_capabilities(
    manifests: tuple[RuntimeServiceManifest, ...],
    capability_env: Mapping[str, Mapping[str, str]],
) -> dict[str, dict[str, str]]:
    if not isinstance(capability_env, Mapping):
        raise TypeError("capability_env must be a mapping")
    service_ids = {manifest.service_id for manifest in manifests}
    if set(capability_env) != service_ids:
        raise ValueError("capability environment service ids must exactly match manifests")
    validated: dict[str, dict[str, str]] = {}
    for manifest in manifests:
        raw = capability_env[manifest.service_id]
        if not isinstance(raw, Mapping):
            raise ValueError("each capability environment must be a mapping")
        if manifest.plane is not RuntimeServicePlane.LIVE and raw:
            raise ValueError(
                f"{manifest.plane.value} services cannot receive capability environment"
            )
        allowed = _CAPABILITY_KEYS.get(manifest.service_kind, frozenset())
        unknown = set(raw) - allowed
        if unknown:
            names = ", ".join(sorted(str(name) for name in unknown))
            raise ValueError(f"unknown capability environment for {manifest.service_id}: {names}")
        validated[manifest.service_id] = {
            str(name): _validate_capability_value(str(name), value)
            for name, value in sorted(raw.items())
        }
    return validated


def _reject_plaintext_secrets(
    manifest_payloads: Mapping[str, bytes],
    capabilities: Mapping[str, Mapping[str, str]],
) -> None:
    for service_id, payload in manifest_payloads.items():
        secret_values = (
            value
            for name, value in capabilities[service_id].items()
            if name in _SECRET_CAPABILITY_KEYS
        )
        if any(value.encode("utf-8") in payload for value in secret_values):
            raise ValueError(f"runtime manifest {service_id} contains a plaintext capability value")
        parsed = json.loads(payload)
        settings = parsed.get("settings", {})
        encoded_settings = json.dumps(settings, ensure_ascii=True, sort_keys=True).upper()
        if any(
            name in encoded_settings
            for name in _CAPABILITY_KEYS.get(
                RuntimeServiceKind(parsed["service_kind"]), frozenset()
            )
        ):
            raise ValueError(
                f"runtime manifest {service_id} contains a capability environment name"
            )


def _quote_environment_value(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _environment_payload(producer_commit: str, values: Mapping[str, str]) -> bytes:
    lines = [f"RQUANT_RUNTIME_COMMIT={producer_commit}"]
    lines.extend(
        f"{name}={_quote_environment_value(value)}" for name, value in sorted(values.items())
    )
    return ("\n".join(lines) + "\n").encode("utf-8")


def _write_secure_file(path: Path, payload: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb", closefd=True) as stream:
            descriptor = -1
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _validate_existing_generation(
    generation: Path,
    expected_files: Mapping[str, bytes],
) -> None:
    observed = generation.lstat()
    if (
        not stat.S_ISDIR(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.getuid()
        or stat.S_IMODE(observed.st_mode) != 0o700
    ):
        raise ValueError("existing runtime generation is not a safely owned directory")
    actual_files = {
        path.relative_to(generation).as_posix() for path in generation.rglob("*") if path.is_file()
    }
    if actual_files != set(expected_files):
        raise ValueError("existing runtime generation contents do not match bundle")
    for relative, payload in expected_files.items():
        path = generation / relative
        file_state = path.lstat()
        if (
            not stat.S_ISREG(file_state.st_mode)
            or stat.S_ISLNK(file_state.st_mode)
            or file_state.st_uid != os.getuid()
            or stat.S_IMODE(file_state.st_mode) != 0o600
            or path.read_bytes() != payload
        ):
            raise ValueError("existing runtime generation file does not match bundle")


def _validate_current_pointer(current: Path) -> None:
    if not current.is_symlink() and not current.exists():
        return
    if not current.is_symlink():
        raise ValueError("runtime current pointer must be a symlink")
    target = os.readlink(current)
    parts = Path(target).parts
    if (
        len(parts) != 2
        or parts[0] != "generations"
        or _GENERATION_PATTERN.fullmatch(parts[1]) is None
    ):
        raise ValueError("runtime current pointer escapes the generation directory")


def _publish_current(root: Path, *, generation_hash: str) -> None:
    current = root / "current"
    _validate_current_pointer(current)
    target = f"generations/{generation_hash}"
    if current.is_symlink() and os.readlink(current) == target:
        return
    descriptor, temporary_name = tempfile.mkstemp(prefix=".current-", dir=root)
    os.close(descriptor)
    os.unlink(temporary_name)
    temporary = Path(temporary_name)
    try:
        os.symlink(target, temporary)
        _fsync_directory(root)
        os.replace(temporary, current)
        _fsync_directory(root)
    finally:
        with suppress(FileNotFoundError):
            temporary.unlink()


def install_runtime_deployment_bundle(
    runtime_root: Path,
    *,
    producer_commit: str,
    manifests: tuple[RuntimeServiceManifest, ...],
    capability_env: Mapping[str, Mapping[str, str]],
) -> RuntimeDeploymentReceipt:
    """Install one coherent runtime generation without starting any service."""

    if _COMMIT_PATTERN.fullmatch(producer_commit) is None:
        raise ValueError("producer commit must be a full lowercase Git SHA")
    root = _absolute_runtime_root(runtime_root)
    if not isinstance(manifests, tuple) or not manifests:
        raise ValueError("manifests must be a non-empty tuple")

    canonical: list[tuple[RuntimeServiceManifest, bytes]] = [
        _canonical_manifest(manifest) for manifest in manifests
    ]
    service_ids = [manifest.service_id for manifest, _ in canonical]
    if len(service_ids) != len(set(service_ids)):
        raise ValueError("runtime bundle contains duplicate service_id values")
    for manifest, _ in canonical:
        _validate_manifest_authority(
            manifest,
            producer_commit=producer_commit,
            runtime_root=root,
        )
    ordered = tuple(sorted(canonical, key=lambda item: item[0].service_id))
    validated_manifests = tuple(manifest for manifest, _ in ordered)
    capabilities = _validate_capabilities(validated_manifests, capability_env)
    payload_by_service = {manifest.service_id: payload for manifest, payload in ordered}
    _reject_plaintext_secrets(payload_by_service, capabilities)

    instance_mapping = {
        manifest.service_id: _instance_name(manifest.service_id) for manifest in validated_manifests
    }
    unit_mapping = {
        manifest.service_id: _systemd_unit_name(
            manifest,
            instance_mapping[manifest.service_id],
        )
        for manifest in validated_manifests
    }
    files: dict[str, bytes] = {
        "runtime.env": _environment_payload(producer_commit, {}),
    }
    for manifest in validated_manifests:
        instance = instance_mapping[manifest.service_id]
        files[f"manifests/{instance}.json"] = payload_by_service[manifest.service_id]
        if (
            manifest.plane is RuntimeServicePlane.LIVE
            and manifest.service_kind is not RuntimeServiceKind.CANDIDATE_PUBLISHER
        ):
            files[f"secrets/{instance}.env"] = _environment_payload(
                producer_commit,
                capabilities[manifest.service_id],
            )
    generation_hash = canonical_sha256(
        {
            "producer_commit": producer_commit,
            "files": {
                relative: hashlib.sha256(payload).hexdigest()
                for relative, payload in sorted(files.items())
            },
            "instance_mapping": instance_mapping,
            "unit_mapping": unit_mapping,
        }
    )

    _ensure_owned_directory(root)
    for manifest in validated_manifests:
        if manifest.service_kind is not RuntimeServiceKind.CANDIDATE_PUBLISHER:
            continue
        instance = instance_mapping[manifest.service_id]
        _ensure_owned_descendant(root, Path(str(manifest.settings["snapshot_root"])))
        _ensure_owned_descendant(root, root / "control" / "candidates" / instance)
    generations = root / "generations"
    _ensure_owned_directory(generations)
    target = generations / generation_hash
    staging = Path(tempfile.mkdtemp(prefix=".staging-", dir=root))
    staging.chmod(0o700)
    created_target = False
    try:
        for directory_name in ("manifests", "secrets"):
            directory = staging / directory_name
            directory.mkdir(mode=0o700)
            directory.chmod(0o700)
        for relative, payload in sorted(files.items()):
            _write_secure_file(staging / relative, payload)
        _fsync_directory(staging / "manifests")
        _fsync_directory(staging / "secrets")
        _fsync_directory(staging)

        if target.exists() or target.is_symlink():
            _validate_existing_generation(target, files)
        else:
            os.replace(staging, target)
            created_target = True
            _fsync_directory(generations)
        _publish_current(root, generation_hash=generation_hash)
    except BaseException:
        if created_target and not (
            (root / "current").is_symlink()
            and os.readlink(root / "current") == f"generations/{generation_hash}"
        ):
            shutil.rmtree(target)
            _fsync_directory(generations)
        raise
    finally:
        if staging.exists():
            shutil.rmtree(staging)

    return RuntimeDeploymentReceipt(
        runtime_root=root,
        producer_commit=producer_commit,
        generation_hash=generation_hash,
        instance_mapping=instance_mapping,
        unit_mapping=unit_mapping,
    )


__all__ = [
    "RuntimeDeploymentReceipt",
    "install_runtime_deployment_bundle",
]
