"""Strict HYBRID runtime profile and atomic authority-record primitives."""

from __future__ import annotations

import hashlib
import os
import re
import stat
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType

from rquant.strict_json import StrictJsonError, canonical_json_bytes, strict_json_loads

PROFILE_SCHEMA_VERSION = 1
RECORD_SCHEMA_VERSION = 1
MAX_PROFILE_BYTES = 1024 * 1024
MAX_RECORD_BYTES = 512 * 1024

PRODUCTION_PROFILE_PATH = Path("/etc/rquant/production-runtime-profile.json")
PRODUCTION_PROFILE_ANCHOR = Path("/etc/rquant")
PRODUCTION_PROFILE_OWNER_UID = 0
PRODUCTION_PROFILE_MODE = 0o444
PRODUCTION_PROFILE_DIRECTORY_MODE = 0o755

PRODUCTION_SYSTEM_PYTHON = Path("/usr/bin/python3.11")
PRODUCTION_DEPLOY_PYZ = Path("/usr/local/libexec/rquant-production-deploy.pyz")
PRODUCTION_RUNTIME_PYZ = Path("/usr/local/libexec/rquant-runtime-exec.pyz")
PRODUCTION_GENERATION_ROOT = Path("/var/lib/rquant/runtime-authority/generations")

RUNTIME_AUTHORITY_PATH = Path("/var/lib/rquant/runtime-authority/current.json")
RUNTIME_AUTHORITY_ANCHOR = Path("/var/lib/rquant/runtime-authority")
RUNTIME_AUTHORITY_OWNER_UID = 0
RUNTIME_AUTHORITY_RECORD_MODE = 0o444
RUNTIME_AUTHORITY_TEMP_MODE = 0o600
RUNTIME_AUTHORITY_DIRECTORY_MODE = 0o755

_SHA256 = re.compile(r"[0-9a-f]{64}")
_OPERATION_ID = re.compile(r"[0-9a-f]{32}")
_ROLE_NAME = re.compile(r"[a-z][a-z0-9_-]{0,63}")
_MODULE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*")


class RuntimeAuthorityError(RuntimeError):
    """A runtime authority contract failed closed."""


class ProductionRuntimeProfileError(RuntimeAuthorityError):
    """The production runtime closure profile is invalid or unsafe."""


class RuntimeAuthorityRecordError(RuntimeAuthorityError):
    """The single runtime authority record is invalid."""


class RuntimeAuthorityPublishError(RuntimeAuthorityError):
    """Atomic publication or exact temporary recovery failed."""


class RuntimeAuthorityRollbackError(RuntimeAuthorityRecordError):
    """The requested automatic rollback violates the one-level contract."""


class RuntimeAuthorityState(StrEnum):
    ACTIVE = "active"
    ROLLED_BACK = "rolled_back"


@dataclass(frozen=True)
class RuntimeAncestorPolicy:
    path: Path
    owner_uid: int
    mode: int

    def __post_init__(self) -> None:
        path = _model_path(self.path, ProductionRuntimeProfileError, "ancestor path")
        if type(self.owner_uid) is not int or self.owner_uid != 0:
            raise ProductionRuntimeProfileError("runtime ancestor owner UID must be root")
        if not _safe_mode(self.mode):
            raise ProductionRuntimeProfileError("runtime ancestor mode is invalid")
        object.__setattr__(self, "path", path)

    def payload(self) -> dict[str, object]:
        return {"path": str(self.path), "owner_uid": self.owner_uid, "mode": self.mode}


@dataclass(frozen=True)
class RuntimeFilePolicy:
    path: Path
    sha256: str
    owner_uid: int
    mode: int

    def __post_init__(self) -> None:
        path = _model_path(self.path, ProductionRuntimeProfileError, "runtime file path")
        _require_sha256(self.sha256, ProductionRuntimeProfileError, "runtime file SHA256")
        if type(self.owner_uid) is not int or self.owner_uid != 0:
            raise ProductionRuntimeProfileError("runtime file owner UID must be root")
        if not _safe_mode(self.mode):
            raise ProductionRuntimeProfileError("runtime file mode is invalid")
        object.__setattr__(self, "path", path)

    def payload(self) -> dict[str, object]:
        return {
            "path": str(self.path),
            "sha256": self.sha256,
            "owner_uid": self.owner_uid,
            "mode": self.mode,
        }


@dataclass(frozen=True)
class RuntimeClosureProfile:
    profile_id: str
    schema_version: int
    platform: str
    ancestors: tuple[RuntimeAncestorPolicy, ...]
    system_python: RuntimeFilePolicy
    elf_loader: RuntimeFilePolicy
    stdlib: tuple[RuntimeFilePolicy, ...]
    shared_libraries: tuple[RuntimeFilePolicy, ...]
    deploy_pyz: RuntimeFilePolicy
    runtime_pyz: RuntimeFilePolicy

    def __post_init__(self) -> None:
        if type(self.schema_version) is not int or self.schema_version != PROFILE_SCHEMA_VERSION:
            raise ProductionRuntimeProfileError("runtime profile schema is unsupported")
        if type(self.platform) is not str or self.platform != "linux":
            raise ProductionRuntimeProfileError("runtime profile platform must be linux")
        if not self.ancestors or not all(
            isinstance(item, RuntimeAncestorPolicy) for item in self.ancestors
        ):
            raise ProductionRuntimeProfileError("runtime ancestor policy is incomplete")
        if tuple(sorted(self.ancestors, key=lambda item: str(item.path))) != self.ancestors:
            raise ProductionRuntimeProfileError("runtime ancestor policy is not canonical")
        ancestor_paths = tuple(item.path for item in self.ancestors)
        if len(ancestor_paths) != len(set(ancestor_paths)):
            raise ProductionRuntimeProfileError("runtime ancestor policy contains duplicates")
        files = self.files
        if len(files) != len({item.path for item in files}):
            raise ProductionRuntimeProfileError("runtime closure contains duplicate file paths")
        if not self.stdlib or not self.shared_libraries:
            raise ProductionRuntimeProfileError("runtime closure file lists are incomplete")
        if tuple(sorted(self.stdlib, key=lambda item: str(item.path))) != self.stdlib:
            raise ProductionRuntimeProfileError("runtime stdlib list is not canonical")
        if (
            tuple(sorted(self.shared_libraries, key=lambda item: str(item.path)))
            != self.shared_libraries
        ):
            raise ProductionRuntimeProfileError("runtime shared library list is not canonical")
        if self.system_python.path != PRODUCTION_SYSTEM_PYTHON:
            raise ProductionRuntimeProfileError(
                "system Python path is not the fixed production path"
            )
        if self.system_python.mode != 0o555:
            raise ProductionRuntimeProfileError("system Python mode is not fixed at 0555")
        if self.deploy_pyz.path != PRODUCTION_DEPLOY_PYZ or self.deploy_pyz.mode != 0o555:
            raise ProductionRuntimeProfileError("deploy pyz policy is not fixed")
        if self.runtime_pyz.path != PRODUCTION_RUNTIME_PYZ or self.runtime_pyz.mode != 0o555:
            raise ProductionRuntimeProfileError("runtime pyz policy is not fixed")
        required_ancestors = {parent for item in files for parent in item.path.parents}
        if set(ancestor_paths) != required_ancestors:
            raise ProductionRuntimeProfileError("runtime ancestor policy does not close every path")
        expected_id = hashlib.sha256(canonical_json_bytes(self.body())).hexdigest()
        if type(self.profile_id) is not str or self.profile_id != expected_id:
            raise ProductionRuntimeProfileError("runtime profile id does not match its content")

    @property
    def files(self) -> tuple[RuntimeFilePolicy, ...]:
        return (
            self.system_python,
            self.elf_loader,
            *self.stdlib,
            *self.shared_libraries,
            self.deploy_pyz,
            self.runtime_pyz,
        )

    def body(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "platform": self.platform,
            "ancestors": [item.payload() for item in self.ancestors],
            "system_python": self.system_python.payload(),
            "elf_loader": self.elf_loader.payload(),
            "stdlib": [item.payload() for item in self.stdlib],
            "shared_libraries": [item.payload() for item in self.shared_libraries],
            "deploy_pyz": self.deploy_pyz.payload(),
            "runtime_pyz": self.runtime_pyz.payload(),
        }


@dataclass(frozen=True)
class RuntimeRoleSpec:
    python_path: Path
    module: str
    working_directory: Path
    app_source: Path
    site_packages: tuple[Path, ...]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "python_path",
            _model_path(self.python_path, RuntimeAuthorityRecordError, "role path"),
        )
        object.__setattr__(
            self,
            "working_directory",
            _model_path(self.working_directory, RuntimeAuthorityRecordError, "role path"),
        )
        object.__setattr__(
            self,
            "app_source",
            _model_path(self.app_source, RuntimeAuthorityRecordError, "role path"),
        )
        if type(self.module) is not str or _MODULE_NAME.fullmatch(self.module) is None:
            raise RuntimeAuthorityRecordError("runtime role module is invalid")
        if not self.site_packages or not all(isinstance(path, Path) for path in self.site_packages):
            raise RuntimeAuthorityRecordError("runtime role site-packages are invalid")
        paths = tuple(
            _model_path(path, RuntimeAuthorityRecordError, "role path")
            for path in self.site_packages
        )
        if paths != tuple(sorted(set(paths), key=str)):
            raise RuntimeAuthorityRecordError("runtime role site-packages are not canonical")
        object.__setattr__(self, "site_packages", paths)

    def validate_for_generation(self, generation: Path) -> None:
        for path in (
            self.python_path,
            self.working_directory,
            self.app_source,
            *self.site_packages,
        ):
            _require_resolved_child(path, generation, "runtime role path")

    def payload(self) -> dict[str, object]:
        return {
            "python_path": str(self.python_path),
            "module": self.module,
            "working_directory": str(self.working_directory),
            "app_source": str(self.app_source),
            "site_packages": [str(path) for path in self.site_packages],
        }


@dataclass(frozen=True)
class RuntimeGenerationSlot:
    generation_id: str
    generation_path: Path
    commit: str
    full_manifest_hash: str
    profile_id: str
    roles: Mapping[str, RuntimeRoleSpec]

    def __post_init__(self) -> None:
        _require_sha256(self.generation_id, RuntimeAuthorityRecordError, "generation id")
        _require_sha256(
            self.full_manifest_hash,
            RuntimeAuthorityRecordError,
            "full manifest hash",
        )
        if self.generation_id != self.full_manifest_hash:
            raise RuntimeAuthorityRecordError("generation identity differs from full manifest hash")
        _require_sha256(self.profile_id, RuntimeAuthorityRecordError, "profile id")
        generation_path = _model_path(
            self.generation_path,
            RuntimeAuthorityRecordError,
            "generation path",
        )
        if (
            type(self.commit) is not str
            or not self.commit
            or len(self.commit.encode("utf-8")) > 512
            or any(ord(character) < 0x20 for character in self.commit)
        ):
            raise RuntimeAuthorityRecordError("untrusted commit audit metadata is invalid")
        if not isinstance(self.roles, Mapping) or not self.roles:
            raise RuntimeAuthorityRecordError("runtime roles are incomplete")
        roles: dict[str, RuntimeRoleSpec] = {}
        for name, role in self.roles.items():
            if type(name) is not str or _ROLE_NAME.fullmatch(name) is None:
                raise RuntimeAuthorityRecordError("runtime role name is invalid")
            if not isinstance(role, RuntimeRoleSpec):
                raise RuntimeAuthorityRecordError("runtime role schema is invalid")
            roles[name] = role
        object.__setattr__(self, "generation_path", generation_path)
        object.__setattr__(self, "roles", MappingProxyType(dict(sorted(roles.items()))))

    def validate_for_root(self, generation_root: Path) -> None:
        root = _model_path(
            generation_root,
            RuntimeAuthorityRecordError,
            "generation root",
        )
        expected = root / self.generation_id
        if self.generation_path != expected:
            raise RuntimeAuthorityRecordError("generation path is outside the fixed root")
        _require_resolved_child(self.generation_path, root, "generation symbolic path", direct=True)
        for role in self.roles.values():
            role.validate_for_generation(self.generation_path)

    def payload(self) -> dict[str, object]:
        return {
            "generation_id": self.generation_id,
            "generation_path": str(self.generation_path),
            "commit": self.commit,
            "full_manifest_hash": self.full_manifest_hash,
            "profile_id": self.profile_id,
            "roles": {name: role.payload() for name, role in self.roles.items()},
        }


@dataclass(frozen=True)
class RuntimeAuthorityRecord:
    schema_version: int
    operation_id: str
    state: RuntimeAuthorityState
    current: RuntimeGenerationSlot
    prior: RuntimeGenerationSlot | None

    def __post_init__(self) -> None:
        if type(self.schema_version) is not int or self.schema_version != RECORD_SCHEMA_VERSION:
            raise RuntimeAuthorityRecordError("runtime authority schema is unsupported")
        _require_operation_id(self.operation_id)
        if not isinstance(self.state, RuntimeAuthorityState):
            raise RuntimeAuthorityRecordError("runtime authority state is invalid")
        if not isinstance(self.current, RuntimeGenerationSlot):
            raise RuntimeAuthorityRecordError("current runtime slot is invalid")
        if self.prior is not None and not isinstance(self.prior, RuntimeGenerationSlot):
            raise RuntimeAuthorityRecordError("prior runtime slot is invalid")
        if self.prior is not None and self.current.generation_id == self.prior.generation_id:
            raise RuntimeAuthorityRecordError("current and prior name the same generation")
        if self.state is RuntimeAuthorityState.ROLLED_BACK and self.prior is None:
            raise RuntimeAuthorityRecordError("rolled-back authority requires a prior slot")

    def validate_for_root(self, generation_root: Path) -> None:
        self.current.validate_for_root(generation_root)
        if self.prior is not None:
            self.prior.validate_for_root(generation_root)

    def payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema_version": self.schema_version,
            "operation_id": self.operation_id,
            "state": self.state.value,
        }
        for prefix, slot in (("current", self.current), ("prior", self.prior)):
            slot_payload = None if slot is None else slot.payload()
            for field in _SLOT_FIELDS:
                payload[f"{prefix}_{field}"] = None if slot_payload is None else slot_payload[field]
        return payload


_PROFILE_FIELDS = {
    "profile_id",
    "schema_version",
    "platform",
    "ancestors",
    "system_python",
    "elf_loader",
    "stdlib",
    "shared_libraries",
    "deploy_pyz",
    "runtime_pyz",
}
_FILE_FIELDS = {"path", "sha256", "owner_uid", "mode"}
_ANCESTOR_FIELDS = {"path", "owner_uid", "mode"}
_SLOT_FIELDS = (
    "generation_id",
    "generation_path",
    "commit",
    "full_manifest_hash",
    "profile_id",
    "roles",
)
_RECORD_FIELDS = {
    "schema_version",
    "operation_id",
    "state",
    *(f"{prefix}_{field}" for prefix in ("current", "prior") for field in _SLOT_FIELDS),
}
_ROLE_FIELDS = {
    "python_path",
    "module",
    "working_directory",
    "app_source",
    "site_packages",
}


def parse_runtime_closure_profile(payload: str | bytes | bytearray) -> RuntimeClosureProfile:
    data = _strict_payload(
        payload,
        max_bytes=MAX_PROFILE_BYTES,
        error_type=ProductionRuntimeProfileError,
        label="runtime profile",
    )
    if type(data) is not dict or set(data) != _PROFILE_FIELDS:
        raise ProductionRuntimeProfileError("runtime profile has unexpected fields")
    ancestors = data["ancestors"]
    stdlib = data["stdlib"]
    shared_libraries = data["shared_libraries"]
    if (
        type(ancestors) is not list
        or type(stdlib) is not list
        or type(shared_libraries) is not list
    ):
        raise ProductionRuntimeProfileError("runtime profile closure lists are invalid")
    return RuntimeClosureProfile(
        profile_id=data["profile_id"],
        schema_version=data["schema_version"],
        platform=data["platform"],
        ancestors=tuple(_parse_ancestor(item) for item in ancestors),
        system_python=_parse_file(data["system_python"], label="system Python"),
        elf_loader=_parse_file(data["elf_loader"], label="ELF loader"),
        stdlib=tuple(_parse_file(item, label="stdlib") for item in stdlib),
        shared_libraries=tuple(
            _parse_file(item, label="shared library") for item in shared_libraries
        ),
        deploy_pyz=_parse_file(data["deploy_pyz"], label="deploy pyz"),
        runtime_pyz=_parse_file(data["runtime_pyz"], label="runtime pyz"),
    )


def load_production_runtime_profile() -> RuntimeClosureProfile:
    payload = _read_trusted_file(
        PRODUCTION_PROFILE_PATH,
        anchor=PRODUCTION_PROFILE_ANCHOR,
        owner_uid=PRODUCTION_PROFILE_OWNER_UID,
        directory_mode=PRODUCTION_PROFILE_DIRECTORY_MODE,
        file_mode=PRODUCTION_PROFILE_MODE,
        max_bytes=MAX_PROFILE_BYTES,
        error_type=ProductionRuntimeProfileError,
        label="production runtime profile",
    )
    return parse_runtime_closure_profile(payload)


def parse_runtime_authority_record(
    payload: str | bytes | bytearray,
    *,
    generation_root: Path | None = None,
) -> RuntimeAuthorityRecord:
    data = _strict_payload(
        payload,
        max_bytes=MAX_RECORD_BYTES,
        error_type=RuntimeAuthorityRecordError,
        label="runtime authority record",
    )
    if type(data) is not dict or set(data) != _RECORD_FIELDS:
        raise RuntimeAuthorityRecordError("runtime authority record has unexpected fields")
    state = data["state"]
    if type(state) is not str:
        raise RuntimeAuthorityRecordError("runtime authority state is invalid")
    try:
        parsed_state = RuntimeAuthorityState(state)
    except ValueError as exc:
        raise RuntimeAuthorityRecordError("runtime authority state is invalid") from exc
    current = _parse_slot(data, prefix="current")
    prior_values = tuple(data[f"prior_{field}"] for field in _SLOT_FIELDS)
    if all(value is None for value in prior_values):
        prior = None
    elif any(value is None for value in prior_values):
        raise RuntimeAuthorityRecordError("prior slot must be complete or explicitly absent")
    else:
        prior = _parse_slot(data, prefix="prior")
    record = RuntimeAuthorityRecord(
        schema_version=data["schema_version"],
        operation_id=data["operation_id"],
        state=parsed_state,
        current=current,
        prior=prior,
    )
    record.validate_for_root(
        PRODUCTION_GENERATION_ROOT if generation_root is None else generation_root
    )
    return record


def canonical_runtime_authority_bytes(record: RuntimeAuthorityRecord) -> bytes:
    if not isinstance(record, RuntimeAuthorityRecord):
        raise RuntimeAuthorityRecordError("runtime authority record model is required")
    return canonical_json_bytes(record.payload(), trailing_newline=True)


def load_runtime_authority() -> RuntimeAuthorityRecord:
    payload = _read_trusted_file(
        RUNTIME_AUTHORITY_PATH,
        anchor=RUNTIME_AUTHORITY_ANCHOR,
        owner_uid=RUNTIME_AUTHORITY_OWNER_UID,
        directory_mode=RUNTIME_AUTHORITY_DIRECTORY_MODE,
        file_mode=RUNTIME_AUTHORITY_RECORD_MODE,
        max_bytes=MAX_RECORD_BYTES,
        error_type=RuntimeAuthorityRecordError,
        label="runtime authority record",
    )
    return parse_runtime_authority_record(payload)


def prepare_runtime_authority_publish(
    previous: RuntimeAuthorityRecord | None,
    next_generation: RuntimeGenerationSlot,
    *,
    operation_id: str,
) -> RuntimeAuthorityRecord:
    _require_operation_id(operation_id)
    if not isinstance(next_generation, RuntimeGenerationSlot):
        raise RuntimeAuthorityRecordError("next runtime generation is invalid")
    if previous is None:
        return RuntimeAuthorityRecord(
            schema_version=RECORD_SCHEMA_VERSION,
            operation_id=operation_id,
            state=RuntimeAuthorityState.ACTIVE,
            current=next_generation,
            prior=None,
        )
    _require_newer_operation(previous.operation_id, operation_id)
    recorded = {previous.current.generation_id}
    if previous.prior is not None:
        recorded.add(previous.prior.generation_id)
    if next_generation.generation_id in recorded:
        raise RuntimeAuthorityRecordError("next generation is already recorded")
    return RuntimeAuthorityRecord(
        schema_version=RECORD_SCHEMA_VERSION,
        operation_id=operation_id,
        state=RuntimeAuthorityState.ACTIVE,
        current=next_generation,
        prior=previous.current,
    )


def prepare_runtime_authority_rollback(
    previous: RuntimeAuthorityRecord,
    *,
    operation_id: str,
) -> RuntimeAuthorityRecord:
    if not isinstance(previous, RuntimeAuthorityRecord):
        raise RuntimeAuthorityRollbackError("runtime authority record is required")
    _require_operation_id(operation_id)
    _require_newer_operation(previous.operation_id, operation_id)
    if previous.state is RuntimeAuthorityState.ROLLED_BACK:
        raise RuntimeAuthorityRollbackError("automatic rollback is single-level")
    if previous.prior is None:
        raise RuntimeAuthorityRollbackError("automatic rollback requires a prior generation")
    return RuntimeAuthorityRecord(
        schema_version=RECORD_SCHEMA_VERSION,
        operation_id=operation_id,
        state=RuntimeAuthorityState.ROLLED_BACK,
        current=previous.prior,
        prior=previous.current,
    )


def publish_runtime_authority(record: RuntimeAuthorityRecord) -> None:
    if not isinstance(record, RuntimeAuthorityRecord):
        raise RuntimeAuthorityPublishError("runtime authority record model is required")
    try:
        record.validate_for_root(PRODUCTION_GENERATION_ROOT)
    except RuntimeAuthorityRecordError as exc:
        raise RuntimeAuthorityPublishError(
            "runtime authority record violates production root"
        ) from exc
    payload = canonical_runtime_authority_bytes(record)
    descriptors: list[int] = []
    temporary_fd = -1
    temporary_name = _temporary_name(record.operation_id)
    try:
        descriptors, parent_fd, target_name = _open_trusted_parent(
            RUNTIME_AUTHORITY_PATH,
            anchor=RUNTIME_AUTHORITY_ANCHOR,
            owner_uid=RUNTIME_AUTHORITY_OWNER_UID,
            directory_mode=RUNTIME_AUTHORITY_DIRECTORY_MODE,
            error_type=RuntimeAuthorityPublishError,
            label="runtime authority directory",
        )
        previous = _read_existing_authority(parent_fd, target_name)
        _require_publish_transition(previous, record)
        flags = (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | _required_flag("O_NOFOLLOW", RuntimeAuthorityPublishError)
            | getattr(os, "O_CLOEXEC", 0)
        )
        temporary_fd = os.open(
            temporary_name,
            flags,
            RUNTIME_AUTHORITY_TEMP_MODE,
            dir_fd=parent_fd,
        )
        _write_all(temporary_fd, payload)
        observed = os.fstat(temporary_fd)
        if observed.st_uid != RUNTIME_AUTHORITY_OWNER_UID:
            os.fchown(temporary_fd, RUNTIME_AUTHORITY_OWNER_UID, -1)
        os.fchmod(temporary_fd, RUNTIME_AUTHORITY_RECORD_MODE)
        _require_file_stat(
            os.fstat(temporary_fd),
            owner_uid=RUNTIME_AUTHORITY_OWNER_UID,
            mode=RUNTIME_AUTHORITY_RECORD_MODE,
            error_type=RuntimeAuthorityPublishError,
            label="runtime authority temporary",
        )
        _fsync_descriptor(temporary_fd, phase_name="temp_fsync")
        os.close(temporary_fd)
        temporary_fd = -1
        _require_file_stat(
            os.stat(temporary_name, dir_fd=parent_fd, follow_symlinks=False),
            owner_uid=RUNTIME_AUTHORITY_OWNER_UID,
            mode=RUNTIME_AUTHORITY_RECORD_MODE,
            error_type=RuntimeAuthorityPublishError,
            label="runtime authority temporary",
        )
        _replace_record(parent_fd, temporary_name)
        _fsync_descriptor(parent_fd, phase_name="parent_fsync")
    except RuntimeAuthorityPublishError:
        raise
    except (OSError, RuntimeAuthorityRecordError) as exc:
        raise RuntimeAuthorityPublishError("runtime authority publication failed") from exc
    finally:
        if temporary_fd >= 0:
            with suppress(OSError):
                os.close(temporary_fd)
        _close_descriptors(descriptors)


def cleanup_runtime_authority_temp(operation_id: str) -> bool:
    try:
        _require_operation_id(operation_id)
    except RuntimeAuthorityRecordError as exc:
        raise RuntimeAuthorityPublishError(
            "runtime authority temporary operation is invalid"
        ) from exc
    descriptors: list[int] = []
    try:
        descriptors, parent_fd, _target_name = _open_trusted_parent(
            RUNTIME_AUTHORITY_PATH,
            anchor=RUNTIME_AUTHORITY_ANCHOR,
            owner_uid=RUNTIME_AUTHORITY_OWNER_UID,
            directory_mode=RUNTIME_AUTHORITY_DIRECTORY_MODE,
            error_type=RuntimeAuthorityPublishError,
            label="runtime authority directory",
        )
        name = _temporary_name(operation_id)
        try:
            observed = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            return False
        if (
            not stat.S_ISREG(observed.st_mode)
            or stat.S_ISLNK(observed.st_mode)
            or observed.st_uid != RUNTIME_AUTHORITY_OWNER_UID
            or observed.st_nlink != 1
            or stat.S_IMODE(observed.st_mode)
            not in {RUNTIME_AUTHORITY_TEMP_MODE, RUNTIME_AUTHORITY_RECORD_MODE}
        ):
            raise RuntimeAuthorityPublishError("runtime authority temporary is unsafe")
        os.unlink(name, dir_fd=parent_fd)
        _fsync_descriptor(parent_fd, phase_name="parent_fsync")
        return True
    except RuntimeAuthorityPublishError:
        raise
    except OSError as exc:
        raise RuntimeAuthorityPublishError("runtime authority temporary cleanup failed") from exc
    finally:
        _close_descriptors(descriptors)


def _parse_ancestor(payload: object) -> RuntimeAncestorPolicy:
    if type(payload) is not dict or set(payload) != _ANCESTOR_FIELDS:
        raise ProductionRuntimeProfileError("runtime ancestor schema is invalid")
    return RuntimeAncestorPolicy(
        path=_payload_path(payload["path"], ProductionRuntimeProfileError, "ancestor path"),
        owner_uid=payload["owner_uid"],
        mode=payload["mode"],
    )


def _parse_file(payload: object, *, label: str) -> RuntimeFilePolicy:
    if type(payload) is not dict or set(payload) != _FILE_FIELDS:
        raise ProductionRuntimeProfileError(f"{label} schema is invalid")
    try:
        return RuntimeFilePolicy(
            path=_payload_path(payload["path"], ProductionRuntimeProfileError, f"{label} path"),
            sha256=payload["sha256"],
            owner_uid=payload["owner_uid"],
            mode=payload["mode"],
        )
    except ProductionRuntimeProfileError as exc:
        raise ProductionRuntimeProfileError(f"{label} policy is invalid: {exc}") from exc


def _parse_slot(payload: dict[str, object], *, prefix: str) -> RuntimeGenerationSlot:
    roles = payload[f"{prefix}_roles"]
    if type(roles) is not dict or not roles:
        raise RuntimeAuthorityRecordError(f"{prefix} slot roles are invalid")
    parsed_roles = {name: _parse_role(role) for name, role in roles.items()}
    return RuntimeGenerationSlot(
        generation_id=payload[f"{prefix}_generation_id"],
        generation_path=_payload_path(
            payload[f"{prefix}_generation_path"],
            RuntimeAuthorityRecordError,
            "generation path",
        ),
        commit=payload[f"{prefix}_commit"],
        full_manifest_hash=payload[f"{prefix}_full_manifest_hash"],
        profile_id=payload[f"{prefix}_profile_id"],
        roles=parsed_roles,
    )


def _parse_role(payload: object) -> RuntimeRoleSpec:
    if type(payload) is not dict or set(payload) != _ROLE_FIELDS:
        raise RuntimeAuthorityRecordError("runtime role schema is invalid")
    site_packages = payload["site_packages"]
    if type(site_packages) is not list:
        raise RuntimeAuthorityRecordError("runtime role site-packages are invalid")
    return RuntimeRoleSpec(
        python_path=_payload_path(payload["python_path"], RuntimeAuthorityRecordError, "role path"),
        module=payload["module"],
        working_directory=_payload_path(
            payload["working_directory"], RuntimeAuthorityRecordError, "role path"
        ),
        app_source=_payload_path(payload["app_source"], RuntimeAuthorityRecordError, "role path"),
        site_packages=tuple(
            _payload_path(path, RuntimeAuthorityRecordError, "role path") for path in site_packages
        ),
    )


def _strict_payload(
    payload: str | bytes | bytearray,
    *,
    max_bytes: int,
    error_type: type[RuntimeAuthorityError],
    label: str,
) -> object:
    if not isinstance(payload, (str, bytes, bytearray)):
        raise error_type(f"{label} must be JSON bytes or text")
    try:
        encoded = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
    except (UnicodeEncodeError, ValueError) as exc:
        raise error_type(f"{label} is not valid UTF-8") from exc
    if len(encoded) > max_bytes:
        raise error_type(f"{label} is too large")

    def reject_number(value: str) -> object:
        raise StrictJsonError(f"non-native JSON number: {value}")

    try:
        return strict_json_loads(
            encoded,
            parse_float=reject_number,
            parse_constant=reject_number,
        )
    except (StrictJsonError, UnicodeDecodeError) as exc:
        raise error_type(f"{label} is not strict JSON") from exc


def _payload_path(
    value: object,
    error_type: type[RuntimeAuthorityError],
    label: str,
) -> Path:
    if type(value) is not str:
        raise error_type(f"{label} is invalid")
    return _model_path(Path(value), error_type, label)


def _model_path(
    value: object,
    error_type: type[RuntimeAuthorityError],
    label: str,
) -> Path:
    if not isinstance(value, Path) or not value.is_absolute() or "\x00" in str(value):
        raise error_type(f"{label} must be one absolute canonical path")
    canonical = Path(os.path.abspath(value))
    if value != canonical:
        raise error_type(f"{label} must be one absolute canonical path")
    return canonical


def _require_resolved_child(
    path: Path,
    root: Path,
    label: str,
    *,
    direct: bool = False,
) -> None:
    try:
        relative = path.relative_to(root)
    except ValueError as exc:
        raise RuntimeAuthorityRecordError(f"{label} escapes its generation") from exc
    if not relative.parts:
        raise RuntimeAuthorityRecordError(f"{label} must be below its generation")
    if direct and len(relative.parts) != 1:
        raise RuntimeAuthorityRecordError("generation path is not one content-addressed child")
    resolved_root = root.resolve(strict=False)
    resolved_path = path.resolve(strict=False)
    expected = resolved_root.joinpath(*relative.parts)
    if resolved_path != expected:
        raise RuntimeAuthorityRecordError(f"{label} contains a symbolic escape")


def _safe_mode(value: object) -> bool:
    return type(value) is int and 0 <= value <= 0o777 and not value & 0o022


def _require_sha256(
    value: object,
    error_type: type[RuntimeAuthorityError],
    label: str,
) -> None:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise error_type(f"{label} is invalid")


def _require_operation_id(value: object) -> None:
    if type(value) is not str or _OPERATION_ID.fullmatch(value) is None:
        raise RuntimeAuthorityRecordError("operation id must be 32 lowercase hexadecimal bytes")


def _require_newer_operation(previous: str, candidate: str) -> None:
    if candidate <= previous:
        raise RuntimeAuthorityRecordError("operation id must be unique and monotonic")


def _identity(observed: os.stat_result) -> tuple[int, ...]:
    return (
        observed.st_dev,
        observed.st_ino,
        observed.st_mode,
        observed.st_uid,
        observed.st_nlink,
        observed.st_size,
        observed.st_mtime_ns,
        observed.st_ctime_ns,
    )


def _required_flag(name: str, error_type: type[RuntimeAuthorityError]) -> int:
    value = getattr(os, name, 0)
    if not value:
        raise error_type(f"platform lacks required {name} filesystem capability")
    return value


def _require_directory_stat(
    observed: os.stat_result,
    *,
    owner_uid: int,
    mode: int,
    error_type: type[RuntimeAuthorityError],
    label: str,
) -> None:
    if (
        not stat.S_ISDIR(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != owner_uid
        or stat.S_IMODE(observed.st_mode) != mode
        or observed.st_mode & 0o022
    ):
        raise error_type(f"{label} is unsafe")


def _require_file_stat(
    observed: os.stat_result,
    *,
    owner_uid: int,
    mode: int,
    error_type: type[RuntimeAuthorityError],
    label: str,
) -> None:
    if (
        not stat.S_ISREG(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != owner_uid
        or stat.S_IMODE(observed.st_mode) != mode
        or observed.st_nlink != 1
    ):
        raise error_type(f"{label} is unsafe")


def _open_trusted_parent(
    path: Path,
    *,
    anchor: Path,
    owner_uid: int,
    directory_mode: int,
    error_type: type[RuntimeAuthorityError],
    label: str,
) -> tuple[list[int], int, str]:
    path = _model_path(path, error_type, label)
    anchor = _model_path(anchor, error_type, f"{label} anchor")
    try:
        relative = path.relative_to(anchor)
    except ValueError as exc:
        raise error_type(f"{label} path escapes its fixed anchor") from exc
    if not relative.parts:
        raise error_type(f"{label} path must be below its anchor")
    flags = (
        os.O_RDONLY
        | _required_flag("O_DIRECTORY", error_type)
        | _required_flag("O_NOFOLLOW", error_type)
        | getattr(os, "O_CLOEXEC", 0)
    )
    descriptors: list[int] = []
    try:
        named_anchor = os.stat(anchor, follow_symlinks=False)
        _require_directory_stat(
            named_anchor,
            owner_uid=owner_uid,
            mode=directory_mode,
            error_type=error_type,
            label=f"{label} anchor",
        )
        anchor_fd = os.open(anchor, flags)
        descriptors.append(anchor_fd)
        if _identity(os.fstat(anchor_fd)) != _identity(named_anchor):
            raise error_type(f"{label} anchor identity changed")
        parent_fd = anchor_fd
        for component in relative.parts[:-1]:
            named = os.stat(component, dir_fd=parent_fd, follow_symlinks=False)
            _require_directory_stat(
                named,
                owner_uid=owner_uid,
                mode=directory_mode,
                error_type=error_type,
                label=label,
            )
            child_fd = os.open(component, flags, dir_fd=parent_fd)
            descriptors.append(child_fd)
            if _identity(os.fstat(child_fd)) != _identity(named):
                raise error_type(f"{label} identity changed")
            parent_fd = child_fd
        return descriptors, parent_fd, relative.parts[-1]
    except BaseException:
        _close_descriptors(descriptors)
        raise


def _read_trusted_file(
    path: Path,
    *,
    anchor: Path,
    owner_uid: int,
    directory_mode: int,
    file_mode: int,
    max_bytes: int,
    error_type: type[RuntimeAuthorityError],
    label: str,
) -> bytes:
    descriptors: list[int] = []
    file_fd = -1
    try:
        descriptors, parent_fd, name = _open_trusted_parent(
            path,
            anchor=anchor,
            owner_uid=owner_uid,
            directory_mode=directory_mode,
            error_type=error_type,
            label=label,
        )
        named = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        _require_file_stat(
            named,
            owner_uid=owner_uid,
            mode=file_mode,
            error_type=error_type,
            label=label,
        )
        file_fd = os.open(
            name,
            os.O_RDONLY | _required_flag("O_NOFOLLOW", error_type) | getattr(os, "O_CLOEXEC", 0),
            dir_fd=parent_fd,
        )
        before = os.fstat(file_fd)
        if _identity(before) != _identity(named):
            raise error_type(f"{label} identity changed while opening")
        payload = _read_limited(file_fd, max_bytes=max_bytes, error_type=error_type, label=label)
        after = os.fstat(file_fd)
        active = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if _identity(before) != _identity(after) or _identity(before) != _identity(active):
            raise error_type(f"{label} identity changed while reading")
        return payload
    except error_type:
        raise
    except OSError as exc:
        raise error_type(f"{label} is unavailable") from exc
    finally:
        if file_fd >= 0:
            with suppress(OSError):
                os.close(file_fd)
        _close_descriptors(descriptors)


def _read_limited(
    descriptor: int,
    *,
    max_bytes: int,
    error_type: type[RuntimeAuthorityError],
    label: str,
) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = os.read(descriptor, min(64 * 1024, max_bytes + 1 - total))
        if not chunk:
            return b"".join(chunks)
        chunks.append(chunk)
        total += len(chunk)
        if total > max_bytes:
            raise error_type(f"{label} is too large")


def _read_existing_authority(parent_fd: int, target_name: str) -> RuntimeAuthorityRecord | None:
    try:
        observed = os.stat(target_name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    _require_file_stat(
        observed,
        owner_uid=RUNTIME_AUTHORITY_OWNER_UID,
        mode=RUNTIME_AUTHORITY_RECORD_MODE,
        error_type=RuntimeAuthorityPublishError,
        label="existing runtime authority record",
    )
    descriptor = -1
    try:
        descriptor = os.open(
            target_name,
            os.O_RDONLY
            | _required_flag("O_NOFOLLOW", RuntimeAuthorityPublishError)
            | getattr(os, "O_CLOEXEC", 0),
            dir_fd=parent_fd,
        )
        before = os.fstat(descriptor)
        if _identity(before) != _identity(observed):
            raise RuntimeAuthorityPublishError("existing runtime authority identity changed")
        payload = _read_limited(
            descriptor,
            max_bytes=MAX_RECORD_BYTES,
            error_type=RuntimeAuthorityPublishError,
            label="existing runtime authority record",
        )
        after = os.fstat(descriptor)
        active = os.stat(target_name, dir_fd=parent_fd, follow_symlinks=False)
        if _identity(before) != _identity(after) or _identity(before) != _identity(active):
            raise RuntimeAuthorityPublishError("existing runtime authority identity changed")
        return parse_runtime_authority_record(payload)
    finally:
        if descriptor >= 0:
            with suppress(OSError):
                os.close(descriptor)


def _require_publish_transition(
    previous: RuntimeAuthorityRecord | None,
    candidate: RuntimeAuthorityRecord,
) -> None:
    if previous is None:
        if candidate.state is not RuntimeAuthorityState.ACTIVE or candidate.prior is not None:
            raise RuntimeAuthorityPublishError("first authority record must have no prior slot")
        return
    try:
        _require_newer_operation(previous.operation_id, candidate.operation_id)
    except RuntimeAuthorityRecordError as exc:
        raise RuntimeAuthorityPublishError("authority operation is not monotonic") from exc
    if candidate.state is RuntimeAuthorityState.ACTIVE:
        valid = candidate.prior == previous.current
    else:
        valid = (
            previous.state is RuntimeAuthorityState.ACTIVE
            and previous.prior is not None
            and candidate.current == previous.prior
            and candidate.prior == previous.current
        )
    if not valid:
        raise RuntimeAuthorityPublishError("authority record is not one valid state transition")


def _temporary_name(operation_id: str) -> str:
    return f".current.{operation_id}.tmp"


def _write_all(descriptor: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = os.write(descriptor, payload[offset:])
        if written <= 0:
            raise OSError("authority record write made no progress")
        offset += written


def _fsync_descriptor(descriptor: int, *, phase_name: str) -> None:
    del phase_name
    os.fsync(descriptor)


def _replace_record(parent_fd: int, temporary_name: str) -> None:
    os.replace(
        temporary_name,
        RUNTIME_AUTHORITY_PATH.name,
        src_dir_fd=parent_fd,
        dst_dir_fd=parent_fd,
    )


def _close_descriptors(descriptors: list[int]) -> None:
    for descriptor in reversed(descriptors):
        with suppress(OSError):
            os.close(descriptor)


__all__ = [
    "ProductionRuntimeProfileError",
    "RuntimeAncestorPolicy",
    "RuntimeAuthorityError",
    "RuntimeAuthorityPublishError",
    "RuntimeAuthorityRecord",
    "RuntimeAuthorityRecordError",
    "RuntimeAuthorityRollbackError",
    "RuntimeAuthorityState",
    "RuntimeClosureProfile",
    "RuntimeFilePolicy",
    "RuntimeGenerationSlot",
    "RuntimeRoleSpec",
    "canonical_runtime_authority_bytes",
    "cleanup_runtime_authority_temp",
    "load_production_runtime_profile",
    "load_runtime_authority",
    "parse_runtime_authority_record",
    "parse_runtime_closure_profile",
    "prepare_runtime_authority_publish",
    "prepare_runtime_authority_rollback",
    "publish_runtime_authority",
]
