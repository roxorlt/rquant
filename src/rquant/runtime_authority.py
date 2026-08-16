"""Strict HYBRID runtime profile and atomic authority-record primitives."""

from __future__ import annotations

import fcntl
import hashlib
import importlib.machinery
import os
import re
import stat
import unicodedata
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass, replace
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType

from rquant.strict_json import StrictJsonError, canonical_json_bytes, strict_json_loads

PROFILE_SCHEMA_VERSION = 1
RECORD_SCHEMA_VERSION = 1
MAX_PROFILE_BYTES = 1024 * 1024
MAX_RECORD_BYTES = 512 * 1024
MAX_PROFILE_ANCESTORS = 256
MAX_CLOSURE_FILES = 4096
MAX_ROLES = 32
MAX_SITE_PACKAGES = 8
MAX_COMMIT_BYTES = 512
MAX_PATH_BYTES = 4096
MAX_MODULE_BYTES = 128
MAX_ENVIRONMENT_NAMES = 32
MAX_ENVIRONMENT_NAME_BYTES = 64
MAX_GENERATION_MANIFEST_BYTES = 16 * 1024 * 1024
MAX_JSON_NESTING_DEPTH = 64
MAX_JSON_INTEGER_DIGITS = 20
FILE_HASH_CHUNK_BYTES = 64 * 1024
MAX_PYVENV_CONFIG_BYTES = 4096

PRODUCTION_PROFILE_PATH = Path("/etc/rquant/production-runtime-profile.json")
PRODUCTION_PROFILE_ANCHOR = Path("/etc/rquant")
PRODUCTION_PROFILE_OWNER_UID = 0
PRODUCTION_PROFILE_MODE = 0o444
PRODUCTION_PROFILE_DIRECTORY_MODE = 0o755
_PRODUCTION_PROFILE_DIRECTORY_POLICY = MappingProxyType(
    {
        Path("/"): (0, 0o755),
        Path("/etc"): (0, 0o755),
        PRODUCTION_PROFILE_ANCHOR: (0, PRODUCTION_PROFILE_DIRECTORY_MODE),
    }
)

PRODUCTION_SYSTEM_PYTHON = Path("/usr/bin/python3.11")
PRODUCTION_DEPLOY_PYZ = Path("/usr/local/libexec/rquant-production-deploy.pyz")
PRODUCTION_RUNTIME_PYZ = Path("/usr/local/libexec/rquant-runtime-exec.pyz")
PRODUCTION_INBOX_ROOT = Path("/var/lib/rquant/runtime-authority/inbox")
PRODUCTION_QUARANTINE_ROOT = Path("/var/lib/rquant/runtime-authority/quarantine")
PRODUCTION_GENERATION_ROOT = Path("/var/lib/rquant/runtime-authority/generations")
GENERATION_MANIFEST_NAME = "full-manifest.json"
GENERATION_DIRECTORY_MODE = 0o555
GENERATION_MANIFEST_MODE = 0o444
PRODUCTION_ALLOWED_OPERATIONS = ("publish", "rollback")
PRODUCTION_ROLE_POLICY = (("daily", "rquant.runtime_service_main", ("LANG", "LC_ALL", "TZ")),)
PRODUCTION_MANIFEST_SCHEMA = MappingProxyType(
    {
        "schema_id": "rquant-full-manifest/v1",
        "entry_types": ("directory", "file"),
        "directory_modes": (0o555,),
        "file_modes": (0o444, 0o555),
        "max_entries": 100_000,
        "max_total_bytes": 4_294_967_296,
        "max_file_bytes": 1_073_741_824,
        "max_path_bytes": MAX_PATH_BYTES,
        "max_depth": 32,
    }
)

RUNTIME_AUTHORITY_PATH = Path("/var/lib/rquant/runtime-authority/current.json")
RUNTIME_AUTHORITY_LOCK_PATH = Path("/var/lib/rquant/runtime-authority/deployment.lock")
RUNTIME_AUTHORITY_ANCHOR = Path("/var/lib/rquant/runtime-authority")
RUNTIME_AUTHORITY_OWNER_UID = 0
RUNTIME_AUTHORITY_LOCK_MODE = 0o600
RUNTIME_AUTHORITY_RECORD_MODE = 0o444
RUNTIME_AUTHORITY_TEMP_MODE = 0o600
RUNTIME_AUTHORITY_DIRECTORY_MODE = 0o755
_PRODUCTION_RUNTIME_DIRECTORY_POLICY = MappingProxyType(
    {
        Path("/"): (0, 0o755),
        Path("/var"): (0, 0o755),
        Path("/var/lib"): (0, 0o755),
        Path("/var/lib/rquant"): (0, 0o755),
        RUNTIME_AUTHORITY_ANCHOR: (0, RUNTIME_AUTHORITY_DIRECTORY_MODE),
        PRODUCTION_GENERATION_ROOT: (0, 0o755),
    }
)

_SHA256 = re.compile(r"[0-9a-f]{64}")
_OPERATION_ID = re.compile(r"[0-9a-f]{32}")
_ROLE_NAME = re.compile(r"[a-z][a-z0-9_-]{0,63}")
_MODULE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*")
_ENVIRONMENT_NAME = re.compile(r"[A-Z][A-Z0-9_]{0,63}")


class RuntimeAuthorityError(RuntimeError):
    """A runtime authority contract failed closed."""


class ProductionRuntimeProfileError(RuntimeAuthorityError):
    """The production runtime closure profile is invalid or unsafe."""


class RuntimeAuthorityRecordError(RuntimeAuthorityError):
    """The single runtime authority record is invalid."""


class RuntimeAuthorityPublishError(RuntimeAuthorityError):
    """Atomic publication or exact temporary recovery failed."""


class RuntimeAuthorityDurabilityError(RuntimeAuthorityPublishError):
    """The visible authority record could not be durably synchronized."""


class RuntimeAuthorityRollbackError(RuntimeAuthorityRecordError):
    """The requested automatic rollback violates the one-level contract."""


class RuntimeAuthorityState(StrEnum):
    ACTIVE = "active"
    ROLLED_BACK = "rolled_back"


class RuntimeGenerationLifecycle(StrEnum):
    ACTIVE = "active"
    ROLLBACK_READY = "rollback_ready"
    FAILED = "failed"


class RuntimeAuthorityPublishResult(StrEnum):
    COMMITTED = "committed"
    IDEMPOTENT = "idempotent"
    COMMITTED_AFTER_RECOVERY = "committed_after_recovery"


@dataclass
class _DeploymentLockLease:
    descriptor: int
    parent_fd: int
    name: str
    descriptors: list[int]
    identity: tuple[int, ...]

    def assert_current(self) -> None:
        descriptor_identity = _identity(os.fstat(self.descriptor))
        named_identity = _identity(os.stat(self.name, dir_fd=self.parent_fd, follow_symlinks=False))
        if descriptor_identity != self.identity or named_identity != self.identity:
            raise RuntimeAuthorityPublishError("deployment lock identity changed")

    def close(self) -> None:
        if self.descriptor < 0:
            return
        descriptor = self.descriptor
        self.descriptor = -1
        with suppress(OSError):
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        with suppress(OSError):
            os.close(descriptor)
        _close_descriptors(self.descriptors)
        self.descriptors.clear()


_DURABLE_EVIDENCE_SEAL = object()


@dataclass(frozen=True)
class _DurableGenerationEvidence:
    seal: object
    slot: RuntimeGenerationSlot
    generation_identity: tuple[int, ...]
    manifest_identity: tuple[int, ...]
    tree_identities: tuple[tuple[str, tuple[int, ...]], ...]

    def __post_init__(self) -> None:
        if self.seal is not _DURABLE_EVIDENCE_SEAL:
            raise RuntimeAuthorityPublishError(
                "durable generation evidence cannot be constructed by callers"
            )


@dataclass(frozen=True)
class _GenerationManifestEntry:
    path: str
    entry_type: str
    owner_uid: int
    mode: int
    nlink: int
    size: int
    sha256: str | None


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
class RuntimeProfileRole:
    module: str
    environment_allowlist: tuple[str, ...]

    def __post_init__(self) -> None:
        if (
            type(self.module) is not str
            or not self.module.startswith("rquant.")
            or _MODULE_NAME.fullmatch(self.module) is None
        ):
            raise ProductionRuntimeProfileError("runtime profile role module is invalid")
        if (
            _utf8_size(
                self.module,
                ProductionRuntimeProfileError,
                "runtime profile role module",
            )
            > MAX_MODULE_BYTES
        ):
            raise ProductionRuntimeProfileError("runtime profile role module is invalid")
        names = self.environment_allowlist
        if (
            not names
            or len(names) > MAX_ENVIRONMENT_NAMES
            or any(
                type(name) is not str
                or _ENVIRONMENT_NAME.fullmatch(name) is None
                or _utf8_size(
                    name,
                    ProductionRuntimeProfileError,
                    "runtime profile environment name",
                )
                > MAX_ENVIRONMENT_NAME_BYTES
                for name in names
            )
            or names != tuple(sorted(set(names)))
        ):
            raise ProductionRuntimeProfileError("runtime profile environment allowlist is invalid")

    def payload(self) -> dict[str, object]:
        return {
            "module": self.module,
            "environment_allowlist": list(self.environment_allowlist),
        }


@dataclass(frozen=True)
class RuntimeManifestSchema:
    schema_id: str
    entry_types: tuple[str, ...]
    directory_modes: tuple[int, ...]
    file_modes: tuple[int, ...]
    max_entries: int
    max_total_bytes: int
    max_file_bytes: int
    max_path_bytes: int
    max_depth: int

    def __post_init__(self) -> None:
        if self.payload() != {
            key: list(value) if isinstance(value, tuple) else value
            for key, value in PRODUCTION_MANIFEST_SCHEMA.items()
        }:
            raise ProductionRuntimeProfileError("runtime manifest schema is not fixed v1")

    def payload(self) -> dict[str, object]:
        return {
            "schema_id": self.schema_id,
            "entry_types": list(self.entry_types),
            "directory_modes": list(self.directory_modes),
            "file_modes": list(self.file_modes),
            "max_entries": self.max_entries,
            "max_total_bytes": self.max_total_bytes,
            "max_file_bytes": self.max_file_bytes,
            "max_path_bytes": self.max_path_bytes,
            "max_depth": self.max_depth,
        }


@dataclass
class _GenerationTreeBudget:
    schema: RuntimeManifestSchema
    entries: int = 0
    total_bytes: int = 0

    def observe_entry(self, path: str) -> None:
        self.entries += 1
        if self.entries > self.schema.max_entries:
            raise RuntimeAuthorityPublishError("generation entry budget exceeded")
        if len(path.split("/")) > self.schema.max_depth:
            raise RuntimeAuthorityPublishError("generation path depth budget exceeded")

    def observe_file(self, size: int) -> None:
        self.require_file_size(size)
        self.observe_file_bytes(size)

    def require_file_size(self, size: int) -> None:
        if size > self.schema.max_file_bytes:
            raise RuntimeAuthorityPublishError("generation file byte budget exceeded")

    def observe_file_bytes(self, size: int) -> None:
        self.total_bytes += size
        if self.total_bytes > self.schema.max_total_bytes:
            raise RuntimeAuthorityPublishError("generation total byte budget exceeded")


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
    inbox_root: Path
    quarantine_root: Path
    generation_root: Path
    allowed_operations: tuple[str, ...]
    roles: Mapping[str, RuntimeProfileRole]
    manifest_schema: RuntimeManifestSchema

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
        if len(self.ancestors) > MAX_PROFILE_ANCESTORS or len(files) > MAX_CLOSURE_FILES:
            raise ProductionRuntimeProfileError("runtime profile closure exceeds fixed limits")
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
        roots = (
            (self.inbox_root, PRODUCTION_INBOX_ROOT, "inbox"),
            (self.quarantine_root, PRODUCTION_QUARANTINE_ROOT, "quarantine"),
            (self.generation_root, PRODUCTION_GENERATION_ROOT, "generation"),
        )
        for actual, expected, label in roots:
            if _model_path(actual, ProductionRuntimeProfileError, f"{label} root") != expected:
                raise ProductionRuntimeProfileError(f"runtime {label} root is not fixed")
        if self.allowed_operations != PRODUCTION_ALLOWED_OPERATIONS:
            raise ProductionRuntimeProfileError("runtime allowed operations are not fixed")
        if not isinstance(self.roles, Mapping) or len(self.roles) > MAX_ROLES:
            raise ProductionRuntimeProfileError("runtime profile roles are invalid")
        expected_roles = {
            name: RuntimeProfileRole(module, environment)
            for name, module, environment in PRODUCTION_ROLE_POLICY
        }
        if dict(self.roles) != expected_roles:
            raise ProductionRuntimeProfileError("runtime profile role policy is not fixed")
        object.__setattr__(
            self,
            "roles",
            MappingProxyType(dict(sorted(self.roles.items()))),
        )
        if not isinstance(self.manifest_schema, RuntimeManifestSchema):
            raise ProductionRuntimeProfileError("runtime manifest schema is invalid")
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
            "inbox_root": str(self.inbox_root),
            "quarantine_root": str(self.quarantine_root),
            "generation_root": str(self.generation_root),
            "allowed_operations": list(self.allowed_operations),
            "roles": {name: role.payload() for name, role in self.roles.items()},
            "manifest_schema": self.manifest_schema.payload(),
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
        if (
            _utf8_size(self.module, RuntimeAuthorityRecordError, "runtime role module")
            > MAX_MODULE_BYTES
        ):
            raise RuntimeAuthorityRecordError("runtime role module is too long")
        if (
            not self.site_packages
            or len(self.site_packages) > MAX_SITE_PACKAGES
            or not all(isinstance(path, Path) for path in self.site_packages)
        ):
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
    lifecycle: RuntimeGenerationLifecycle
    generation_id: str
    generation_path: Path
    commit: str
    full_manifest_hash: str
    profile_id: str
    roles: Mapping[str, RuntimeRoleSpec]

    def __post_init__(self) -> None:
        if not isinstance(self.lifecycle, RuntimeGenerationLifecycle):
            raise RuntimeAuthorityRecordError("runtime generation lifecycle is invalid")
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
            or any(ord(character) < 0x20 for character in self.commit)
        ):
            raise RuntimeAuthorityRecordError("untrusted commit audit metadata is invalid")
        if (
            _utf8_size(
                self.commit,
                RuntimeAuthorityRecordError,
                "untrusted commit audit metadata",
            )
            > MAX_COMMIT_BYTES
        ):
            raise RuntimeAuthorityRecordError("untrusted commit audit metadata is invalid")
        if not isinstance(self.roles, Mapping) or not self.roles or len(self.roles) > MAX_ROLES:
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
            "lifecycle": self.lifecycle.value,
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
    sequence: int
    state: RuntimeAuthorityState
    current: RuntimeGenerationSlot
    prior: RuntimeGenerationSlot | None

    def __post_init__(self) -> None:
        if type(self.schema_version) is not int or self.schema_version != RECORD_SCHEMA_VERSION:
            raise RuntimeAuthorityRecordError("runtime authority schema is unsupported")
        _require_operation_id(self.operation_id)
        if type(self.sequence) is not int or self.sequence < 1:
            raise RuntimeAuthorityRecordError("runtime authority sequence is invalid")
        if not isinstance(self.state, RuntimeAuthorityState):
            raise RuntimeAuthorityRecordError("runtime authority state is invalid")
        if not isinstance(self.current, RuntimeGenerationSlot):
            raise RuntimeAuthorityRecordError("current runtime slot is invalid")
        if self.prior is not None and not isinstance(self.prior, RuntimeGenerationSlot):
            raise RuntimeAuthorityRecordError("prior runtime slot is invalid")
        if self.prior is not None and self.current.generation_id == self.prior.generation_id:
            raise RuntimeAuthorityRecordError("current and prior name the same generation")
        prior_lifecycle = None if self.prior is None else self.prior.lifecycle
        allowed_prior = {
            RuntimeAuthorityState.ACTIVE: {
                None,
                RuntimeGenerationLifecycle.ROLLBACK_READY,
            },
            RuntimeAuthorityState.ROLLED_BACK: {RuntimeGenerationLifecycle.FAILED},
        }
        if (
            self.current.lifecycle is not RuntimeGenerationLifecycle.ACTIVE
            or prior_lifecycle not in allowed_prior[self.state]
        ):
            raise RuntimeAuthorityRecordError(
                "runtime authority state and slot lifecycles are inconsistent"
            )

    def validate_for_root(self, generation_root: Path) -> None:
        self.current.validate_for_root(generation_root)
        if self.prior is not None:
            self.prior.validate_for_root(generation_root)

    def payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema_version": self.schema_version,
            "operation_id": self.operation_id,
            "sequence": self.sequence,
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
    "inbox_root",
    "quarantine_root",
    "generation_root",
    "allowed_operations",
    "roles",
    "manifest_schema",
}
_FILE_FIELDS = {"path", "sha256", "owner_uid", "mode"}
_ANCESTOR_FIELDS = {"path", "owner_uid", "mode"}
_SLOT_FIELDS = (
    "lifecycle",
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
    "sequence",
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
_PROFILE_ROLE_FIELDS = {"module", "environment_allowlist"}
_MANIFEST_FIELDS = set(PRODUCTION_MANIFEST_SCHEMA)
_GENERATION_MANIFEST_FIELDS = {"schema_id", "profile_id", "roles", "entries"}
_GENERATION_MANIFEST_ENTRY_FIELDS = {
    "path",
    "type",
    "owner_uid",
    "mode",
    "nlink",
    "size",
    "sha256",
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
    operations = data["allowed_operations"]
    roles = data["roles"]
    if (
        type(ancestors) is not list
        or type(stdlib) is not list
        or type(shared_libraries) is not list
        or type(operations) is not list
        or type(roles) is not dict
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
        inbox_root=_payload_path(data["inbox_root"], ProductionRuntimeProfileError, "inbox root"),
        quarantine_root=_payload_path(
            data["quarantine_root"],
            ProductionRuntimeProfileError,
            "quarantine root",
        ),
        generation_root=_payload_path(
            data["generation_root"],
            ProductionRuntimeProfileError,
            "generation root",
        ),
        allowed_operations=tuple(operations),
        roles={name: _parse_profile_role(role) for name, role in roles.items()},
        manifest_schema=_parse_manifest_schema(data["manifest_schema"]),
    )


def load_production_runtime_profile() -> RuntimeClosureProfile:
    payload = _read_trusted_file(
        PRODUCTION_PROFILE_PATH,
        directory_policy=_PRODUCTION_PROFILE_DIRECTORY_POLICY,
        owner_uid=PRODUCTION_PROFILE_OWNER_UID,
        file_mode=PRODUCTION_PROFILE_MODE,
        max_bytes=MAX_PROFILE_BYTES,
        error_type=ProductionRuntimeProfileError,
        label="production runtime profile",
    )
    return parse_runtime_closure_profile(payload)


def parse_runtime_authority_record(
    payload: str | bytes | bytearray,
) -> RuntimeAuthorityRecord:
    return _parse_runtime_authority_record(payload, load_production_runtime_profile())


def _parse_runtime_authority_record(
    payload: str | bytes | bytearray,
    profile: RuntimeClosureProfile,
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
        sequence=data["sequence"],
        state=parsed_state,
        current=current,
        prior=prior,
    )
    _validate_record(record, profile)
    return record


def canonical_runtime_authority_bytes(record: RuntimeAuthorityRecord) -> bytes:
    if not isinstance(record, RuntimeAuthorityRecord):
        raise RuntimeAuthorityRecordError("runtime authority record model is required")
    payload = canonical_json_bytes(record.payload(), trailing_newline=True)
    if len(payload) > MAX_RECORD_BYTES:
        raise RuntimeAuthorityRecordError("runtime authority record is too large")
    return payload


def load_runtime_authority() -> RuntimeAuthorityRecord:
    profile = load_production_runtime_profile()
    payload = _read_trusted_file(
        RUNTIME_AUTHORITY_PATH,
        directory_policy=_PRODUCTION_RUNTIME_DIRECTORY_POLICY,
        owner_uid=RUNTIME_AUTHORITY_OWNER_UID,
        file_mode=RUNTIME_AUTHORITY_RECORD_MODE,
        max_bytes=MAX_RECORD_BYTES,
        error_type=RuntimeAuthorityRecordError,
        label="runtime authority record",
    )
    record = _parse_runtime_authority_record(payload, profile)
    if payload != canonical_runtime_authority_bytes(record):
        raise RuntimeAuthorityRecordError("runtime authority record is not canonical")
    return record


def prepare_runtime_authority_publish(
    previous: RuntimeAuthorityRecord | None,
    next_generation: RuntimeGenerationSlot,
    *,
    operation_id: str,
) -> RuntimeAuthorityRecord:
    _require_operation_id(operation_id)
    if not isinstance(next_generation, RuntimeGenerationSlot):
        raise RuntimeAuthorityRecordError("next runtime generation is invalid")
    if next_generation.lifecycle is not RuntimeGenerationLifecycle.ACTIVE:
        raise RuntimeAuthorityRecordError("next runtime generation must be active")
    profile = load_production_runtime_profile()
    if previous is None:
        candidate = RuntimeAuthorityRecord(
            schema_version=RECORD_SCHEMA_VERSION,
            operation_id=operation_id,
            sequence=1,
            state=RuntimeAuthorityState.ACTIVE,
            current=next_generation,
            prior=None,
        )
        _validate_record(candidate, profile)
        return candidate
    _validate_record(previous, profile)
    if operation_id == previous.operation_id:
        raise RuntimeAuthorityRecordError("operation id must be unique")
    recorded = {previous.current.generation_id}
    if previous.prior is not None:
        recorded.add(previous.prior.generation_id)
    if next_generation.generation_id in recorded:
        raise RuntimeAuthorityRecordError("next generation is already recorded")
    candidate = RuntimeAuthorityRecord(
        schema_version=RECORD_SCHEMA_VERSION,
        operation_id=operation_id,
        sequence=previous.sequence + 1,
        state=RuntimeAuthorityState.ACTIVE,
        current=next_generation,
        prior=replace(
            previous.current,
            lifecycle=RuntimeGenerationLifecycle.ROLLBACK_READY,
        ),
    )
    _validate_publication_transition(previous, candidate, profile)
    return candidate


def prepare_runtime_authority_rollback(
    previous: RuntimeAuthorityRecord,
    *,
    operation_id: str,
) -> RuntimeAuthorityRecord:
    if not isinstance(previous, RuntimeAuthorityRecord):
        raise RuntimeAuthorityRollbackError("runtime authority record is required")
    _require_operation_id(operation_id)
    profile = load_production_runtime_profile()
    _validate_record(previous, profile)
    if operation_id == previous.operation_id:
        raise RuntimeAuthorityRollbackError("operation id must be unique")
    if previous.state is RuntimeAuthorityState.ROLLED_BACK:
        raise RuntimeAuthorityRollbackError("automatic rollback is single-level")
    if (
        previous.prior is None
        or previous.prior.lifecycle is not RuntimeGenerationLifecycle.ROLLBACK_READY
    ):
        raise RuntimeAuthorityRollbackError("automatic rollback requires a prior generation")
    candidate = RuntimeAuthorityRecord(
        schema_version=RECORD_SCHEMA_VERSION,
        operation_id=operation_id,
        sequence=previous.sequence + 1,
        state=RuntimeAuthorityState.ROLLED_BACK,
        current=replace(previous.prior, lifecycle=RuntimeGenerationLifecycle.ACTIVE),
        prior=replace(previous.current, lifecycle=RuntimeGenerationLifecycle.FAILED),
    )
    _validate_publication_transition(previous, candidate, profile)
    return candidate


def publish_runtime_authority(
    record: RuntimeAuthorityRecord,
) -> RuntimeAuthorityPublishResult:
    if not isinstance(record, RuntimeAuthorityRecord):
        raise RuntimeAuthorityPublishError("runtime authority record model is required")
    profile = load_production_runtime_profile()
    try:
        _validate_record(record, profile)
        payload = canonical_runtime_authority_bytes(record)
    except RuntimeAuthorityRecordError as exc:
        raise RuntimeAuthorityPublishError(
            "runtime authority record violates the loaded profile"
        ) from exc
    lock_lease: _DeploymentLockLease | None = None
    descriptors: list[int] = []
    temporary_fd = -1
    temporary_name = _temporary_name(record.operation_id)
    try:
        lock_lease = _acquire_deployment_lock()
        lock_lease.assert_current()
        descriptors, parent_fd, target_name = _open_trusted_parent(
            RUNTIME_AUTHORITY_PATH,
            directory_policy=_PRODUCTION_RUNTIME_DIRECTORY_POLICY,
            error_type=RuntimeAuthorityPublishError,
            label="runtime authority directory",
        )
        existing = _read_record_at(
            parent_fd,
            target_name,
            profile,
            missing_ok=True,
            label="existing runtime authority record",
        )
        previous = None if existing is None else existing[0]
        if previous is not None and previous.operation_id == record.operation_id:
            if existing[1] == payload:
                _revalidate_record_generations(record, profile)
                _fsync_authority_parent(parent_fd, lock_lease)
                return RuntimeAuthorityPublishResult.IDEMPOTENT
            raise RuntimeAuthorityPublishError("authority operation id conflicts")
        _validate_publication_transition(previous, record, profile)
        evidence = _revalidate_record_generations(record, profile)
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
        temporary = _read_record_at(
            parent_fd,
            temporary_name,
            profile,
            missing_ok=False,
            label="runtime authority temporary",
        )
        if temporary is None or temporary[0] != record or temporary[1] != payload:
            raise RuntimeAuthorityPublishError("runtime authority temporary content changed")
        _consume_generation_evidence(evidence, record, profile)
        lock_lease.assert_current()
        _replace_record(parent_fd, temporary_name)
        try:
            _fsync_descriptor(parent_fd, phase_name="parent_fsync")
        except OSError as exc:
            recovered = _read_record_at(
                parent_fd,
                target_name,
                profile,
                missing_ok=False,
                label="recovered runtime authority record",
            )
            if recovered is not None and recovered[0] == record and recovered[1] == payload:
                _consume_generation_evidence(evidence, record, profile)
                _fsync_authority_parent(parent_fd, lock_lease)
                return RuntimeAuthorityPublishResult.COMMITTED_AFTER_RECOVERY
            raise RuntimeAuthorityDurabilityError(
                "runtime authority recovery did not find the exact committed record"
            ) from exc
        lock_lease.assert_current()
        return RuntimeAuthorityPublishResult.COMMITTED
    except RuntimeAuthorityPublishError:
        raise
    except (OSError, RuntimeAuthorityRecordError) as exc:
        raise RuntimeAuthorityPublishError("runtime authority publication failed") from exc
    finally:
        if temporary_fd >= 0:
            with suppress(OSError):
                os.close(temporary_fd)
        _close_descriptors(descriptors)
        if lock_lease is not None:
            lock_lease.close()


def cleanup_runtime_authority_temp(operation_id: str) -> bool:
    try:
        _require_operation_id(operation_id)
    except RuntimeAuthorityRecordError as exc:
        raise RuntimeAuthorityPublishError(
            "runtime authority temporary operation is invalid"
        ) from exc
    lock_lease: _DeploymentLockLease | None = None
    descriptors: list[int] = []
    try:
        lock_lease = _acquire_deployment_lock()
        descriptors, parent_fd, _target_name = _open_trusted_parent(
            RUNTIME_AUTHORITY_PATH,
            directory_policy=_PRODUCTION_RUNTIME_DIRECTORY_POLICY,
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
        if lock_lease is not None:
            lock_lease.close()


def _parse_profile_role(payload: object) -> RuntimeProfileRole:
    if type(payload) is not dict or set(payload) != _PROFILE_ROLE_FIELDS:
        raise ProductionRuntimeProfileError("runtime profile role schema is invalid")
    environment = payload["environment_allowlist"]
    if type(environment) is not list:
        raise ProductionRuntimeProfileError("runtime profile environment allowlist is invalid")
    return RuntimeProfileRole(
        module=payload["module"],
        environment_allowlist=tuple(environment),
    )


def _parse_manifest_schema(payload: object) -> RuntimeManifestSchema:
    if type(payload) is not dict or set(payload) != _MANIFEST_FIELDS:
        raise ProductionRuntimeProfileError("runtime manifest schema is invalid")
    collection_fields = ("entry_types", "directory_modes", "file_modes")
    if any(type(payload[field]) is not list for field in collection_fields):
        raise ProductionRuntimeProfileError("runtime manifest schema is invalid")
    return RuntimeManifestSchema(
        schema_id=payload["schema_id"],
        entry_types=tuple(payload["entry_types"]),
        directory_modes=tuple(payload["directory_modes"]),
        file_modes=tuple(payload["file_modes"]),
        max_entries=payload["max_entries"],
        max_total_bytes=payload["max_total_bytes"],
        max_file_bytes=payload["max_file_bytes"],
        max_path_bytes=payload["max_path_bytes"],
        max_depth=payload["max_depth"],
    )


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
    lifecycle = payload[f"{prefix}_lifecycle"]
    if type(lifecycle) is not str:
        raise RuntimeAuthorityRecordError(f"{prefix} slot lifecycle is invalid")
    try:
        parsed_lifecycle = RuntimeGenerationLifecycle(lifecycle)
    except ValueError as exc:
        raise RuntimeAuthorityRecordError(f"{prefix} slot lifecycle is invalid") from exc
    return RuntimeGenerationSlot(
        lifecycle=parsed_lifecycle,
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
    _preflight_json_resources(encoded, error_type=error_type, label=label)

    def reject_number(value: str) -> object:
        raise StrictJsonError(f"non-native JSON number: {value}")

    try:
        parsed = strict_json_loads(
            encoded,
            parse_float=reject_number,
            parse_constant=reject_number,
        )
        _reject_json_surrogates(parsed, error_type=error_type, label=label)
        return parsed
    except error_type:
        raise
    except (
        StrictJsonError,
        UnicodeDecodeError,
        UnicodeEncodeError,
        ValueError,
        RecursionError,
    ) as exc:
        raise error_type(f"{label} is not strict JSON") from exc


def _preflight_json_resources(
    encoded: bytes,
    *,
    error_type: type[RuntimeAuthorityError],
    label: str,
) -> None:
    depth = 0
    integer_digits = 0
    in_string = False
    escaped = False
    for byte in encoded:
        if in_string:
            if escaped:
                escaped = False
            elif byte == ord("\\"):
                escaped = True
            elif byte == ord('"'):
                in_string = False
            continue
        if byte == ord('"'):
            in_string = True
            integer_digits = 0
        elif byte in (ord("{"), ord("[")):
            depth += 1
            integer_digits = 0
            if depth > MAX_JSON_NESTING_DEPTH:
                raise error_type(f"{label} JSON nesting exceeds the fixed limit")
        elif byte in (ord("}"), ord("]")):
            depth -= 1
            integer_digits = 0
        elif ord("0") <= byte <= ord("9"):
            integer_digits += 1
            if integer_digits > MAX_JSON_INTEGER_DIGITS:
                raise error_type(f"{label} JSON integer exceeds the fixed digit limit")
        else:
            integer_digits = 0


def _reject_json_surrogates(
    payload: object,
    *,
    error_type: type[RuntimeAuthorityError],
    label: str,
) -> None:
    pending = [payload]
    while pending:
        value = pending.pop()
        if type(value) is str:
            if any(0xD800 <= ord(character) <= 0xDFFF for character in value):
                raise error_type(f"{label} contains text that is not valid UTF-8")
        elif type(value) is list:
            pending.extend(value)
        elif type(value) is dict:
            pending.extend(value.keys())
            pending.extend(value.values())


def _utf8_size(
    value: str,
    error_type: type[RuntimeAuthorityError],
    label: str,
) -> int:
    try:
        return len(value.encode("utf-8"))
    except (UnicodeEncodeError, ValueError) as exc:
        raise error_type(f"{label} is not valid UTF-8") from exc


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
    if not isinstance(value, Path):
        raise error_type(f"{label} must be one absolute canonical path")
    try:
        invalid = (
            not value.is_absolute()
            or "\x00" in str(value)
            or len(os.fsencode(value)) > MAX_PATH_BYTES
        )
        canonical = Path(os.path.abspath(value))
    except (UnicodeEncodeError, UnicodeDecodeError, ValueError, OSError) as exc:
        raise error_type(f"{label} must be one valid UTF-8 canonical path") from exc
    if invalid:
        raise error_type(f"{label} must be one absolute canonical path")
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


def _inode_identity(observed: os.stat_result) -> tuple[int, int]:
    return observed.st_dev, observed.st_ino


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


def _acquire_deployment_lock() -> _DeploymentLockLease:
    descriptors: list[int] = []
    descriptor = -1
    lease: _DeploymentLockLease | None = None
    acquired = False
    try:
        descriptors, parent_fd, name = _open_trusted_parent(
            RUNTIME_AUTHORITY_LOCK_PATH,
            directory_policy=_PRODUCTION_RUNTIME_DIRECTORY_POLICY,
            error_type=RuntimeAuthorityPublishError,
            label="deployment lock",
        )
        base_flags = (
            os.O_RDWR
            | _required_flag("O_NOFOLLOW", RuntimeAuthorityPublishError)
            | getattr(os, "O_CLOEXEC", 0)
        )
        try:
            descriptor = os.open(
                name,
                base_flags | os.O_CREAT | os.O_EXCL,
                RUNTIME_AUTHORITY_LOCK_MODE,
                dir_fd=parent_fd,
            )
        except FileExistsError:
            descriptor = os.open(name, base_flags, dir_fd=parent_fd)
        observed = os.fstat(descriptor)
        _require_file_stat(
            observed,
            owner_uid=RUNTIME_AUTHORITY_OWNER_UID,
            mode=RUNTIME_AUTHORITY_LOCK_MODE,
            error_type=RuntimeAuthorityPublishError,
            label="deployment lock",
        )
        named = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if _identity(observed) != _identity(named):
            raise RuntimeAuthorityPublishError("deployment lock identity changed while opening")
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        lease = _DeploymentLockLease(
            descriptor=descriptor,
            parent_fd=parent_fd,
            name=name,
            descriptors=descriptors,
            identity=_identity(observed),
        )
        lease.assert_current()
        acquired = True
        return lease
    except RuntimeAuthorityPublishError:
        raise
    except OSError as exc:
        raise RuntimeAuthorityPublishError("deployment lock is unavailable") from exc
    finally:
        if not acquired:
            if lease is not None:
                lease.close()
            else:
                if descriptor >= 0:
                    with suppress(OSError):
                        os.close(descriptor)
                _close_descriptors(descriptors)


def _open_trusted_parent(
    path: Path,
    *,
    directory_policy: Mapping[Path, tuple[int, int]],
    error_type: type[RuntimeAuthorityError],
    label: str,
) -> tuple[list[int], int, str]:
    path = _model_path(path, error_type, label)
    if path.parent == path:
        raise error_type(f"{label} has no trusted parent")
    descriptors, parent_fd = _open_trusted_directory(
        path.parent,
        directory_policy=directory_policy,
        error_type=error_type,
        label=label,
    )
    return descriptors, parent_fd, path.name


def _open_trusted_directory(
    path: Path,
    *,
    directory_policy: Mapping[Path, tuple[int, int]],
    error_type: type[RuntimeAuthorityError],
    label: str,
) -> tuple[list[int], int]:
    path = _model_path(path, error_type, f"{label} directory")
    if not isinstance(directory_policy, Mapping):
        raise error_type(f"{label} ancestor policy is invalid")
    flags = (
        os.O_RDONLY
        | _required_flag("O_DIRECTORY", error_type)
        | _required_flag("O_NOFOLLOW", error_type)
        | getattr(os, "O_CLOEXEC", 0)
    )
    descriptors: list[int] = []
    try:
        current = Path("/")
        expected = directory_policy.get(current)
        if expected is None:
            raise error_type(f"{label} ancestor policy is incomplete")
        named = os.stat(current, follow_symlinks=False)
        _require_directory_stat(
            named,
            owner_uid=expected[0],
            mode=expected[1],
            error_type=error_type,
            label=f"{label} ancestor {current}",
        )
        parent_fd = os.open(current, flags)
        descriptors.append(parent_fd)
        opened = os.fstat(parent_fd)
        _require_directory_stat(
            opened,
            owner_uid=expected[0],
            mode=expected[1],
            error_type=error_type,
            label=f"{label} ancestor {current}",
        )
        if _inode_identity(opened) != _inode_identity(named):
            raise error_type(f"{label} ancestor identity changed")
        for component in path.parts[1:]:
            current /= component
            expected = directory_policy.get(current)
            if expected is None:
                raise error_type(f"{label} ancestor policy is incomplete")
            named = os.stat(component, dir_fd=parent_fd, follow_symlinks=False)
            child_fd = _open_directory_entry(
                parent_fd,
                component,
                named,
                owner_uid=expected[0],
                mode=expected[1],
                error_type=error_type,
                label=f"{label} ancestor {current}",
            )
            descriptors.append(child_fd)
            parent_fd = child_fd
        return descriptors, parent_fd
    except BaseException:
        _close_descriptors(descriptors)
        raise


def _open_directory_entry(
    parent_fd: int,
    name: str,
    named: os.stat_result,
    *,
    owner_uid: int,
    mode: int,
    error_type: type[RuntimeAuthorityError],
    label: str,
) -> int:
    _require_directory_stat(
        named,
        owner_uid=owner_uid,
        mode=mode,
        error_type=error_type,
        label=label,
    )
    descriptor = -1
    try:
        descriptor = os.open(
            name,
            os.O_RDONLY
            | _required_flag("O_DIRECTORY", error_type)
            | _required_flag("O_NOFOLLOW", error_type)
            | getattr(os, "O_CLOEXEC", 0),
            dir_fd=parent_fd,
        )
        opened = os.fstat(descriptor)
        _require_directory_stat(
            opened,
            owner_uid=owner_uid,
            mode=mode,
            error_type=error_type,
            label=label,
        )
        if _inode_identity(opened) != _inode_identity(named):
            raise error_type(f"{label} identity changed while opening")
        return descriptor
    except BaseException:
        if descriptor >= 0:
            with suppress(OSError):
                os.close(descriptor)
        raise


def _read_trusted_file(
    path: Path,
    *,
    directory_policy: Mapping[Path, tuple[int, int]],
    owner_uid: int,
    file_mode: int,
    max_bytes: int,
    error_type: type[RuntimeAuthorityError],
    label: str,
) -> bytes:
    descriptors: list[int] = []
    try:
        descriptors, parent_fd, name = _open_trusted_parent(
            path,
            directory_policy=directory_policy,
            error_type=error_type,
            label=label,
        )
        payload = _read_file_at(
            parent_fd,
            name,
            owner_uid=owner_uid,
            file_mode=file_mode,
            max_bytes=max_bytes,
            error_type=error_type,
            label=label,
        )
        if payload is None:
            raise error_type(f"{label} is missing")
        return payload
    except error_type:
        raise
    except OSError as exc:
        raise error_type(f"{label} is unavailable") from exc
    finally:
        _close_descriptors(descriptors)


def _read_file_at(
    parent_fd: int,
    name: str,
    *,
    owner_uid: int,
    file_mode: int,
    max_bytes: int,
    error_type: type[RuntimeAuthorityError],
    label: str,
    missing_ok: bool = False,
) -> bytes | None:
    try:
        named = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        if missing_ok:
            return None
        raise error_type(f"{label} is missing") from None
    _require_file_stat(
        named,
        owner_uid=owner_uid,
        mode=file_mode,
        error_type=error_type,
        label=label,
    )
    descriptor = -1
    try:
        descriptor = os.open(
            name,
            os.O_RDONLY | _required_flag("O_NOFOLLOW", error_type) | getattr(os, "O_CLOEXEC", 0),
            dir_fd=parent_fd,
        )
        before = os.fstat(descriptor)
        if _identity(before) != _identity(named):
            raise error_type(f"{label} identity changed while opening")
        payload = _read_limited(
            descriptor,
            max_bytes=max_bytes,
            error_type=error_type,
            label=label,
        )
        after = os.fstat(descriptor)
        active = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if _identity(before) != _identity(after) or _identity(before) != _identity(active):
            raise error_type(f"{label} identity changed while reading")
        return payload
    except error_type:
        raise
    except OSError as exc:
        raise error_type(f"{label} is unavailable") from exc
    finally:
        if descriptor >= 0:
            with suppress(OSError):
                os.close(descriptor)


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


def _read_record_at(
    parent_fd: int,
    target_name: str,
    profile: RuntimeClosureProfile,
    *,
    missing_ok: bool,
    label: str,
) -> tuple[RuntimeAuthorityRecord, bytes] | None:
    payload = _read_file_at(
        parent_fd,
        target_name,
        owner_uid=RUNTIME_AUTHORITY_OWNER_UID,
        file_mode=RUNTIME_AUTHORITY_RECORD_MODE,
        max_bytes=MAX_RECORD_BYTES,
        error_type=RuntimeAuthorityPublishError,
        label=label,
        missing_ok=missing_ok,
    )
    if payload is None:
        return None
    record = _parse_runtime_authority_record(payload, profile)
    if canonical_runtime_authority_bytes(record) != payload:
        raise RuntimeAuthorityPublishError(f"{label} bytes are not canonical")
    return record, payload


def _validate_record(
    record: RuntimeAuthorityRecord,
    profile: RuntimeClosureProfile,
) -> None:
    if not isinstance(profile, RuntimeClosureProfile):
        raise RuntimeAuthorityRecordError("loaded runtime profile is invalid")
    for slot in (record.current, record.prior):
        if slot is not None:
            _validate_slot_against_profile(slot, profile)


def _revalidate_record_generations(
    record: RuntimeAuthorityRecord,
    profile: RuntimeClosureProfile,
) -> tuple[_DurableGenerationEvidence, ...]:
    return tuple(
        _revalidate_generation_slot(slot, profile)
        for slot in (record.current, record.prior)
        if slot is not None
    )


def _revalidate_generation_slot(
    slot: RuntimeGenerationSlot,
    profile: RuntimeClosureProfile,
) -> _DurableGenerationEvidence:
    descriptors: list[int] = []
    generation_fd = -1
    try:
        _validate_slot_against_profile(slot, profile)
        descriptors, generation_root_fd = _open_trusted_directory(
            PRODUCTION_GENERATION_ROOT,
            directory_policy=_PRODUCTION_RUNTIME_DIRECTORY_POLICY,
            error_type=RuntimeAuthorityPublishError,
            label="generation root",
        )
        named = os.stat(
            slot.generation_id,
            dir_fd=generation_root_fd,
            follow_symlinks=False,
        )
        _require_directory_stat(
            named,
            owner_uid=RUNTIME_AUTHORITY_OWNER_UID,
            mode=GENERATION_DIRECTORY_MODE,
            error_type=RuntimeAuthorityPublishError,
            label="durable generation",
        )
        generation_fd = os.open(
            slot.generation_id,
            os.O_RDONLY
            | _required_flag("O_DIRECTORY", RuntimeAuthorityPublishError)
            | _required_flag("O_NOFOLLOW", RuntimeAuthorityPublishError)
            | getattr(os, "O_CLOEXEC", 0),
            dir_fd=generation_root_fd,
        )
        generation_stat = os.fstat(generation_fd)
        if _identity(generation_stat) != _identity(named):
            raise RuntimeAuthorityPublishError("durable generation identity changed while opening")
        manifest = _read_file_at(
            generation_fd,
            GENERATION_MANIFEST_NAME,
            owner_uid=RUNTIME_AUTHORITY_OWNER_UID,
            file_mode=GENERATION_MANIFEST_MODE,
            max_bytes=MAX_GENERATION_MANIFEST_BYTES,
            error_type=RuntimeAuthorityPublishError,
            label="generation manifest",
        )
        if manifest is None:
            raise RuntimeAuthorityPublishError("generation manifest is missing")
        manifest_entries = _validate_generation_manifest(manifest, slot, profile)
        tree_identities = _validate_generation_tree(
            generation_fd,
            manifest_entries,
            profile,
        )
        _validate_generation_semantics(generation_fd, manifest_entries, slot)
        active_generation = os.stat(
            slot.generation_id,
            dir_fd=generation_root_fd,
            follow_symlinks=False,
        )
        active_manifest = os.stat(
            GENERATION_MANIFEST_NAME,
            dir_fd=generation_fd,
            follow_symlinks=False,
        )
        _require_file_stat(
            active_manifest,
            owner_uid=RUNTIME_AUTHORITY_OWNER_UID,
            mode=GENERATION_MANIFEST_MODE,
            error_type=RuntimeAuthorityPublishError,
            label="generation manifest",
        )
        if _identity(generation_stat) != _identity(active_generation):
            raise RuntimeAuthorityPublishError(
                "durable generation identity changed while validating"
            )
        return _DurableGenerationEvidence(
            seal=_DURABLE_EVIDENCE_SEAL,
            slot=slot,
            generation_identity=_identity(generation_stat),
            manifest_identity=_identity(active_manifest),
            tree_identities=tree_identities,
        )
    except RuntimeAuthorityPublishError:
        raise
    except (OSError, RuntimeAuthorityRecordError) as exc:
        raise RuntimeAuthorityPublishError("durable generation validation failed") from exc
    finally:
        if generation_fd >= 0:
            with suppress(OSError):
                os.close(generation_fd)
        _close_descriptors(descriptors)


def _validate_slot_against_profile(
    slot: RuntimeGenerationSlot,
    profile: RuntimeClosureProfile,
) -> None:
    slot.validate_for_root(profile.generation_root)
    if slot.profile_id != profile.profile_id:
        raise RuntimeAuthorityRecordError("runtime slot profile id is not active")
    if set(slot.roles) != set(profile.roles) or any(
        role.module != profile.roles[name].module for name, role in slot.roles.items()
    ):
        raise RuntimeAuthorityRecordError("runtime slot roles do not match the loaded profile")


def _validate_generation_manifest(
    payload: bytes,
    slot: RuntimeGenerationSlot,
    profile: RuntimeClosureProfile,
) -> tuple[_GenerationManifestEntry, ...]:
    data = _strict_payload(
        payload,
        max_bytes=MAX_GENERATION_MANIFEST_BYTES,
        error_type=RuntimeAuthorityPublishError,
        label="generation manifest",
    )
    if type(data) is not dict or set(data) != _GENERATION_MANIFEST_FIELDS:
        raise RuntimeAuthorityPublishError("generation manifest schema is invalid")
    if canonical_json_bytes(data, trailing_newline=True) != payload:
        raise RuntimeAuthorityPublishError("generation manifest is not canonical")
    if hashlib.sha256(payload).hexdigest() != slot.full_manifest_hash:
        raise RuntimeAuthorityPublishError("generation manifest hash does not match slot")
    if (
        data["schema_id"] != profile.manifest_schema.schema_id
        or data["profile_id"] != profile.profile_id
    ):
        raise RuntimeAuthorityPublishError("generation manifest does not match the loaded profile")
    expected_roles = {
        name: _manifest_role_payload(role, slot.generation_path)
        for name, role in slot.roles.items()
    }
    if type(data["roles"]) is not dict or data["roles"] != expected_roles:
        raise RuntimeAuthorityPublishError(
            "generation manifest roles do not match the authority slot"
        )
    entries = data["entries"]
    if type(entries) is not list:
        raise RuntimeAuthorityPublishError("generation manifest entries are invalid")
    manifest_entries: list[_GenerationManifestEntry] = []
    entry_types: dict[str, str] = {}
    declared_budget = _GenerationTreeBudget(profile.manifest_schema)
    for entry in entries:
        parsed = _validate_generation_manifest_entry(entry, profile)
        declared_budget.observe_entry(parsed.path)
        if parsed.entry_type == "file":
            declared_budget.observe_file(parsed.size)
        manifest_entries.append(parsed)
        entry_types[parsed.path] = parsed.entry_type
    observed_paths = [entry.path for entry in manifest_entries]
    if observed_paths != sorted(set(observed_paths)):
        raise RuntimeAuthorityPublishError("generation manifest entries are not canonical")
    if entry_types.get("pyvenv.cfg") != "file":
        raise RuntimeAuthorityPublishError("generation manifest is missing pyvenv.cfg")
    required_paths = {role["python_path"]: "file" for role in expected_roles.values()}
    for role in expected_roles.values():
        required_paths[role["working_directory"]] = "directory"
        required_paths[role["app_source"]] = "directory"
        required_paths.update({path: "directory" for path in role["site_packages"]})
    if any(entry_types.get(path) != kind for path, kind in required_paths.items()):
        raise RuntimeAuthorityPublishError(
            "generation manifest does not cover every runtime role path"
        )
    return tuple(manifest_entries)


def _manifest_role_payload(
    role: RuntimeRoleSpec,
    generation_path: Path,
) -> dict[str, object]:
    def relative(path: Path) -> str:
        try:
            value = path.relative_to(generation_path)
        except ValueError as exc:
            raise RuntimeAuthorityPublishError(
                "generation manifest role path escapes its generation"
            ) from exc
        if not value.parts:
            raise RuntimeAuthorityPublishError("generation manifest role path is empty")
        return value.as_posix()

    return {
        "python_path": relative(role.python_path),
        "module": role.module,
        "working_directory": relative(role.working_directory),
        "app_source": relative(role.app_source),
        "site_packages": [relative(path) for path in role.site_packages],
    }


def _validate_generation_manifest_entry(
    entry: object,
    profile: RuntimeClosureProfile,
) -> _GenerationManifestEntry:
    if type(entry) is not dict or set(entry) != _GENERATION_MANIFEST_ENTRY_FIELDS:
        raise RuntimeAuthorityPublishError("generation manifest entry schema is invalid")
    path = entry["path"]
    entry_type = entry["type"]
    if (
        type(path) is not str
        or not path
        or path.startswith("/")
        or path == GENERATION_MANIFEST_NAME
        or len(path.encode("utf-8")) > profile.manifest_schema.max_path_bytes
        or any(component in {"", ".", ".."} for component in path.split("/"))
    ):
        raise RuntimeAuthorityPublishError("generation manifest entry path is invalid")
    if entry_type not in profile.manifest_schema.entry_types:
        raise RuntimeAuthorityPublishError("generation manifest entry type is invalid")
    if type(entry["owner_uid"]) is not int or entry["owner_uid"] != RUNTIME_AUTHORITY_OWNER_UID:
        raise RuntimeAuthorityPublishError("generation manifest entry owner is invalid")
    mode = entry["mode"]
    nlink = entry["nlink"]
    size = entry["size"]
    sha256 = entry["sha256"]
    if entry_type == "directory":
        valid = (
            type(mode) is int
            and mode in profile.manifest_schema.directory_modes
            and type(nlink) is int
            and nlink >= 2
            and type(size) is int
            and size == 0
            and sha256 is None
        )
    else:
        valid = (
            type(mode) is int
            and mode in profile.manifest_schema.file_modes
            and type(nlink) is int
            and nlink == 1
            and type(size) is int
            and size >= 0
            and type(sha256) is str
            and _SHA256.fullmatch(sha256) is not None
        )
    if not valid:
        raise RuntimeAuthorityPublishError("generation manifest entry metadata is invalid")
    return _GenerationManifestEntry(
        path=path,
        entry_type=entry_type,
        owner_uid=entry["owner_uid"],
        mode=mode,
        nlink=nlink,
        size=size,
        sha256=sha256,
    )


def _classify_forbidden_import_path(
    path: str,
    *,
    entry_type: str,
    import_roots: tuple[str, ...],
    extension_suffixes: tuple[str, ...],
) -> str | None:
    parts = tuple(path.split("/"))
    normalized_parts = tuple(
        unicodedata.normalize("NFKC", component).casefold() for component in parts
    )
    if entry_type == "file" and normalized_parts[-1].endswith(".pth"):
        return ".pth"

    normalized_suffixes = tuple(
        unicodedata.normalize("NFKC", suffix).casefold() for suffix in extension_suffixes
    )
    for import_root in import_roots:
        root_parts = tuple(import_root.split("/"))
        if len(parts) <= len(root_parts) or parts[: len(root_parts)] != root_parts:
            continue
        relative_parts = normalized_parts[len(root_parts) :]
        directory_parts = relative_parts if entry_type == "directory" else relative_parts[:-1]
        for module in ("sitecustomize", "usercustomize"):
            if module in directory_parts:
                return module
            if entry_type != "file":
                continue
            location = _import_candidate_location(relative_parts)
            if location is None:
                continue
            filename = relative_parts[-1]
            if location == "cache":
                if _is_cache_tag_bytecode(filename, module):
                    return module
                continue
            if filename in {f"{module}.py", f"{module}.pyc"}:
                return module
            if any(filename == f"{module}{suffix}" for suffix in normalized_suffixes):
                return module
    return None


def _import_candidate_location(relative_parts: tuple[str, ...]) -> str | None:
    if len(relative_parts) == 1:
        return "top"
    if len(relative_parts) == 2 and relative_parts[0] == "__pycache__":
        return "cache"
    return None


def _is_cache_tag_bytecode(filename: str, module: str) -> bool:
    if not filename.startswith(f"{module}.") or not filename.endswith(".pyc"):
        return False
    tag = filename[len(module) + 1 : -4]
    return re.fullmatch(r"[a-z0-9_]+(?:-[a-z0-9_]+)+(?:\.opt-[0-9]+)?", tag) is not None


def _validate_generation_semantics(
    generation_fd: int,
    entries: tuple[_GenerationManifestEntry, ...],
    slot: RuntimeGenerationSlot,
) -> None:
    by_path = {entry.path: entry for entry in entries}
    role_payloads = tuple(
        (role, _manifest_role_payload(role, slot.generation_path)) for role in slot.roles.values()
    )
    import_roots = tuple(
        sorted(
            {
                path
                for _role, payload in role_payloads
                for path in (payload["app_source"], *payload["site_packages"])
                if type(path) is str
            }
        )
    )
    for entry in entries:
        classification = _classify_forbidden_import_path(
            entry.path,
            entry_type=entry.entry_type,
            import_roots=import_roots,
            extension_suffixes=tuple(importlib.machinery.EXTENSION_SUFFIXES),
        )
        if classification is not None:
            raise RuntimeAuthorityPublishError(
                f"generation tree contains a forbidden import hook: {classification}"
            )

    pyvenv_entry = by_path.get("pyvenv.cfg")
    if pyvenv_entry is None or pyvenv_entry.entry_type != "file":
        raise RuntimeAuthorityPublishError("generation pyvenv.cfg is missing")
    pyvenv_payload = _read_file_at(
        generation_fd,
        "pyvenv.cfg",
        owner_uid=pyvenv_entry.owner_uid,
        file_mode=pyvenv_entry.mode,
        max_bytes=MAX_PYVENV_CONFIG_BYTES,
        error_type=RuntimeAuthorityPublishError,
        label="generation pyvenv.cfg",
    )
    if pyvenv_payload is None:
        raise RuntimeAuthorityPublishError("generation pyvenv.cfg is missing")
    _validate_pyvenv_config(pyvenv_payload)

    for role, role_payload in role_payloads:
        app_source = role_payload["app_source"]
        if type(app_source) is not str:
            raise RuntimeAuthorityPublishError("generation app source is invalid")
        module_parts = role.module.split(".")
        module_stem = f"{app_source}/{'/'.join(module_parts)}"
        module_file = f"{module_stem}.py"
        package_file = f"{module_stem}/__init__.py"
        resolutions = [
            path
            for path in (module_file, package_file)
            if by_path.get(path) is not None and by_path[path].entry_type == "file"
        ]
        if not resolutions:
            raise RuntimeAuthorityPublishError(
                f"allowlisted module {role.module} has no unique regular source"
            )
        if len(resolutions) != 1:
            raise RuntimeAuthorityPublishError(
                f"allowlisted module {role.module} source is ambiguous"
            )
        for depth in range(1, len(module_parts)):
            package_init = f"{app_source}/{'/'.join(module_parts[:depth])}/__init__.py"
            parent = by_path.get(package_init)
            if parent is None or parent.entry_type != "file":
                raise RuntimeAuthorityPublishError(
                    f"allowlisted module {role.module} uses a namespace package"
                )


def _validate_pyvenv_config(payload: bytes) -> None:
    try:
        text = payload.decode("utf-8")
    except (UnicodeDecodeError, ValueError) as exc:
        raise RuntimeAuthorityPublishError("generation pyvenv.cfg is not valid UTF-8") from exc
    if not text.endswith("\n") or "\r" in text:
        raise RuntimeAuthorityPublishError("generation pyvenv.cfg is not canonical")
    values: dict[str, str] = {}
    for line in text.splitlines():
        key, separator, value = line.partition("=")
        normalized_key = unicodedata.normalize("NFKC", key.strip()).casefold()
        if not separator or not normalized_key or not value.strip():
            raise RuntimeAuthorityPublishError("generation pyvenv.cfg is malformed")
        if normalized_key in values:
            raise RuntimeAuthorityPublishError("generation pyvenv.cfg contains a duplicate key")
        if normalized_key != "include-system-site-packages":
            raise RuntimeAuthorityPublishError("generation pyvenv.cfg contains an unknown key")
        values[normalized_key] = value.strip()
    if values.get("include-system-site-packages") != "false":
        raise RuntimeAuthorityPublishError(
            "generation pyvenv.cfg include-system-site-packages must be false"
        )


def _validate_generation_tree(
    generation_fd: int,
    entries: tuple[_GenerationManifestEntry, ...],
    profile: RuntimeClosureProfile,
) -> tuple[tuple[str, tuple[int, ...]], ...]:
    expected = {entry.path: entry for entry in entries}
    observed: dict[str, tuple[int, ...]] = {}
    observed_budget = _GenerationTreeBudget(profile.manifest_schema)
    root_before = os.fstat(generation_fd)
    _walk_generation_directory(
        generation_fd,
        relative_parent="",
        expected=expected,
        observed=observed,
        profile=profile,
        budget=observed_budget,
    )
    root_after = os.fstat(generation_fd)
    if _identity(root_before) != _identity(root_after):
        raise RuntimeAuthorityPublishError("generation tree root identity changed while validating")
    if root_before.st_nlink < 2:
        raise RuntimeAuthorityPublishError("generation tree root directory link count is invalid")
    if set(observed) != set(expected):
        raise RuntimeAuthorityPublishError("generation tree does not exactly match manifest paths")
    return tuple(sorted(observed.items()))


def _walk_generation_directory(
    directory_fd: int,
    *,
    relative_parent: str,
    expected: Mapping[str, _GenerationManifestEntry],
    observed: dict[str, tuple[int, ...]],
    profile: RuntimeClosureProfile,
    budget: _GenerationTreeBudget,
) -> None:
    with os.scandir(directory_fd) as iterator:
        for directory_entry in iterator:
            _validate_generation_directory_entry(
                directory_fd,
                directory_entry,
                relative_parent=relative_parent,
                expected=expected,
                observed=observed,
                profile=profile,
                budget=budget,
            )


def _validate_generation_directory_entry(
    directory_fd: int,
    directory_entry: os.DirEntry[str],
    *,
    relative_parent: str,
    expected: Mapping[str, _GenerationManifestEntry],
    observed: dict[str, tuple[int, ...]],
    profile: RuntimeClosureProfile,
    budget: _GenerationTreeBudget,
) -> None:
    name = directory_entry.name
    if not relative_parent and name == GENERATION_MANIFEST_NAME:
        manifest_stat = directory_entry.stat(follow_symlinks=False)
        _require_file_stat(
            manifest_stat,
            owner_uid=RUNTIME_AUTHORITY_OWNER_UID,
            mode=GENERATION_MANIFEST_MODE,
            error_type=RuntimeAuthorityPublishError,
            label="generation manifest",
        )
        return
    relative_path = name if not relative_parent else f"{relative_parent}/{name}"
    try:
        path_size = _utf8_size(
            relative_path,
            RuntimeAuthorityPublishError,
            "generation tree path",
        )
    except RuntimeAuthorityPublishError as exc:
        raise RuntimeAuthorityPublishError("generation tree path is not valid UTF-8") from exc
    if path_size > profile.manifest_schema.max_path_bytes:
        raise RuntimeAuthorityPublishError("generation tree path is too long")
    budget.observe_entry(relative_path)
    named = directory_entry.stat(follow_symlinks=False)
    if not stat.S_ISDIR(named.st_mode) and not stat.S_ISREG(named.st_mode):
        raise RuntimeAuthorityPublishError("generation tree contains a symlink or special entry")
    entry = expected.get(relative_path)
    if entry is None:
        raise RuntimeAuthorityPublishError(
            "generation tree contains a path absent from its manifest"
        )
    if stat.S_ISDIR(named.st_mode):
        if entry.entry_type != "directory" or named.st_nlink != entry.nlink:
            raise RuntimeAuthorityPublishError(
                "generation tree directory metadata does not match its manifest: "
                f"{relative_path} has nlink {named.st_nlink}, expected {entry.nlink}"
            )
        child_fd = _open_directory_entry(
            directory_fd,
            name,
            named,
            owner_uid=entry.owner_uid,
            mode=entry.mode,
            error_type=RuntimeAuthorityPublishError,
            label=f"generation tree directory {relative_path}",
        )
        try:
            _walk_generation_directory(
                child_fd,
                relative_parent=relative_path,
                expected=expected,
                observed=observed,
                profile=profile,
                budget=budget,
            )
            after = os.fstat(child_fd)
            active = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            if _identity(named) != _identity(after) or _identity(named) != _identity(active):
                raise RuntimeAuthorityPublishError(
                    "generation tree directory identity changed while validating"
                )
        finally:
            with suppress(OSError):
                os.close(child_fd)
    else:
        if entry.entry_type != "file" or named.st_nlink != entry.nlink:
            raise RuntimeAuthorityPublishError(
                "generation tree file metadata does not match its manifest"
            )
        _hash_generation_file_at(
            directory_fd,
            name,
            named=named,
            entry=entry,
            budget=budget,
            label=f"generation tree file {relative_path}",
        )
    observed[relative_path] = _identity(named)


def _hash_generation_file_at(
    parent_fd: int,
    name: str,
    *,
    named: os.stat_result,
    entry: _GenerationManifestEntry,
    budget: _GenerationTreeBudget,
    label: str,
) -> None:
    _require_file_stat(
        named,
        owner_uid=entry.owner_uid,
        mode=entry.mode,
        error_type=RuntimeAuthorityPublishError,
        label=label,
    )
    budget.require_file_size(named.st_size)
    if named.st_size != entry.size:
        raise RuntimeAuthorityPublishError("generation tree file bytes do not match its manifest")
    descriptor = -1
    try:
        descriptor = os.open(
            name,
            os.O_RDONLY
            | _required_flag("O_NOFOLLOW", RuntimeAuthorityPublishError)
            | getattr(os, "O_CLOEXEC", 0),
            dir_fd=parent_fd,
        )
        before = os.fstat(descriptor)
        if _identity(before) != _identity(named):
            raise RuntimeAuthorityPublishError(f"{label} identity changed while opening")
        digest = hashlib.sha256()
        total = 0
        while True:
            chunk = os.read(descriptor, FILE_HASH_CHUNK_BYTES)
            if not chunk:
                break
            total += len(chunk)
            if total > named.st_size:
                raise RuntimeAuthorityPublishError(
                    "generation tree file bytes do not match its manifest"
                )
            budget.observe_file_bytes(len(chunk))
            digest.update(chunk)
        after = os.fstat(descriptor)
        active = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if _identity(before) != _identity(after) or _identity(before) != _identity(active):
            raise RuntimeAuthorityPublishError(f"{label} identity changed while reading")
        if total != entry.size or digest.hexdigest() != entry.sha256:
            raise RuntimeAuthorityPublishError(
                "generation tree file bytes do not match its manifest"
            )
    finally:
        if descriptor >= 0:
            with suppress(OSError):
                os.close(descriptor)


def _consume_generation_evidence(
    evidence: tuple[_DurableGenerationEvidence, ...],
    record: RuntimeAuthorityRecord,
    profile: RuntimeClosureProfile,
) -> None:
    if len(evidence) != 1 + (record.prior is not None):
        raise RuntimeAuthorityPublishError("durable generation evidence is incomplete")
    try:
        refreshed = _revalidate_record_generations(record, profile)
    except RuntimeAuthorityPublishError as exc:
        raise RuntimeAuthorityPublishError("durable generation evidence expired") from exc
    if refreshed != evidence:
        raise RuntimeAuthorityPublishError("durable generation evidence expired")


def _validate_publication_transition(
    previous: RuntimeAuthorityRecord | None,
    candidate: RuntimeAuthorityRecord,
    profile: RuntimeClosureProfile,
) -> None:
    _validate_record(candidate, profile)
    if previous is None:
        if candidate.state is not RuntimeAuthorityState.ACTIVE or candidate.prior is not None:
            raise RuntimeAuthorityPublishError("first authority record must have no prior slot")
        if candidate.sequence != 1:
            raise RuntimeAuthorityPublishError("first authority sequence must be one")
        return
    _validate_record(previous, profile)
    if candidate.operation_id == previous.operation_id:
        raise RuntimeAuthorityPublishError("authority operation id conflicts")
    if candidate.sequence != previous.sequence + 1:
        raise RuntimeAuthorityPublishError("authority sequence must advance by one")
    if candidate.state is RuntimeAuthorityState.ACTIVE:
        forbidden = {previous.current.generation_id}
        if previous.prior is not None:
            forbidden.add(previous.prior.generation_id)
        valid = candidate.current.generation_id not in forbidden and candidate.prior == replace(
            previous.current,
            lifecycle=RuntimeGenerationLifecycle.ROLLBACK_READY,
        )
    else:
        valid = (
            previous.state is RuntimeAuthorityState.ACTIVE
            and previous.prior is not None
            and previous.prior.lifecycle is RuntimeGenerationLifecycle.ROLLBACK_READY
            and candidate.current
            == replace(
                previous.prior,
                lifecycle=RuntimeGenerationLifecycle.ACTIVE,
            )
            and candidate.prior
            == replace(
                previous.current,
                lifecycle=RuntimeGenerationLifecycle.FAILED,
            )
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


def _fsync_authority_parent(
    parent_fd: int,
    lock_lease: _DeploymentLockLease,
) -> None:
    lock_lease.assert_current()
    try:
        _fsync_descriptor(parent_fd, phase_name="parent_fsync")
    except OSError as exc:
        raise RuntimeAuthorityDurabilityError(
            "runtime authority durability remains blocked"
        ) from exc
    lock_lease.assert_current()


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
    "RuntimeAuthorityDurabilityError",
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
