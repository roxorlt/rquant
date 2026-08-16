"""Anchored hostile-candidate quarantine and immutable generation publication."""

from __future__ import annotations

import ctypes
import errno
import hashlib
import os
import re
import stat
import sys
import unicodedata
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType

import rquant.runtime_authority as authority
from rquant.runtime_authority import (
    MAX_GENERATION_MANIFEST_BYTES,
    RuntimeClosureProfile,
    RuntimeGenerationLifecycle,
    RuntimeGenerationSlot,
    RuntimeManifestSchema,
    RuntimeRoleSpec,
    load_production_runtime_profile,
)
from rquant.strict_json import StrictJsonError, canonical_json_bytes, strict_json_loads

REQUEST_SCHEMA_VERSION = 1
MAX_REQUEST_BYTES = 64 * 1024
MAX_CANDIDATE_BASENAME_BYTES = 128
MAX_AUDIT_COMMIT_BYTES = authority.MAX_COMMIT_BYTES
COPY_CHUNK_BYTES = authority.FILE_HASH_CHUNK_BYTES
TEMP_DIRECTORY_MODE = 0o700
TEMP_FILE_MODE = 0o600

_REQUEST_FIELDS = {
    "schema_version",
    "operation_id",
    "candidate_id",
    "candidate_basename",
    "untrusted_commit",
    "untrusted_manifest_hash",
}
_MANIFEST_FIELDS = {"schema_id", "profile_id", "roles", "entries"}
_OPERATION_ID = re.compile(r"[0-9a-f]{32}")
_CANDIDATE_ID = re.compile(r"[0-9a-f]{64}")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_BASENAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")


def _production_anchor_policy() -> MappingProxyType[Path, tuple[int, int]]:
    policy: dict[Path, tuple[int, int]] = {}
    for root in (
        authority.PRODUCTION_INBOX_ROOT,
        authority.PRODUCTION_QUARANTINE_ROOT,
        authority.PRODUCTION_GENERATION_ROOT,
    ):
        for path in (Path("/"), *reversed(root.parents[:-1]), root):
            policy[path] = (0, 0o755)
    return MappingProxyType(policy)


_ANCHOR_DIRECTORY_POLICY: Mapping[Path, tuple[int, int]] = _production_anchor_policy()


def _no_failpoint(_stage: str) -> None:
    return


_FAILPOINT: Callable[[str], None] = _no_failpoint
_FSYNC_PHASE = ""


def _resolve_atomic_rename_noreplace() -> Callable[[int, str, int, str], None] | None:
    libc = ctypes.CDLL(None, use_errno=True)
    if sys.platform.startswith("linux"):
        primitive = getattr(libc, "renameat2", None)
        flags = 1  # RENAME_NOREPLACE
    elif sys.platform == "darwin":
        primitive = getattr(libc, "renameatx_np", None)
        flags = 4  # RENAME_EXCL
    else:
        return None
    if primitive is None:
        return None
    primitive.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    primitive.restype = ctypes.c_int

    def rename(source_fd: int, source: str, target_fd: int, target: str) -> None:
        ctypes.set_errno(0)
        result = primitive(
            source_fd,
            os.fsencode(source),
            target_fd,
            os.fsencode(target),
            flags,
        )
        if result != 0:
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error), target)

    return rename


_ATOMIC_RENAME_NOREPLACE = _resolve_atomic_rename_noreplace()


class RuntimeQuarantineError(RuntimeError):
    """A quarantine request or filesystem transaction failed closed."""


class RuntimeQuarantineDurabilityError(RuntimeQuarantineError):
    """A visible generation could not be durably synchronized."""


class RuntimeQuarantineStatus(StrEnum):
    PUBLISHED = "published"
    IDEMPOTENT = "idempotent"
    PUBLISHED_AFTER_RECOVERY = "published_after_recovery"


@dataclass(frozen=True)
class RuntimeQuarantineRequest:
    schema_version: int
    operation_id: str
    candidate_id: str
    candidate_basename: str
    untrusted_commit: str
    untrusted_manifest_hash: str

    def __post_init__(self) -> None:
        if type(self.schema_version) is not int or self.schema_version != REQUEST_SCHEMA_VERSION:
            raise RuntimeQuarantineError("quarantine request schema is unsupported")
        _require_pattern(self.operation_id, _OPERATION_ID, "operation id")
        _require_pattern(self.candidate_id, _CANDIDATE_ID, "candidate id")
        _require_pattern(self.untrusted_manifest_hash, _SHA256, "untrusted manifest hash")
        if (
            type(self.candidate_basename) is not str
            or _BASENAME.fullmatch(self.candidate_basename) is None
            or self.candidate_basename in {".", ".."}
            or _utf8_size(self.candidate_basename, "candidate basename")
            > MAX_CANDIDATE_BASENAME_BYTES
        ):
            raise RuntimeQuarantineError("candidate basename is invalid")
        if (
            type(self.untrusted_commit) is not str
            or not self.untrusted_commit
            or any(ord(character) < 0x20 for character in self.untrusted_commit)
            or _utf8_size(self.untrusted_commit, "untrusted commit") > MAX_AUDIT_COMMIT_BYTES
        ):
            raise RuntimeQuarantineError("untrusted commit audit metadata is invalid")


@dataclass(frozen=True)
class RuntimeQuarantineResult:
    status: RuntimeQuarantineStatus
    slot: RuntimeGenerationSlot
    untrusted_candidate_id: str
    untrusted_manifest_hash: str
    untrusted_manifest_matches: bool

    def __post_init__(self) -> None:
        if not isinstance(self.status, RuntimeQuarantineStatus):
            raise RuntimeQuarantineError("quarantine result status is invalid")
        if not isinstance(self.slot, RuntimeGenerationSlot):
            raise RuntimeQuarantineError("quarantine result slot is invalid")
        _require_pattern(self.untrusted_candidate_id, _CANDIDATE_ID, "candidate id")
        _require_pattern(
            self.untrusted_manifest_hash,
            _SHA256,
            "untrusted manifest hash",
        )
        if type(self.untrusted_manifest_matches) is not bool:
            raise RuntimeQuarantineError("quarantine manifest comparison is invalid")

    @property
    def generation_id(self) -> str:
        return self.slot.generation_id


@dataclass(frozen=True)
class _TreeEntry:
    path: str
    entry_type: str
    owner_uid: int
    mode: int
    nlink: int
    size: int
    sha256: str | None
    identity: tuple[int, ...]

    def payload(self) -> dict[str, object]:
        return {
            "path": self.path,
            "type": self.entry_type,
            "owner_uid": self.owner_uid,
            "mode": self.mode,
            "nlink": self.nlink,
            "size": self.size,
            "sha256": self.sha256,
        }


@dataclass
class _TreeBudget:
    schema: RuntimeManifestSchema
    entries: int = 0
    total_bytes: int = 0
    manifest_bytes: int = 0

    def observe_path(self, path: str, normalized_path: str) -> None:
        self.entries += 1
        if self.entries > self.schema.max_entries:
            raise RuntimeQuarantineError("candidate entry budget exceeded")
        if len(path.split("/")) > self.schema.max_depth:
            raise RuntimeQuarantineError("candidate path depth budget exceeded")
        if _utf8_size(path, "candidate path") > self.schema.max_path_bytes:
            raise RuntimeQuarantineError("candidate path byte budget exceeded")
        self.manifest_bytes += _utf8_size(
            normalized_path,
            "normalized candidate path",
        )
        if self.manifest_bytes > MAX_GENERATION_MANIFEST_BYTES:
            raise RuntimeQuarantineError("candidate manifest byte budget exceeded")

    def observe_manifest_entry(self, entry: _TreeEntry) -> None:
        self.manifest_bytes += len(canonical_json_bytes(entry.payload())) + 1
        if self.manifest_bytes > MAX_GENERATION_MANIFEST_BYTES:
            raise RuntimeQuarantineError("candidate manifest byte budget exceeded")

    def observe_file(self, size: int) -> None:
        if type(size) is not int or size < 0 or size > self.schema.max_file_bytes:
            raise RuntimeQuarantineError("candidate file byte budget exceeded")
        self.total_bytes += size
        if self.total_bytes > self.schema.max_total_bytes:
            raise RuntimeQuarantineError("candidate total byte budget exceeded")


@dataclass
class _CandidateLease:
    descriptors: list[int]
    inbox_fd: int
    candidate_id_fd: int
    candidate_fd: int
    candidate_id_name: str
    candidate_name: str
    candidate_id_identity: tuple[int, ...]
    candidate_identity: tuple[int, ...]
    source_owner_uid: int

    def assert_current(self) -> None:
        try:
            active_id = os.stat(
                self.candidate_id_name,
                dir_fd=self.inbox_fd,
                follow_symlinks=False,
            )
            active_candidate = os.stat(
                self.candidate_name,
                dir_fd=self.candidate_id_fd,
                follow_symlinks=False,
            )
        except OSError as exc:
            raise RuntimeQuarantineError("candidate path identity changed") from exc
        if (
            _identity(os.fstat(self.candidate_id_fd)) != self.candidate_id_identity
            or _identity(active_id) != self.candidate_id_identity
            or _identity(os.fstat(self.candidate_fd)) != self.candidate_identity
            or _identity(active_candidate) != self.candidate_identity
        ):
            raise RuntimeQuarantineError("candidate path identity changed")

    def close(self) -> None:
        _close_descriptors(self.descriptors)
        self.descriptors.clear()


@dataclass
class _PublicationState:
    renamed: bool = False


def parse_runtime_quarantine_request(
    payload: str | bytes | bytearray,
) -> RuntimeQuarantineRequest:
    if not isinstance(payload, (str, bytes, bytearray)):
        raise RuntimeQuarantineError("quarantine request must be JSON text")
    try:
        encoded = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
    except (UnicodeEncodeError, ValueError) as exc:
        raise RuntimeQuarantineError("quarantine request is not valid UTF-8") from exc
    if len(encoded) > MAX_REQUEST_BYTES:
        raise RuntimeQuarantineError("quarantine request is too large")
    try:
        data = strict_json_loads(encoded)
    except (StrictJsonError, UnicodeDecodeError, ValueError, RecursionError) as exc:
        raise RuntimeQuarantineError(f"quarantine request JSON is invalid: {exc}") from exc
    if type(data) is not dict or set(data) != _REQUEST_FIELDS:
        raise RuntimeQuarantineError("quarantine request has unexpected fields")
    return RuntimeQuarantineRequest(
        schema_version=data["schema_version"],
        operation_id=data["operation_id"],
        candidate_id=data["candidate_id"],
        candidate_basename=data["candidate_basename"],
        untrusted_commit=data["untrusted_commit"],
        untrusted_manifest_hash=data["untrusted_manifest_hash"],
    )


def publish_runtime_candidate(request: RuntimeQuarantineRequest) -> RuntimeQuarantineResult:
    if not isinstance(request, RuntimeQuarantineRequest):
        raise RuntimeQuarantineError("quarantine request model is required")
    profile = load_production_runtime_profile()
    _validate_loaded_profile(profile)
    temporary_name = _temporary_name(request.operation_id)
    manifest_bytes: bytes | None = None
    generation_id: str | None = None
    candidate_lease: _CandidateLease | None = None
    quarantine_descriptors: list[int] = []
    temporary_fd = -1
    deployment_lock: authority.RuntimeDeploymentLock | None = None
    publication_state = _PublicationState()
    try:
        deployment_lock = authority.acquire_runtime_deployment_lock()
        deployment_lock.assert_current()
        candidate_lease = _open_candidate(request, profile)
        quarantine_descriptors, quarantine_fd = _open_fixed_root(
            profile.quarantine_root,
            "quarantine root",
        )
        _create_temporary_directory(quarantine_fd, temporary_name)
        temporary_fd = _open_owned_directory_at(
            quarantine_fd,
            temporary_name,
            allowed_modes={TEMP_DIRECTORY_MODE},
            label="operation quarantine",
        )
        entries = _copy_candidate_tree(candidate_lease, temporary_fd, profile)
        candidate_lease.assert_current()
        roles = _manifest_roles(profile)
        manifest_bytes = _canonical_manifest(profile, roles, entries)
        if len(manifest_bytes) > MAX_GENERATION_MANIFEST_BYTES:
            raise RuntimeQuarantineError("root-derived generation manifest is too large")
        _write_manifest(temporary_fd, manifest_bytes)
        _set_owned_mode(temporary_fd, authority.GENERATION_DIRECTORY_MODE)
        _fsync_descriptor(temporary_fd, "directory")
        _FAILPOINT("completed_quarantine_directory_fsync")
        _assert_named_identity(
            quarantine_fd,
            temporary_name,
            temporary_fd,
            "operation quarantine",
        )
        os.close(temporary_fd)
        temporary_fd = -1
        candidate_lease.close()
        candidate_lease = None
        _close_descriptors(quarantine_descriptors)
        quarantine_descriptors.clear()
        _FAILPOINT("close_reopen")
        verified = _revalidate_quarantine(
            temporary_name,
            profile,
            transition_hook="first_full_revalidation",
        )
        if verified != manifest_bytes:
            raise RuntimeQuarantineError("closed quarantine manifest identity changed")
        generation_id = hashlib.sha256(verified).hexdigest()
        if _revalidate_quarantine(temporary_name, profile) != verified:
            raise RuntimeQuarantineError("quarantine changed before generation publication")
        status = _publish_verified_quarantine(
            temporary_name,
            generation_id,
            verified,
            profile,
            deployment_lock,
            publication_state,
        )
        return _result(request, profile, generation_id, status)
    except RuntimeQuarantineError:
        raise
    except authority.RuntimeAuthorityPublishError as exc:
        raise RuntimeQuarantineError("deployment lock transaction failed") from exc
    except OSError as exc:
        if publication_state.renamed:
            raise RuntimeQuarantineDurabilityError(
                "generation publication durability failed"
            ) from exc
        raise RuntimeQuarantineError("candidate quarantine transaction failed") from exc
    finally:
        if temporary_fd >= 0:
            with suppress(OSError):
                os.close(temporary_fd)
        if candidate_lease is not None:
            candidate_lease.close()
        _close_descriptors(quarantine_descriptors)
        if deployment_lock is not None:
            deployment_lock.close()


def cleanup_runtime_quarantine(operation_id: str) -> bool:
    _require_pattern(operation_id, _OPERATION_ID, "operation id")
    profile = load_production_runtime_profile()
    _validate_loaded_profile(profile)
    descriptors: list[int] = []
    deployment_lock: authority.RuntimeDeploymentLock | None = None
    try:
        deployment_lock = authority.acquire_runtime_deployment_lock()
        deployment_lock.assert_current()
        descriptors, quarantine_fd = _open_fixed_root(
            profile.quarantine_root,
            "quarantine root",
        )
        name = _temporary_name(operation_id)
        try:
            os.stat(name, dir_fd=quarantine_fd, follow_symlinks=False)
        except FileNotFoundError:
            return False
        _FAILPOINT("cleanup_before_remove")
        _remove_temporary_tree(quarantine_fd, name)
        _fsync_descriptor(quarantine_fd, "quarantine_parent")
        return True
    except RuntimeQuarantineError:
        raise
    except authority.RuntimeAuthorityPublishError as exc:
        raise RuntimeQuarantineError("deployment lock cleanup failed") from exc
    except OSError as exc:
        raise RuntimeQuarantineError("operation quarantine cleanup failed") from exc
    finally:
        _close_descriptors(descriptors)
        if deployment_lock is not None:
            deployment_lock.close()


def _validate_loaded_profile(profile: RuntimeClosureProfile) -> None:
    if not isinstance(profile, RuntimeClosureProfile):
        raise RuntimeQuarantineError("loaded runtime profile is invalid")
    if (
        profile.inbox_root != authority.PRODUCTION_INBOX_ROOT
        or profile.quarantine_root != authority.PRODUCTION_QUARANTINE_ROOT
        or profile.generation_root != authority.PRODUCTION_GENERATION_ROOT
        or "publish" not in profile.allowed_operations
    ):
        raise RuntimeQuarantineError("loaded runtime profile does not authorize quarantine")


def _require_pattern(value: object, pattern: re.Pattern[str], label: str) -> None:
    if type(value) is not str or pattern.fullmatch(value) is None:
        raise RuntimeQuarantineError(f"{label} is invalid")


def _utf8_size(value: str, label: str) -> int:
    try:
        return len(value.encode("utf-8"))
    except (UnicodeEncodeError, ValueError) as exc:
        raise RuntimeQuarantineError(f"{label} is not valid UTF-8") from exc


def _temporary_name(operation_id: str) -> str:
    return f".quarantine-{operation_id}"


def _identity(observed: os.stat_result) -> tuple[int, ...]:
    return (
        observed.st_dev,
        observed.st_ino,
        observed.st_mode,
        observed.st_nlink,
        observed.st_uid,
        observed.st_gid,
        observed.st_size,
        observed.st_mtime_ns,
        observed.st_ctime_ns,
    )


def _inode_identity(observed: os.stat_result) -> tuple[int, int]:
    return observed.st_dev, observed.st_ino


def _required_flag(name: str) -> int:
    value = getattr(os, name, None)
    if type(value) is not int or value == 0:
        raise RuntimeQuarantineError(f"platform lacks required {name} support")
    return value


def _close_descriptors(descriptors: list[int]) -> None:
    while descriptors:
        descriptor = descriptors.pop()
        with suppress(OSError):
            os.close(descriptor)


def _require_directory_stat(
    observed: os.stat_result,
    *,
    owner_uid: int,
    modes: set[int],
    label: str,
) -> None:
    mode = stat.S_IMODE(observed.st_mode)
    if (
        not stat.S_ISDIR(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != owner_uid
        or observed.st_nlink < 2
        or mode not in modes
        or mode & 0o022
    ):
        raise RuntimeQuarantineError(f"{label} directory metadata is unsafe")


def _require_file_stat(
    observed: os.stat_result,
    *,
    owner_uid: int,
    modes: set[int],
    label: str,
) -> None:
    mode = stat.S_IMODE(observed.st_mode)
    if (
        not stat.S_ISREG(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != owner_uid
        or observed.st_nlink != 1
        or mode not in modes
        or mode & (stat.S_ISUID | stat.S_ISGID | stat.S_ISVTX)
    ):
        raise RuntimeQuarantineError(f"{label} file metadata is unsafe")


def _open_fixed_root(path: Path, label: str) -> tuple[list[int], int]:
    if not path.is_absolute():
        raise RuntimeQuarantineError(f"{label} is not absolute")
    flags = (
        os.O_RDONLY
        | _required_flag("O_DIRECTORY")
        | _required_flag("O_NOFOLLOW")
        | getattr(os, "O_CLOEXEC", 0)
    )
    descriptors: list[int] = []
    try:
        current = Path("/")
        parent_fd = -1
        components: tuple[str | None, ...] = (None, *path.parts[1:])
        for component in components:
            if component is not None:
                current /= component
            expected = _ANCHOR_DIRECTORY_POLICY.get(current)
            if expected is None:
                raise RuntimeQuarantineError(f"{label} ancestor policy is incomplete")
            if component is None:
                named = os.stat(current, follow_symlinks=False)
                descriptor = os.open(current, flags)
            else:
                named = os.stat(component, dir_fd=parent_fd, follow_symlinks=False)
                descriptor = os.open(component, flags, dir_fd=parent_fd)
            _require_directory_stat(
                named,
                owner_uid=expected[0],
                modes={expected[1]},
                label=f"{label} ancestor {current}",
            )
            opened = os.fstat(descriptor)
            _require_directory_stat(
                opened,
                owner_uid=expected[0],
                modes={expected[1]},
                label=f"{label} ancestor {current}",
            )
            if _identity(named) != _identity(opened):
                os.close(descriptor)
                raise RuntimeQuarantineError(f"{label} ancestor identity changed")
            descriptors.append(descriptor)
            parent_fd = descriptor
        return descriptors, parent_fd
    except OSError as exc:
        _close_descriptors(descriptors)
        raise RuntimeQuarantineError(f"{label} ancestor is unavailable") from exc
    except BaseException:
        _close_descriptors(descriptors)
        raise


def _open_candidate(
    request: RuntimeQuarantineRequest,
    profile: RuntimeClosureProfile,
) -> _CandidateLease:
    descriptors, inbox_fd = _open_fixed_root(profile.inbox_root, "candidate inbox")
    try:
        candidate_id_fd, candidate_id_stat = _open_source_directory(
            inbox_fd,
            request.candidate_id,
            owner_uid=None,
            modes=set(profile.manifest_schema.directory_modes),
            label="candidate id",
        )
        descriptors.append(candidate_id_fd)
        candidate_fd, candidate_stat = _open_source_directory(
            candidate_id_fd,
            request.candidate_basename,
            owner_uid=candidate_id_stat.st_uid,
            modes=set(profile.manifest_schema.directory_modes),
            label="candidate root",
        )
        descriptors.append(candidate_fd)
        return _CandidateLease(
            descriptors=descriptors,
            inbox_fd=inbox_fd,
            candidate_id_fd=candidate_id_fd,
            candidate_fd=candidate_fd,
            candidate_id_name=request.candidate_id,
            candidate_name=request.candidate_basename,
            candidate_id_identity=_identity(candidate_id_stat),
            candidate_identity=_identity(candidate_stat),
            source_owner_uid=candidate_stat.st_uid,
        )
    except BaseException:
        _close_descriptors(descriptors)
        raise


def _open_source_directory(
    parent_fd: int,
    name: str,
    *,
    owner_uid: int | None,
    modes: set[int],
    label: str,
) -> tuple[int, os.stat_result]:
    named = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    expected_owner = named.st_uid if owner_uid is None else owner_uid
    _require_directory_stat(named, owner_uid=expected_owner, modes=modes, label=label)
    descriptor = -1
    try:
        descriptor = os.open(
            name,
            os.O_RDONLY
            | _required_flag("O_DIRECTORY")
            | _required_flag("O_NOFOLLOW")
            | getattr(os, "O_CLOEXEC", 0),
            dir_fd=parent_fd,
        )
        opened = os.fstat(descriptor)
        _require_directory_stat(opened, owner_uid=expected_owner, modes=modes, label=label)
        if _identity(named) != _identity(opened):
            raise RuntimeQuarantineError(f"{label} identity changed while opening")
        return descriptor, opened
    except BaseException:
        if descriptor >= 0:
            with suppress(OSError):
                os.close(descriptor)
        raise


def _create_temporary_directory(parent_fd: int, name: str) -> None:
    try:
        os.mkdir(name, TEMP_DIRECTORY_MODE, dir_fd=parent_fd)
    except FileExistsError as exc:
        raise RuntimeQuarantineError(
            "operation quarantine already exists; exact cleanup is required"
        ) from exc
    _FAILPOINT("temporary_created_before_mode_fix")
    _apply_exact_created_directory_mode(
        parent_fd,
        name,
        TEMP_DIRECTORY_MODE,
        label="operation quarantine",
    )
    descriptor = _open_owned_directory_at(
        parent_fd,
        name,
        allowed_modes={TEMP_DIRECTORY_MODE},
        label="operation quarantine",
    )
    os.close(descriptor)
    _FAILPOINT("temporary_directory_ready")


def _apply_exact_created_directory_mode(
    parent_fd: int,
    name: str,
    mode: int,
    *,
    label: str,
) -> None:
    named = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    observed_mode = stat.S_IMODE(named.st_mode)
    if (
        not stat.S_ISDIR(named.st_mode)
        or stat.S_ISLNK(named.st_mode)
        or named.st_uid != authority.RUNTIME_AUTHORITY_OWNER_UID
        or named.st_nlink < 2
        or observed_mode & ~mode
    ):
        raise RuntimeQuarantineError(f"{label} directory metadata is unsafe")
    os.chmod(
        name,
        mode,
        dir_fd=parent_fd,
        follow_symlinks=False,
    )
    exact = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    _require_directory_stat(
        exact,
        owner_uid=authority.RUNTIME_AUTHORITY_OWNER_UID,
        modes={mode},
        label=label,
    )


def _open_owned_directory_at(
    parent_fd: int,
    name: str,
    *,
    allowed_modes: set[int],
    label: str,
) -> int:
    named = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    descriptor = -1
    try:
        descriptor = os.open(
            name,
            os.O_RDONLY
            | _required_flag("O_DIRECTORY")
            | _required_flag("O_NOFOLLOW")
            | getattr(os, "O_CLOEXEC", 0),
            dir_fd=parent_fd,
        )
        opened = os.fstat(descriptor)
        _require_directory_stat(
            named,
            owner_uid=authority.RUNTIME_AUTHORITY_OWNER_UID,
            modes=allowed_modes,
            label=label,
        )
        _require_directory_stat(
            opened,
            owner_uid=authority.RUNTIME_AUTHORITY_OWNER_UID,
            modes=allowed_modes,
            label=label,
        )
        if _identity(named) != _identity(opened):
            raise RuntimeQuarantineError(f"{label} identity changed while opening")
        return descriptor
    except BaseException:
        if descriptor >= 0:
            with suppress(OSError):
                os.close(descriptor)
        raise


def _set_owned_mode(descriptor: int, mode: int) -> None:
    observed = os.fstat(descriptor)
    if observed.st_uid != authority.RUNTIME_AUTHORITY_OWNER_UID:
        os.fchown(descriptor, authority.RUNTIME_AUTHORITY_OWNER_UID, -1)
    os.fchmod(descriptor, mode)


def _copy_candidate_tree(
    lease: _CandidateLease,
    destination_fd: int,
    profile: RuntimeClosureProfile,
) -> tuple[_TreeEntry, ...]:
    budget = _TreeBudget(profile.manifest_schema)
    normalized_paths: set[str] = set()
    source_inodes: set[tuple[int, int]] = set()
    entries: list[_TreeEntry] = []
    _copy_directory_contents(
        lease.candidate_fd,
        destination_fd,
        relative_parent="",
        normalized_parent="",
        source_owner_uid=lease.source_owner_uid,
        profile=profile,
        budget=budget,
        normalized_paths=normalized_paths,
        source_inodes=source_inodes,
        entries=entries,
    )
    return tuple(sorted(entries, key=lambda entry: entry.path))


def _copy_directory_contents(
    source_fd: int,
    destination_fd: int,
    *,
    relative_parent: str,
    normalized_parent: str,
    source_owner_uid: int,
    profile: RuntimeClosureProfile,
    budget: _TreeBudget,
    normalized_paths: set[str],
    source_inodes: set[tuple[int, int]],
    entries: list[_TreeEntry],
) -> None:
    with os.scandir(source_fd) as iterator:
        for directory_entry in iterator:
            name = directory_entry.name
            _require_leaf_name(name)
            normalized_name = _normalize_tree_component(name)
            relative = name if not relative_parent else f"{relative_parent}/{name}"
            normalized = (
                normalized_name
                if not normalized_parent
                else f"{normalized_parent}/{normalized_name}"
            )
            if not relative_parent and normalized_name == _NORMALIZED_MANIFEST_NAME:
                raise RuntimeQuarantineError("candidate contains the reserved generation manifest")
            if normalized in normalized_paths:
                raise RuntimeQuarantineError(
                    "candidate contains a duplicate normalized relative path"
                )
            normalized_paths.add(normalized)
            budget.observe_path(relative, normalized)
            _FAILPOINT(f"candidate_traversal:{relative}")
            named = directory_entry.stat(follow_symlinks=False)
            inode = _inode_identity(named)
            if inode in source_inodes:
                raise RuntimeQuarantineError("candidate contains a dev/inode alias")
            source_inodes.add(inode)
            if stat.S_ISDIR(named.st_mode):
                entry = _copy_source_directory(
                    source_fd,
                    destination_fd,
                    name,
                    relative,
                    normalized,
                    named,
                    source_owner_uid=source_owner_uid,
                    profile=profile,
                    budget=budget,
                    normalized_paths=normalized_paths,
                    source_inodes=source_inodes,
                    entries=entries,
                )
            elif stat.S_ISREG(named.st_mode):
                budget.observe_file(named.st_size)
                entry = _copy_source_file(
                    source_fd,
                    destination_fd,
                    name,
                    relative,
                    named,
                    source_owner_uid=source_owner_uid,
                    file_modes=set(profile.manifest_schema.file_modes),
                )
            else:
                raise RuntimeQuarantineError("candidate contains a symlink or special entry")
            budget.observe_manifest_entry(entry)
            entries.append(entry)


def _copy_source_directory(
    source_parent_fd: int,
    destination_parent_fd: int,
    name: str,
    relative: str,
    normalized_relative: str,
    named: os.stat_result,
    *,
    source_owner_uid: int,
    profile: RuntimeClosureProfile,
    budget: _TreeBudget,
    normalized_paths: set[str],
    source_inodes: set[tuple[int, int]],
    entries: list[_TreeEntry],
) -> _TreeEntry:
    _require_directory_stat(
        named,
        owner_uid=source_owner_uid,
        modes=set(profile.manifest_schema.directory_modes),
        label=f"candidate directory {relative}",
    )
    source_fd = -1
    destination_fd = -1
    try:
        source_fd = os.open(
            name,
            os.O_RDONLY
            | _required_flag("O_DIRECTORY")
            | _required_flag("O_NOFOLLOW")
            | getattr(os, "O_CLOEXEC", 0),
            dir_fd=source_parent_fd,
        )
        if _identity(os.fstat(source_fd)) != _identity(named):
            raise RuntimeQuarantineError(
                f"candidate directory {relative} identity changed while opening"
            )
        os.mkdir(name, TEMP_DIRECTORY_MODE, dir_fd=destination_parent_fd)
        _apply_exact_created_directory_mode(
            destination_parent_fd,
            name,
            TEMP_DIRECTORY_MODE,
            label=f"quarantine directory {relative}",
        )
        destination_fd = _open_owned_directory_at(
            destination_parent_fd,
            name,
            allowed_modes={TEMP_DIRECTORY_MODE},
            label=f"quarantine directory {relative}",
        )
        _copy_directory_contents(
            source_fd,
            destination_fd,
            relative_parent=relative,
            normalized_parent=normalized_relative,
            source_owner_uid=source_owner_uid,
            profile=profile,
            budget=budget,
            normalized_paths=normalized_paths,
            source_inodes=source_inodes,
            entries=entries,
        )
        _assert_source_identity(source_parent_fd, name, source_fd, named, relative)
        destination_mode = profile.manifest_schema.directory_modes[0]
        _set_owned_mode(destination_fd, destination_mode)
        _fsync_descriptor(destination_fd, "directory")
        observed = os.fstat(destination_fd)
        _require_directory_stat(
            observed,
            owner_uid=authority.RUNTIME_AUTHORITY_OWNER_UID,
            modes={destination_mode},
            label=f"quarantine directory {relative}",
        )
        _assert_named_identity(
            destination_parent_fd,
            name,
            destination_fd,
            f"quarantine directory {relative}",
        )
        return _TreeEntry(
            path=relative,
            entry_type="directory",
            owner_uid=observed.st_uid,
            mode=stat.S_IMODE(observed.st_mode),
            nlink=observed.st_nlink,
            size=0,
            sha256=None,
            identity=_identity(observed),
        )
    finally:
        if destination_fd >= 0:
            with suppress(OSError):
                os.close(destination_fd)
        if source_fd >= 0:
            with suppress(OSError):
                os.close(source_fd)


def _copy_source_file(
    source_parent_fd: int,
    destination_parent_fd: int,
    name: str,
    relative: str,
    named: os.stat_result,
    *,
    source_owner_uid: int,
    file_modes: set[int],
) -> _TreeEntry:
    _require_file_stat(
        named,
        owner_uid=source_owner_uid,
        modes=file_modes,
        label=f"candidate file {relative}",
    )
    source_fd = -1
    destination_fd = -1
    try:
        source_fd = os.open(
            name,
            os.O_RDONLY | _required_flag("O_NOFOLLOW") | getattr(os, "O_CLOEXEC", 0),
            dir_fd=source_parent_fd,
        )
        before = os.fstat(source_fd)
        if _identity(before) != _identity(named):
            raise RuntimeQuarantineError(
                f"candidate file {relative} identity changed while opening"
            )
        destination_fd = os.open(
            name,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | _required_flag("O_NOFOLLOW")
            | getattr(os, "O_CLOEXEC", 0),
            TEMP_FILE_MODE,
            dir_fd=destination_parent_fd,
        )
        _set_owned_mode(destination_fd, TEMP_FILE_MODE)
        digest = hashlib.sha256()
        copied = 0
        while True:
            chunk = os.read(source_fd, COPY_CHUNK_BYTES)
            if not chunk:
                break
            copied += len(chunk)
            if copied > named.st_size:
                raise RuntimeQuarantineError(f"candidate file {relative} grew while copying")
            _write_all(destination_fd, chunk)
            digest.update(chunk)
            _FAILPOINT(f"candidate_copy:{relative}")
        if copied != named.st_size:
            raise RuntimeQuarantineError(f"candidate file {relative} size changed while copying")
        _assert_source_identity(source_parent_fd, name, source_fd, named, relative)
        destination_mode = stat.S_IMODE(named.st_mode)
        _set_owned_mode(destination_fd, destination_mode)
        _FAILPOINT(f"content_file_fsync:{relative}")
        _fsync_descriptor(destination_fd, "file")
        destination = os.fstat(destination_fd)
        _require_file_stat(
            destination,
            owner_uid=authority.RUNTIME_AUTHORITY_OWNER_UID,
            modes={destination_mode},
            label=f"quarantine file {relative}",
        )
        if destination.st_size != copied:
            raise RuntimeQuarantineError(f"quarantine file {relative} size changed")
        _assert_named_identity(
            destination_parent_fd,
            name,
            destination_fd,
            f"quarantine file {relative}",
        )
        return _TreeEntry(
            path=relative,
            entry_type="file",
            owner_uid=destination.st_uid,
            mode=stat.S_IMODE(destination.st_mode),
            nlink=destination.st_nlink,
            size=copied,
            sha256=digest.hexdigest(),
            identity=_identity(destination),
        )
    finally:
        if destination_fd >= 0:
            with suppress(OSError):
                os.close(destination_fd)
        if source_fd >= 0:
            with suppress(OSError):
                os.close(source_fd)


def _assert_source_identity(
    parent_fd: int,
    name: str,
    descriptor: int,
    before: os.stat_result,
    relative: str,
) -> None:
    try:
        active = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except OSError as exc:
        raise RuntimeQuarantineError(
            f"candidate entry {relative} identity changed while copying"
        ) from exc
    if _identity(os.fstat(descriptor)) != _identity(before) or _identity(active) != _identity(
        before
    ):
        raise RuntimeQuarantineError(f"candidate entry {relative} identity changed while copying")


def _assert_named_identity(parent_fd: int, name: str, descriptor: int, label: str) -> None:
    active = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    if _identity(active) != _identity(os.fstat(descriptor)):
        raise RuntimeQuarantineError(f"{label} identity changed")


def _write_all(descriptor: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise RuntimeQuarantineError("quarantine write made no progress")
        view = view[written:]


def _manifest_roles(profile: RuntimeClosureProfile) -> dict[str, dict[str, object]]:
    return {
        name: {
            "python_path": "venv/bin/python",
            "module": role.module,
            "working_directory": "release",
            "app_source": "release/src",
            "site_packages": ["venv/lib/python3.11/site-packages"],
        }
        for name, role in profile.roles.items()
    }


def _canonical_manifest(
    profile: RuntimeClosureProfile,
    roles: Mapping[str, Mapping[str, object]],
    entries: tuple[_TreeEntry, ...],
) -> bytes:
    _require_role_paths(entries, roles)
    output = bytearray(b'{"entries":[')
    for index, entry in enumerate(entries):
        if index:
            _append_manifest_bytes(output, b",")
        _append_manifest_bytes(output, canonical_json_bytes(entry.payload()))
    _append_manifest_bytes(
        output,
        b'],"profile_id":' + canonical_json_bytes(profile.profile_id),
    )
    _append_manifest_bytes(
        output,
        b',"roles":' + canonical_json_bytes({name: dict(role) for name, role in roles.items()}),
    )
    _append_manifest_bytes(
        output,
        b',"schema_id":' + canonical_json_bytes(profile.manifest_schema.schema_id) + b"}\n",
    )
    return bytes(output)


def _append_manifest_bytes(output: bytearray, payload: bytes) -> None:
    if len(output) + len(payload) > MAX_GENERATION_MANIFEST_BYTES:
        raise RuntimeQuarantineError("root-derived generation manifest is too large")
    output.extend(payload)


def _require_role_paths(
    entries: tuple[_TreeEntry, ...],
    roles: Mapping[str, Mapping[str, object]],
) -> None:
    kinds = {entry.path: entry.entry_type for entry in entries}
    if kinds.get("pyvenv.cfg") != "file":
        raise RuntimeQuarantineError("candidate is missing pyvenv.cfg")
    for role in roles.values():
        site_packages = role["site_packages"]
        if type(site_packages) is not list:
            raise RuntimeQuarantineError("fixed runtime role paths are invalid")
        required = {
            role["python_path"]: "file",
            role["working_directory"]: "directory",
            role["app_source"]: "directory",
            **{path: "directory" for path in site_packages},
        }
        if any(type(path) is not str or kinds.get(path) != kind for path, kind in required.items()):
            raise RuntimeQuarantineError("candidate does not cover every fixed runtime role path")


def _write_manifest(directory_fd: int, payload: bytes) -> None:
    descriptor = -1
    try:
        descriptor = os.open(
            authority.GENERATION_MANIFEST_NAME,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | _required_flag("O_NOFOLLOW")
            | getattr(os, "O_CLOEXEC", 0),
            TEMP_FILE_MODE,
            dir_fd=directory_fd,
        )
        _set_owned_mode(descriptor, TEMP_FILE_MODE)
        _FAILPOINT("manifest_write")
        _write_all(descriptor, payload)
        _set_owned_mode(descriptor, authority.GENERATION_MANIFEST_MODE)
        _FAILPOINT("manifest_fsync")
        _fsync_descriptor(descriptor, "file")
        observed = os.fstat(descriptor)
        _require_file_stat(
            observed,
            owner_uid=authority.RUNTIME_AUTHORITY_OWNER_UID,
            modes={authority.GENERATION_MANIFEST_MODE},
            label="root-derived generation manifest",
        )
        if observed.st_size != len(payload):
            raise RuntimeQuarantineError("root-derived generation manifest size changed")
    finally:
        if descriptor >= 0:
            with suppress(OSError):
                os.close(descriptor)


def _revalidate_quarantine(
    name: str,
    profile: RuntimeClosureProfile,
    *,
    transition_hook: str | None = None,
) -> bytes:
    descriptors: list[int] = []
    tree_fd = -1
    try:
        descriptors, quarantine_fd = _open_fixed_root(
            profile.quarantine_root,
            "quarantine root",
        )
        tree_fd = _open_owned_directory_at(
            quarantine_fd,
            name,
            allowed_modes={authority.GENERATION_DIRECTORY_MODE},
            label="closed operation quarantine",
        )
        if transition_hook is not None:
            _FAILPOINT(transition_hook)
        return _verify_closed_tree(tree_fd, profile, label="closed operation quarantine")
    finally:
        if tree_fd >= 0:
            with suppress(OSError):
                os.close(tree_fd)
        _close_descriptors(descriptors)


def _verify_closed_tree(
    tree_fd: int,
    profile: RuntimeClosureProfile,
    *,
    label: str,
) -> bytes:
    root_before = os.fstat(tree_fd)
    _require_directory_stat(
        root_before,
        owner_uid=authority.RUNTIME_AUTHORITY_OWNER_UID,
        modes={authority.GENERATION_DIRECTORY_MODE},
        label=label,
    )
    manifest = _read_file_at(
        tree_fd,
        authority.GENERATION_MANIFEST_NAME,
        max_bytes=MAX_GENERATION_MANIFEST_BYTES,
        modes={authority.GENERATION_MANIFEST_MODE},
        label=f"{label} manifest",
    )
    try:
        decoded = strict_json_loads(manifest)
    except (StrictJsonError, UnicodeDecodeError, ValueError, RecursionError) as exc:
        raise RuntimeQuarantineError(f"{label} manifest is invalid") from exc
    if type(decoded) is not dict or set(decoded) != _MANIFEST_FIELDS:
        raise RuntimeQuarantineError(f"{label} manifest schema is invalid")
    roles = _manifest_roles(profile)
    if (
        decoded["schema_id"] != profile.manifest_schema.schema_id
        or decoded["profile_id"] != profile.profile_id
        or decoded["roles"] != roles
        or type(decoded["entries"]) is not list
    ):
        raise RuntimeQuarantineError(f"{label} manifest does not match the loaded profile")
    entries = _scan_owned_tree(tree_fd, profile)
    expected = _canonical_manifest(profile, roles, entries)
    if expected != manifest:
        raise RuntimeQuarantineError(f"{label} tree differs from its root-derived manifest")
    root_after = os.fstat(tree_fd)
    if _identity(root_before) != _identity(root_after):
        raise RuntimeQuarantineError(f"{label} root identity changed while revalidating")
    return manifest


def _scan_owned_tree(
    tree_fd: int,
    profile: RuntimeClosureProfile,
) -> tuple[_TreeEntry, ...]:
    budget = _TreeBudget(profile.manifest_schema)
    normalized_paths: set[str] = set()
    observed_inodes: set[tuple[int, int]] = set()
    entries: list[_TreeEntry] = []
    _scan_owned_directory(
        tree_fd,
        relative_parent="",
        normalized_parent="",
        profile=profile,
        budget=budget,
        normalized_paths=normalized_paths,
        observed_inodes=observed_inodes,
        entries=entries,
    )
    return tuple(sorted(entries, key=lambda entry: entry.path))


def _scan_owned_directory(
    directory_fd: int,
    *,
    relative_parent: str,
    normalized_parent: str,
    profile: RuntimeClosureProfile,
    budget: _TreeBudget,
    normalized_paths: set[str],
    observed_inodes: set[tuple[int, int]],
    entries: list[_TreeEntry],
) -> None:
    with os.scandir(directory_fd) as iterator:
        for directory_entry in iterator:
            name = directory_entry.name
            if not relative_parent and name == authority.GENERATION_MANIFEST_NAME:
                continue
            _require_leaf_name(name)
            normalized_name = _normalize_tree_component(name)
            relative = name if not relative_parent else f"{relative_parent}/{name}"
            normalized = (
                normalized_name
                if not normalized_parent
                else f"{normalized_parent}/{normalized_name}"
            )
            if not relative_parent and normalized_name == _NORMALIZED_MANIFEST_NAME:
                raise RuntimeQuarantineError(
                    "root-owned tree contains a reserved normalized manifest alias"
                )
            if normalized in normalized_paths:
                raise RuntimeQuarantineError(
                    "root-owned tree contains a duplicate normalized relative path"
                )
            normalized_paths.add(normalized)
            budget.observe_path(relative, normalized)
            named = directory_entry.stat(follow_symlinks=False)
            inode = _inode_identity(named)
            if inode in observed_inodes:
                raise RuntimeQuarantineError("root-owned tree contains a dev/inode alias")
            observed_inodes.add(inode)
            if stat.S_ISDIR(named.st_mode):
                entry = _scan_owned_subdirectory(
                    directory_fd,
                    name,
                    relative,
                    normalized,
                    named,
                    profile=profile,
                    budget=budget,
                    normalized_paths=normalized_paths,
                    observed_inodes=observed_inodes,
                    entries=entries,
                )
            elif stat.S_ISREG(named.st_mode):
                budget.observe_file(named.st_size)
                entry = _hash_owned_file(
                    directory_fd,
                    name,
                    relative,
                    named,
                    modes=set(profile.manifest_schema.file_modes),
                )
            else:
                raise RuntimeQuarantineError("root-owned tree contains a symlink or special entry")
            budget.observe_manifest_entry(entry)
            entries.append(entry)


def _scan_owned_subdirectory(
    parent_fd: int,
    name: str,
    relative: str,
    normalized_relative: str,
    named: os.stat_result,
    *,
    profile: RuntimeClosureProfile,
    budget: _TreeBudget,
    normalized_paths: set[str],
    observed_inodes: set[tuple[int, int]],
    entries: list[_TreeEntry],
) -> _TreeEntry:
    _require_directory_stat(
        named,
        owner_uid=authority.RUNTIME_AUTHORITY_OWNER_UID,
        modes=set(profile.manifest_schema.directory_modes),
        label=f"root-owned directory {relative}",
    )
    descriptor = -1
    try:
        descriptor = os.open(
            name,
            os.O_RDONLY
            | _required_flag("O_DIRECTORY")
            | _required_flag("O_NOFOLLOW")
            | getattr(os, "O_CLOEXEC", 0),
            dir_fd=parent_fd,
        )
        opened = os.fstat(descriptor)
        if _identity(opened) != _identity(named):
            raise RuntimeQuarantineError(
                f"root-owned directory {relative} identity changed while opening"
            )
        _scan_owned_directory(
            descriptor,
            relative_parent=relative,
            normalized_parent=normalized_relative,
            profile=profile,
            budget=budget,
            normalized_paths=normalized_paths,
            observed_inodes=observed_inodes,
            entries=entries,
        )
        _assert_source_identity(parent_fd, name, descriptor, named, relative)
        return _TreeEntry(
            path=relative,
            entry_type="directory",
            owner_uid=named.st_uid,
            mode=stat.S_IMODE(named.st_mode),
            nlink=named.st_nlink,
            size=0,
            sha256=None,
            identity=_identity(named),
        )
    finally:
        if descriptor >= 0:
            with suppress(OSError):
                os.close(descriptor)


def _hash_owned_file(
    parent_fd: int,
    name: str,
    relative: str,
    named: os.stat_result,
    *,
    modes: set[int],
) -> _TreeEntry:
    _require_file_stat(
        named,
        owner_uid=authority.RUNTIME_AUTHORITY_OWNER_UID,
        modes=modes,
        label=f"root-owned file {relative}",
    )
    descriptor = -1
    try:
        descriptor = os.open(
            name,
            os.O_RDONLY | _required_flag("O_NOFOLLOW") | getattr(os, "O_CLOEXEC", 0),
            dir_fd=parent_fd,
        )
        if _identity(os.fstat(descriptor)) != _identity(named):
            raise RuntimeQuarantineError(
                f"root-owned file {relative} identity changed while opening"
            )
        digest = hashlib.sha256()
        total = 0
        while True:
            chunk = os.read(descriptor, COPY_CHUNK_BYTES)
            if not chunk:
                break
            total += len(chunk)
            if total > named.st_size:
                raise RuntimeQuarantineError(f"root-owned file {relative} grew while hashing")
            digest.update(chunk)
        if total != named.st_size:
            raise RuntimeQuarantineError(f"root-owned file {relative} size changed while hashing")
        _assert_source_identity(parent_fd, name, descriptor, named, relative)
        return _TreeEntry(
            path=relative,
            entry_type="file",
            owner_uid=named.st_uid,
            mode=stat.S_IMODE(named.st_mode),
            nlink=named.st_nlink,
            size=total,
            sha256=digest.hexdigest(),
            identity=_identity(named),
        )
    finally:
        if descriptor >= 0:
            with suppress(OSError):
                os.close(descriptor)


def _read_file_at(
    parent_fd: int,
    name: str,
    *,
    max_bytes: int,
    modes: set[int],
    label: str,
) -> bytes:
    named = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    _require_file_stat(
        named,
        owner_uid=authority.RUNTIME_AUTHORITY_OWNER_UID,
        modes=modes,
        label=label,
    )
    if named.st_size > max_bytes:
        raise RuntimeQuarantineError(f"{label} is too large")
    descriptor = -1
    chunks: list[bytes] = []
    try:
        descriptor = os.open(
            name,
            os.O_RDONLY | _required_flag("O_NOFOLLOW") | getattr(os, "O_CLOEXEC", 0),
            dir_fd=parent_fd,
        )
        if _identity(os.fstat(descriptor)) != _identity(named):
            raise RuntimeQuarantineError(f"{label} identity changed while opening")
        total = 0
        while True:
            chunk = os.read(descriptor, min(COPY_CHUNK_BYTES, max_bytes + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > max_bytes:
                raise RuntimeQuarantineError(f"{label} is too large")
        _assert_source_identity(parent_fd, name, descriptor, named, label)
        return b"".join(chunks)
    finally:
        if descriptor >= 0:
            with suppress(OSError):
                os.close(descriptor)


def _publish_verified_quarantine(
    temporary_name: str,
    generation_id: str,
    manifest: bytes,
    profile: RuntimeClosureProfile,
    deployment_lock: authority.RuntimeDeploymentLock,
    publication_state: _PublicationState,
) -> RuntimeQuarantineStatus:
    quarantine_descriptors: list[int] = []
    generation_descriptors: list[int] = []
    final_quarantine_fd = -1
    try:
        quarantine_descriptors, quarantine_fd = _open_fixed_root(
            profile.quarantine_root,
            "quarantine root",
        )
        generation_descriptors, generation_fd = _open_fixed_root(
            profile.generation_root,
            "generation root",
        )
        deployment_lock.assert_current()
        if _named_entry(generation_fd, generation_id) is not None:
            _verify_existing_generation(
                generation_fd,
                generation_id,
                manifest,
                profile,
            )
            _remove_temporary_tree(quarantine_fd, temporary_name)
            _fsync_descriptor(quarantine_fd, "quarantine_parent")
            _fsync_generation_parent_after_verify(
                generation_fd,
                generation_id,
                manifest,
                profile,
            )
            return RuntimeQuarantineStatus.IDEMPOTENT
        final_quarantine_fd = _open_owned_directory_at(
            quarantine_fd,
            temporary_name,
            allowed_modes={authority.GENERATION_DIRECTORY_MODE},
            label="final operation quarantine",
        )
        final_manifest = _verify_closed_tree(
            final_quarantine_fd,
            profile,
            label="final operation quarantine",
        )
        if final_manifest != manifest:
            raise RuntimeQuarantineError("quarantine changed before generation rename")
        _assert_named_identity(
            quarantine_fd,
            temporary_name,
            final_quarantine_fd,
            "final operation quarantine",
        )
        deployment_lock.assert_current()
        _FAILPOINT("final_identity_to_rename")
        _atomic_rename_noreplace(
            quarantine_fd,
            temporary_name,
            generation_fd,
            generation_id,
        )
        publication_state.renamed = True
        _FAILPOINT("post_rename_pre_parent_fsync")
        published = os.stat(generation_id, dir_fd=generation_fd, follow_symlinks=False)
        if _identity(published) != _identity(os.fstat(final_quarantine_fd)):
            raise RuntimeQuarantineDurabilityError(
                "published generation identity changed during rename"
            )
        try:
            _FAILPOINT("generation_parent_fsync_first")
            _fsync_descriptor(generation_fd, "store_parent")
            _FAILPOINT("generation_durable_before_authority")
            return RuntimeQuarantineStatus.PUBLISHED
        except OSError:
            try:
                _verify_existing_generation(
                    generation_fd,
                    generation_id,
                    manifest,
                    profile,
                )
                _FAILPOINT("generation_parent_fsync_recovery")
                _fsync_descriptor(generation_fd, "store_parent")
            except (OSError, RuntimeQuarantineError) as recovery_exc:
                raise RuntimeQuarantineDurabilityError(
                    "published generation parent fsync remains blocked"
                ) from recovery_exc
            _FAILPOINT("generation_durable_before_authority")
            return RuntimeQuarantineStatus.PUBLISHED_AFTER_RECOVERY
    finally:
        if final_quarantine_fd >= 0:
            with suppress(OSError):
                os.close(final_quarantine_fd)
        _close_descriptors(quarantine_descriptors)
        _close_descriptors(generation_descriptors)


def _atomic_rename_noreplace(
    source_parent_fd: int,
    source_name: str,
    target_parent_fd: int,
    target_name: str,
) -> None:
    primitive = _ATOMIC_RENAME_NOREPLACE
    if primitive is None:
        raise RuntimeQuarantineError("platform lacks an atomic no-replace rename primitive")
    _FAILPOINT("atomic_rename")
    try:
        primitive(source_parent_fd, source_name, target_parent_fd, target_name)
    except OSError as exc:
        if exc.errno in {errno.EEXIST, errno.ENOTEMPTY}:
            raise RuntimeQuarantineError(
                "generation appeared before atomic no-replace publication"
            ) from exc
        raise


def _fsync_generation_parent_after_verify(
    generation_fd: int,
    generation_id: str,
    manifest: bytes,
    profile: RuntimeClosureProfile,
) -> None:
    try:
        _fsync_descriptor(generation_fd, "store_parent")
    except OSError:
        _verify_existing_generation(generation_fd, generation_id, manifest, profile)
        try:
            _fsync_descriptor(generation_fd, "store_parent")
        except OSError as recovery_exc:
            raise RuntimeQuarantineDurabilityError(
                "existing generation parent fsync remains blocked"
            ) from recovery_exc


def _verify_existing_generation(
    generation_parent_fd: int,
    generation_id: str,
    expected_manifest: bytes,
    profile: RuntimeClosureProfile,
) -> None:
    descriptor = -1
    try:
        descriptor = _open_owned_directory_at(
            generation_parent_fd,
            generation_id,
            allowed_modes={authority.GENERATION_DIRECTORY_MODE},
            label="existing generation",
        )
        observed = _verify_closed_tree(descriptor, profile, label="existing generation")
        if observed != expected_manifest or hashlib.sha256(observed).hexdigest() != generation_id:
            raise RuntimeQuarantineError("existing generation conflicts with quarantine")
    except RuntimeQuarantineError as exc:
        raise RuntimeQuarantineError("existing generation conflicts with quarantine") from exc
    finally:
        if descriptor >= 0:
            with suppress(OSError):
                os.close(descriptor)


def _named_entry(parent_fd: int, name: str) -> os.stat_result | None:
    try:
        return os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None


def _result(
    request: RuntimeQuarantineRequest,
    profile: RuntimeClosureProfile,
    generation_id: str,
    status: RuntimeQuarantineStatus,
) -> RuntimeQuarantineResult:
    generation = profile.generation_root / generation_id
    roles = {
        name: RuntimeRoleSpec(
            python_path=generation / "venv/bin/python",
            module=role.module,
            working_directory=generation / "release",
            app_source=generation / "release/src",
            site_packages=(generation / "venv/lib/python3.11/site-packages",),
        )
        for name, role in profile.roles.items()
    }
    slot = RuntimeGenerationSlot(
        lifecycle=RuntimeGenerationLifecycle.ACTIVE,
        generation_id=generation_id,
        generation_path=generation,
        commit=request.untrusted_commit,
        full_manifest_hash=generation_id,
        profile_id=profile.profile_id,
        roles=roles,
    )
    slot.validate_for_root(profile.generation_root)
    return RuntimeQuarantineResult(
        status=status,
        slot=slot,
        untrusted_candidate_id=request.candidate_id,
        untrusted_manifest_hash=request.untrusted_manifest_hash,
        untrusted_manifest_matches=request.untrusted_manifest_hash == generation_id,
    )


def _remove_temporary_tree(parent_fd: int, name: str) -> None:
    if re.fullmatch(r"\.quarantine-[0-9a-f]{32}", name) is None:
        raise RuntimeQuarantineError("operation quarantine cleanup name is invalid")
    named = _prepare_operation_residue_for_cleanup(parent_fd, name)
    _require_directory_stat(
        named,
        owner_uid=authority.RUNTIME_AUTHORITY_OWNER_UID,
        modes={TEMP_DIRECTORY_MODE, authority.GENERATION_DIRECTORY_MODE},
        label="operation quarantine cleanup root",
    )
    descriptor = -1
    try:
        descriptor = os.open(
            name,
            os.O_RDONLY
            | _required_flag("O_DIRECTORY")
            | _required_flag("O_NOFOLLOW")
            | getattr(os, "O_CLOEXEC", 0),
            dir_fd=parent_fd,
        )
        if _inode_identity(os.fstat(descriptor)) != _inode_identity(named):
            raise RuntimeQuarantineError("operation quarantine identity changed during cleanup")
        _remove_directory_contents(descriptor)
        active = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if _inode_identity(active) != _inode_identity(named):
            raise RuntimeQuarantineError("operation quarantine identity changed during cleanup")
    finally:
        if descriptor >= 0:
            with suppress(OSError):
                os.close(descriptor)
    os.rmdir(name, dir_fd=parent_fd)


def _prepare_operation_residue_for_cleanup(
    parent_fd: int,
    name: str,
) -> os.stat_result:
    named = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    mode = stat.S_IMODE(named.st_mode)
    if (
        not stat.S_ISDIR(named.st_mode)
        or stat.S_ISLNK(named.st_mode)
        or named.st_uid != authority.RUNTIME_AUTHORITY_OWNER_UID
        or named.st_nlink < 2
    ):
        raise RuntimeQuarantineError(
            "operation quarantine cleanup root directory metadata is unsafe"
        )
    allowed = {
        TEMP_DIRECTORY_MODE,
        authority.GENERATION_DIRECTORY_MODE,
        *authority.PRODUCTION_MANIFEST_SCHEMA["directory_modes"],
    }
    if mode in allowed:
        return named
    if mode & ~TEMP_DIRECTORY_MODE:
        raise RuntimeQuarantineError(
            "operation quarantine cleanup root directory metadata is unsafe"
        )
    os.chmod(
        name,
        TEMP_DIRECTORY_MODE,
        dir_fd=parent_fd,
        follow_symlinks=False,
    )
    repaired = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    _require_directory_stat(
        repaired,
        owner_uid=authority.RUNTIME_AUTHORITY_OWNER_UID,
        modes={TEMP_DIRECTORY_MODE},
        label="operation quarantine cleanup root",
    )
    return repaired


def _remove_directory_contents(directory_fd: int) -> None:
    observed = os.fstat(directory_fd)
    _require_directory_stat(
        observed,
        owner_uid=authority.RUNTIME_AUTHORITY_OWNER_UID,
        modes={
            TEMP_DIRECTORY_MODE,
            authority.GENERATION_DIRECTORY_MODE,
            *authority.PRODUCTION_MANIFEST_SCHEMA["directory_modes"],
        },
        label="operation quarantine cleanup directory",
    )
    os.fchmod(directory_fd, TEMP_DIRECTORY_MODE)
    with os.scandir(directory_fd) as iterator:
        for entry in iterator:
            name = entry.name
            _require_leaf_name(name)
            named = entry.stat(follow_symlinks=False)
            if stat.S_ISDIR(named.st_mode):
                named = _prepare_cleanup_directory_entry(directory_fd, name, named)
                child_fd = -1
                try:
                    child_fd = os.open(
                        name,
                        os.O_RDONLY
                        | _required_flag("O_DIRECTORY")
                        | _required_flag("O_NOFOLLOW")
                        | getattr(os, "O_CLOEXEC", 0),
                        dir_fd=directory_fd,
                    )
                    if _inode_identity(os.fstat(child_fd)) != _inode_identity(named):
                        raise RuntimeQuarantineError(
                            "operation quarantine identity changed during cleanup"
                        )
                    _remove_directory_contents(child_fd)
                finally:
                    if child_fd >= 0:
                        with suppress(OSError):
                            os.close(child_fd)
                active = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
                if _inode_identity(active) != _inode_identity(named):
                    raise RuntimeQuarantineError(
                        "operation quarantine identity changed during cleanup"
                    )
                os.rmdir(name, dir_fd=directory_fd)
            elif stat.S_ISREG(named.st_mode):
                named = _prepare_cleanup_file_entry(directory_fd, name, named)
                _require_file_stat(
                    named,
                    owner_uid=authority.RUNTIME_AUTHORITY_OWNER_UID,
                    modes={
                        TEMP_FILE_MODE,
                        *authority.PRODUCTION_MANIFEST_SCHEMA["file_modes"],
                    },
                    label="operation quarantine cleanup file",
                )
                active = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
                if _identity(active) != _identity(named):
                    raise RuntimeQuarantineError(
                        "operation quarantine identity changed during cleanup"
                    )
                os.unlink(name, dir_fd=directory_fd)
            else:
                raise RuntimeQuarantineError(
                    "operation quarantine cleanup found a symlink or special entry"
                )
    _fsync_descriptor(directory_fd, "cleanup_directory")


def _prepare_cleanup_directory_entry(
    parent_fd: int,
    name: str,
    named: os.stat_result,
) -> os.stat_result:
    mode = stat.S_IMODE(named.st_mode)
    allowed = {
        TEMP_DIRECTORY_MODE,
        authority.GENERATION_DIRECTORY_MODE,
        *authority.PRODUCTION_MANIFEST_SCHEMA["directory_modes"],
    }
    if mode in allowed:
        return named
    if (
        not stat.S_ISDIR(named.st_mode)
        or stat.S_ISLNK(named.st_mode)
        or named.st_uid != authority.RUNTIME_AUTHORITY_OWNER_UID
        or named.st_nlink < 2
        or mode & ~TEMP_DIRECTORY_MODE
    ):
        raise RuntimeQuarantineError("operation quarantine cleanup directory metadata is unsafe")
    os.chmod(
        name,
        TEMP_DIRECTORY_MODE,
        dir_fd=parent_fd,
        follow_symlinks=False,
    )
    repaired = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    if _inode_identity(repaired) != _inode_identity(named):
        raise RuntimeQuarantineError("operation quarantine cleanup directory identity changed")
    return repaired


def _prepare_cleanup_file_entry(
    parent_fd: int,
    name: str,
    named: os.stat_result,
) -> os.stat_result:
    mode = stat.S_IMODE(named.st_mode)
    allowed = {
        TEMP_FILE_MODE,
        *authority.PRODUCTION_MANIFEST_SCHEMA["file_modes"],
    }
    if mode in allowed:
        return named
    if (
        not stat.S_ISREG(named.st_mode)
        or stat.S_ISLNK(named.st_mode)
        or named.st_uid != authority.RUNTIME_AUTHORITY_OWNER_UID
        or named.st_nlink != 1
        or mode & ~TEMP_FILE_MODE
    ):
        raise RuntimeQuarantineError("operation quarantine cleanup file metadata is unsafe")
    os.chmod(
        name,
        TEMP_FILE_MODE,
        dir_fd=parent_fd,
        follow_symlinks=False,
    )
    repaired = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    if _inode_identity(repaired) != _inode_identity(named):
        raise RuntimeQuarantineError("operation quarantine cleanup file identity changed")
    return repaired


def _require_leaf_name(name: str) -> None:
    if not name or name in {".", ".."} or "/" in name or "\x00" in name:
        raise RuntimeQuarantineError("filesystem entry name is invalid")
    _utf8_size(name, "filesystem entry name")


_NORMALIZED_MANIFEST_NAME = unicodedata.normalize(
    "NFKC",
    authority.GENERATION_MANIFEST_NAME,
).casefold()


def _normalize_tree_component(name: str) -> str:
    try:
        normalized = unicodedata.normalize("NFKC", name).casefold()
    except (TypeError, ValueError) as exc:
        raise RuntimeQuarantineError("filesystem component normalization failed") from exc
    if (
        not normalized
        or normalized in {".", ".."}
        or "/" in normalized
        or "\\" in normalized
        or "\x00" in normalized
    ):
        raise RuntimeQuarantineError("normalized filesystem component is invalid")
    _utf8_size(normalized, "normalized filesystem component")
    return normalized


def _fsync_descriptor(descriptor: int, phase: str) -> None:
    global _FSYNC_PHASE
    previous = _FSYNC_PHASE
    _FSYNC_PHASE = phase
    try:
        os.fsync(descriptor)
    finally:
        _FSYNC_PHASE = previous
