"""Root-side publication of a staged runtime authority generation, and the shapes it shares.

Two programs consume this module. `rquant runtime-authority-stage` (`runtime_authority_stage`)
runs as `lighthouse` and lays a complete staging directory out of a checkout; the root-owned
`rquant-production-deploy.pyz` runs `publish` here, copies that staging into the inbox,
recomputes every hash against the operator-confirmed `plan.json`, installs the profile,
renames the generation into place and calls the existing `publish_runtime_authority`
transaction. Both sides derive the documents — full manifest, profile, candidate record —
from the functions below, so the root side can require byte equality with what the
unprivileged side staged rather than trusting it (S1 §1.3).

This module is packaged into the deploy pyz, so it imports the standard library,
`rquant.strict_json`, `rquant.runtime_authority` and the wrapper's `_verify` only.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import stat
import sys
import unicodedata
from collections.abc import Iterable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from rquant import runtime_authority as authority
from rquant.runtime_authority import (
    PRODUCTION_ROLE_POLICY,
    RuntimeAncestorPolicy,
    RuntimeAuthorityError,
    RuntimeAuthorityPublishError,
    RuntimeAuthorityRecord,
    RuntimeAuthorityRecordError,
    RuntimeAuthorityState,
    RuntimeClosureProfile,
    RuntimeFilePolicy,
    RuntimeGenerationLifecycle,
    RuntimeGenerationSlot,
    RuntimeRoleSpec,
    canonical_runtime_authority_bytes,
    parse_runtime_closure_profile,
)
from rquant.runtime_exec_wrapper import _verify
from rquant.strict_json import StrictJsonError, canonical_json_bytes, strict_json_loads

PLAN_SCHEMA_ID = "rquant-authority-plan/v1"
PLAN_SCHEMA_VERSION = 1
PLAN_NAME = "plan.json"
PROFILE_NAME = "production-runtime-profile.json"
RECORD_NAME = "current.json"
GENERATION_NAME = "generation"
PUBLICATIONS_NAME = "publications.jsonl"
MAX_PLAN_BYTES = 64 * 1024 * 1024

#: The generation layout `acceptance-pra.md` E-1 fixes: `app_source` is `<gen>/src` (a
#: mirror of the checkout, so the two import-time file hops in `lab_daemon.py:53` and
#: `scripts/strict_json.py:8` both land inside the generation), the interpreter is a
#: physical copy under `bin/`, and the frozen venv goes to `lib/site-packages`.
GENERATION_PYTHON = "bin/python"
GENERATION_CWD = "cwd"
GENERATION_APP_SOURCE = "src"
GENERATION_SITE_PACKAGES = "lib/site-packages"
GENERATION_SCRIPTS = "scripts"
GENERATION_MANIFESTS = "manifests"
GENERATION_PYVENV = "pyvenv.cfg"
DIRECTORY_MODE = 0o555
FILE_MODE = 0o444
EXECUTABLE_MODE = 0o555
INBOX_DIRECTORY_MODE = 0o700
PUBLICATIONS_MODE = 0o600

#: Where the root transaction expects the already-installed runtime wrapper. The profile's
#: `runtime_pyz.path` is the fixed production literal the wrapper checks itself against, so
#: it cannot be redirected; this constant is the publisher's own view of the same file and
#: is the seam the offline suite points at a temporary artifact (S1 §1.5: constants only).
INSTALLED_RUNTIME_PYZ: Path = authority.PRODUCTION_RUNTIME_PYZ
#: Group every root-owned file is given. The verifiers check the owner UID only; `root:root`
#: is the documented shape (S1 §1.2A).
PUBLISH_OWNER_GID = 0
#: The wrapper's trusted root for the pre-publication preflight (`_verify.TRUSTED_ROOT`).
WRAPPER_TRUSTED_ROOT = "/"

_SHA256_HEX = frozenset("0123456789abcdef")
_STAGED_FILE_FIELDS = frozenset({"type", "mode", "size", "sha256"})
_PLAN_FIELDS = frozenset(
    {
        "schema_id",
        "schema_version",
        "operation_id",
        "mode",
        "producer_commit",
        "profile_id",
        "generation_id",
        "sequence",
        "previous_operation_id",
        "runtime_pyz_sha256",
        "deploy_pyz_sha256",
        "system_python_sha256",
        "instance_mapping",
        "service_manifests",
        "closure_summary",
        "staged_files",
    }
)
_PLAN_MODES = ("bootstrap", "legacy")
_HASH_CHUNK = 1024 * 1024
#: Read side of every path the unprivileged staging tree can name (N-6). `O_NOFOLLOW` keeps a
#: swapped final segment from being followed; `O_NONBLOCK` is what keeps a FIFO left in place
#: of a planned file from blocking `open()` forever while `publish` already holds
#: `deployment.lock`. It is a no-op for a regular file, so the `S_ISREG` check on the
#: descriptor still decides, one syscall later, what this process is allowed to read.
_READ_FLAGS = (
    os.O_RDONLY
    | getattr(os, "O_NOFOLLOW", 0)
    | getattr(os, "O_CLOEXEC", 0)
    | getattr(os, "O_NONBLOCK", 0)
)

DirectoryLinkConvention = Literal["subdirectories", "entries"]


class RuntimeAuthorityStageError(RuntimeAuthorityError):
    """The unprivileged staging step refused; nothing was written."""


# ---------------------------------------------------------------------------------------
# Hashing and tree shapes
# ---------------------------------------------------------------------------------------


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path) -> tuple[str, int]:
    """Digest and size of one regular file, opened without following a final symlink."""

    descriptor = os.open(path, _READ_FLAGS)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise RuntimeAuthorityStageError(f"not a regular file: {path}")
        digest = hashlib.sha256()
        size = 0
        while chunk := os.read(descriptor, _HASH_CHUNK):
            digest.update(chunk)
            size += len(chunk)
        return digest.hexdigest(), size
    finally:
        os.close(descriptor)


def require_sha256(value: object, label: str) -> str:
    if type(value) is not str or len(value) != 64 or any(c not in _SHA256_HEX for c in value):
        raise RuntimeAuthorityStageError(f"{label} is not a lowercase sha256 hex digest")
    return value


def validate_relative_path(path: str, label: str) -> tuple[str, ...]:
    """A generation-relative path: non-empty POSIX components, no `.`/`..`, no separators."""

    if type(path) is not str or not path or path.startswith("/") or path.endswith("/"):
        raise RuntimeAuthorityStageError(f"{label} is not a relative path: {path!r}")
    parts = tuple(path.split("/"))
    for part in parts:
        if not part or part in {".", ".."} or "\\" in part or "\x00" in part:
            raise RuntimeAuthorityStageError(f"{label} has an invalid component: {path!r}")
    return parts


def normalized_path(path: str) -> str:
    return "/".join(unicodedata.normalize("NFKC", part).casefold() for part in path.split("/"))


@dataclass(frozen=True)
class StagedFile:
    """One file of the generation: bytes come from `source` on disk or inline `payload`."""

    mode: int
    source: Path | None = None
    payload: bytes | None = None

    def __post_init__(self) -> None:
        if (self.source is None) == (self.payload is None):
            raise RuntimeAuthorityStageError("a staged file names a source path or a payload")
        if self.mode not in (FILE_MODE, EXECUTABLE_MODE):
            raise RuntimeAuthorityStageError("a staged file mode must be 0444 or 0555")

    def digest(self) -> tuple[str, int]:
        if self.payload is not None:
            return sha256_bytes(self.payload), len(self.payload)
        assert self.source is not None
        return sha256_file(self.source)


@dataclass(frozen=True)
class GenerationLayout:
    """Every file of a generation and every directory, including the empty ones."""

    files: Mapping[str, StagedFile]
    directories: tuple[str, ...]

    @classmethod
    def build(
        cls,
        files: Mapping[str, StagedFile],
        *,
        empty_directories: Iterable[str] = (),
    ) -> GenerationLayout:
        directories: set[str] = set()
        for relative in (*files, *empty_directories):
            validate_relative_path(relative, "generation path")
        for relative in empty_directories:
            if relative in files:
                raise RuntimeAuthorityStageError(f"{relative} is both a file and a directory")
            directories.add(relative)
        for relative in (*files, *empty_directories):
            parent = Path(relative).parent.as_posix()
            while parent != ".":
                if parent in files:
                    raise RuntimeAuthorityStageError(f"{parent} is both a file and a directory")
                directories.add(parent)
                parent = Path(parent).parent.as_posix()
        normalized: dict[str, str] = {}
        for relative in (*files, *directories):
            key = normalized_path(relative)
            if key in normalized:
                raise RuntimeAuthorityStageError(
                    "two generation paths collide after NFKC case folding: "
                    f"{normalized[key]!r} and {relative!r}"
                )
            normalized[key] = relative
        if authority.GENERATION_MANIFEST_NAME in files:
            raise RuntimeAuthorityStageError("the full manifest is written by the publisher")
        return cls(files=dict(sorted(files.items())), directories=tuple(sorted(directories)))

    def children(self) -> dict[str, tuple[int, int]]:
        """Per directory: (subdirectory count, total entry count)."""

        counts = {relative: [0, 0] for relative in self.directories}
        for relative in self.directories:
            parent = Path(relative).parent.as_posix()
            if parent != ".":
                counts[parent][0] += 1
                counts[parent][1] += 1
        for relative in self.files:
            parent = Path(relative).parent.as_posix()
            if parent != ".":
                counts[parent][1] += 1
        return {key: (value[0], value[1]) for key, value in counts.items()}


def detect_directory_link_convention(path: Path) -> DirectoryLinkConvention:
    """How this filesystem counts a directory's links: POSIX `2 + subdirectories`, or APFS
    `2 + entries`. Decided from the nearest existing ancestor that holds at least one file,
    so a dry run can predict the manifest of a tree it never writes."""

    candidate = Path(os.path.abspath(path))
    while True:
        if candidate.is_dir() and not candidate.is_symlink():
            subdirectories = 0
            entries = 0
            with os.scandir(candidate) as iterator:
                for entry in iterator:
                    entries += 1
                    if entry.is_dir(follow_symlinks=False):
                        subdirectories += 1
            links = candidate.lstat().st_nlink
            if entries != subdirectories:
                if links == 2 + subdirectories:
                    return "subdirectories"
                if links == 2 + entries:
                    return "entries"
                raise RuntimeAuthorityStageError(
                    f"{candidate} reports {links} links for {subdirectories} subdirectories "
                    f"and {entries} entries; this filesystem cannot host a generation"
                )
        if candidate.parent == candidate:
            raise RuntimeAuthorityStageError(
                "no ancestor directory reveals the filesystem link-count convention"
            )
        candidate = candidate.parent


def predicted_directory_links(
    convention: DirectoryLinkConvention,
    subdirectories: int,
    entries: int,
) -> int:
    return 2 + (subdirectories if convention == "subdirectories" else entries)


def predict_manifest_entries(
    layout: GenerationLayout,
    *,
    owner_uid: int,
    convention: DirectoryLinkConvention,
) -> tuple[dict[str, object], ...]:
    """The full-manifest entries the materialised layout will have, without writing it."""

    children = layout.children()
    entries: list[dict[str, object]] = [
        {
            "path": relative,
            "type": "directory",
            "owner_uid": owner_uid,
            "mode": DIRECTORY_MODE,
            "nlink": predicted_directory_links(convention, *children[relative]),
            "size": 0,
            "sha256": None,
        }
        for relative in layout.directories
    ]
    for relative, staged in layout.files.items():
        digest, size = staged.digest()
        entries.append(
            {
                "path": relative,
                "type": "file",
                "owner_uid": owner_uid,
                "mode": staged.mode,
                "nlink": 1,
                "size": size,
                "sha256": digest,
            }
        )
    return tuple(sorted(entries, key=lambda entry: str(entry["path"])))


def materialize_layout(layout: GenerationLayout, target: Path) -> None:
    """Write the layout below `target` (which must already exist, writable), frozen."""

    for relative in layout.directories:
        (target / relative).mkdir(mode=0o700)
    for relative, staged in layout.files.items():
        destination = target / relative
        if staged.payload is not None:
            _write_new_file(destination, staged.payload)
        else:
            assert staged.source is not None
            _copy_new_file(staged.source, destination)
        destination.chmod(staged.mode)
    for relative in reversed(layout.directories):
        (target / relative).chmod(DIRECTORY_MODE)


def scan_frozen_tree(root: Path, *, owner_uid: int) -> tuple[dict[str, object], ...]:
    """Manifest entries of a materialised tree, from `lstat` and the bytes on disk."""

    entries: list[dict[str, object]] = []
    for directory, subdirectories, files in os.walk(root, followlinks=False):
        base = Path(directory)
        for name in (*subdirectories, *files):
            path = base / name
            info = path.lstat()
            relative = path.relative_to(root).as_posix()
            if stat.S_ISDIR(info.st_mode):
                entries.append(
                    {
                        "path": relative,
                        "type": "directory",
                        "owner_uid": owner_uid,
                        "mode": stat.S_IMODE(info.st_mode),
                        "nlink": info.st_nlink,
                        "size": 0,
                        "sha256": None,
                    }
                )
            elif stat.S_ISREG(info.st_mode):
                digest, size = sha256_file(path)
                entries.append(
                    {
                        "path": relative,
                        "type": "file",
                        "owner_uid": owner_uid,
                        "mode": stat.S_IMODE(info.st_mode),
                        "nlink": info.st_nlink,
                        "size": size,
                        "sha256": digest,
                    }
                )
            else:
                raise RuntimeAuthorityStageError(
                    f"generation tree holds a symlink or special entry: {relative}"
                )
    return tuple(sorted(entries, key=lambda entry: str(entry["path"])))


def _write_all(descriptor: int, payload: bytes) -> None:
    """The publisher's own loop: the record transaction's `_write_all` is a fault-injection
    seam (T-19) and must not be shared with the inbox copy."""

    offset = 0
    while offset < len(payload):
        written = os.write(descriptor, payload[offset:])
        if written <= 0:
            raise OSError("staged file write made no progress")
        offset += written


def _open_new(path: Path, mode: int = 0o600) -> int:
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    return os.open(path, flags, mode)


def _write_new_file(path: Path, payload: bytes) -> None:
    descriptor = _open_new(path)
    try:
        _write_all(descriptor, payload)
    finally:
        os.close(descriptor)


def _copy_new_file(source: Path, destination: Path) -> tuple[str, int]:
    """Copy `source` to a new `destination` and return the digest of the bytes written."""

    source_fd = os.open(source, _READ_FLAGS)
    try:
        info = os.fstat(source_fd)
        if not stat.S_ISREG(info.st_mode):
            raise RuntimeAuthorityStageError(f"not a regular file: {source}")
        destination_fd = _open_new(destination)
        try:
            digest = hashlib.sha256()
            size = 0
            while chunk := os.read(source_fd, _HASH_CHUNK):
                _write_all(destination_fd, chunk)
                digest.update(chunk)
                size += len(chunk)
            return digest.hexdigest(), size
        finally:
            os.close(destination_fd)
    finally:
        os.close(source_fd)


# ---------------------------------------------------------------------------------------
# The documents both sides derive
# ---------------------------------------------------------------------------------------


def relative_role_payloads() -> dict[str, dict[str, object]]:
    """`full-manifest.json` `roles`: every policy role, paths relative to the generation."""

    return {
        entry.name: {
            "python_path": GENERATION_PYTHON,
            "module": entry.module,
            "working_directory": GENERATION_CWD,
            "app_source": GENERATION_APP_SOURCE,
            "site_packages": [GENERATION_SITE_PACKAGES],
        }
        for entry in PRODUCTION_ROLE_POLICY
    }


def role_specs(generation_path: Path) -> dict[str, RuntimeRoleSpec]:
    return {
        entry.name: RuntimeRoleSpec(
            python_path=generation_path / GENERATION_PYTHON,
            module=entry.module,
            working_directory=generation_path / GENERATION_CWD,
            app_source=generation_path / GENERATION_APP_SOURCE,
            site_packages=(generation_path / GENERATION_SITE_PACKAGES,),
        )
        for entry in PRODUCTION_ROLE_POLICY
    }


def full_manifest_bytes(profile_id: str, entries: Sequence[Mapping[str, object]]) -> bytes:
    return canonical_json_bytes(
        {
            "schema_id": authority.PRODUCTION_MANIFEST_SCHEMA["schema_id"],
            "profile_id": profile_id,
            "roles": relative_role_payloads(),
            "entries": list(entries),
        },
        trailing_newline=True,
    )


def generation_slot(
    *,
    generation_id: str,
    commit: str,
    profile_id: str,
    generation_root: Path | None = None,
) -> RuntimeGenerationSlot:
    root = authority.PRODUCTION_GENERATION_ROOT if generation_root is None else generation_root
    generation_path = root / generation_id
    return RuntimeGenerationSlot(
        lifecycle=RuntimeGenerationLifecycle.ACTIVE,
        generation_id=generation_id,
        generation_path=generation_path,
        commit=commit,
        full_manifest_hash=generation_id,
        profile_id=profile_id,
        roles=role_specs(generation_path),
    )


def candidate_record(
    previous: RuntimeAuthorityRecord | None,
    slot: RuntimeGenerationSlot,
    *,
    operation_id: str,
) -> RuntimeAuthorityRecord:
    """The record `prepare_runtime_authority_publish` will produce, computed without the
    installed profile so the unprivileged side can stage `current.json` ahead of time."""

    authority._require_operation_id(operation_id)
    if previous is None:
        return RuntimeAuthorityRecord(
            schema_version=authority.RECORD_SCHEMA_VERSION,
            operation_id=operation_id,
            sequence=1,
            state=RuntimeAuthorityState.ACTIVE,
            current=slot,
            prior=None,
        )
    if operation_id == previous.operation_id:
        raise RuntimeAuthorityRecordError("operation id must be unique")
    recorded = {previous.current.generation_id}
    if previous.prior is not None:
        recorded.add(previous.prior.generation_id)
    if slot.generation_id in recorded:
        raise RuntimeAuthorityRecordError("next generation is already recorded")
    return RuntimeAuthorityRecord(
        schema_version=authority.RECORD_SCHEMA_VERSION,
        operation_id=operation_id,
        sequence=previous.sequence + 1,
        state=RuntimeAuthorityState.ACTIVE,
        current=slot,
        prior=replace(previous.current, lifecycle=RuntimeGenerationLifecycle.ROLLBACK_READY),
    )


@dataclass(frozen=True)
class ClosurePolicy:
    """The declarative interpreter closure a profile carries (S1 §1.2D)."""

    system_python: RuntimeFilePolicy
    elf_loader: RuntimeFilePolicy
    stdlib: tuple[RuntimeFilePolicy, ...]
    shared_libraries: tuple[RuntimeFilePolicy, ...]
    deploy_pyz: RuntimeFilePolicy
    runtime_pyz: RuntimeFilePolicy
    ancestors: tuple[RuntimeAncestorPolicy, ...]

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


def profile_document(
    closure: ClosurePolicy,
    instances: Mapping[str, Sequence[str]],
) -> tuple[str, bytes, RuntimeClosureProfile]:
    """The profile body for the frozen policy plus these instance labels, self-identified."""

    roles: dict[str, object] = {}
    for entry in PRODUCTION_ROLE_POLICY:
        labels = tuple(sorted(set(instances.get(entry.name, ()))))
        if bool(labels) is not entry.instanced:
            raise RuntimeAuthorityStageError(
                f"role {entry.name} instance labels do not match its instanced policy"
            )
        roles[entry.name] = {
            "module": entry.module,
            "environment_allowlist": list(entry.environment_allowlist),
            "instances": list(labels),
            "service_kind": entry.service_kind,
            "control_root": entry.control_root,
            "once": entry.once,
            "module_arguments": list(entry.module_arguments),
        }
    body: dict[str, object] = {
        "schema_version": authority.PROFILE_SCHEMA_VERSION,
        "platform": "linux",
        "ancestors": [item.payload() for item in closure.ancestors],
        "system_python": closure.system_python.payload(),
        "elf_loader": closure.elf_loader.payload(),
        "stdlib": [item.payload() for item in closure.stdlib],
        "shared_libraries": [item.payload() for item in closure.shared_libraries],
        "deploy_pyz": closure.deploy_pyz.payload(),
        "runtime_pyz": closure.runtime_pyz.payload(),
        "inbox_root": str(authority.PRODUCTION_INBOX_ROOT),
        "quarantine_root": str(authority.PRODUCTION_QUARANTINE_ROOT),
        "generation_root": str(authority.PRODUCTION_GENERATION_ROOT),
        "allowed_operations": list(authority.PRODUCTION_ALLOWED_OPERATIONS),
        "roles": roles,
        "manifest_schema": {
            key: list(value) if isinstance(value, tuple) else value
            for key, value in authority.PRODUCTION_MANIFEST_SCHEMA.items()
        },
    }
    profile_id = sha256_bytes(canonical_json_bytes(body))
    payload = canonical_json_bytes({**body, "profile_id": profile_id})
    profile = parse_runtime_closure_profile(payload)
    return profile_id, payload, profile


# ---------------------------------------------------------------------------------------
# plan.json
# ---------------------------------------------------------------------------------------


def plan_bytes(plan: Mapping[str, object]) -> bytes:
    return canonical_json_bytes(dict(plan), trailing_newline=True)


def parse_plan(payload: bytes) -> dict[str, object]:
    """Decode `plan.json` strictly: canonical bytes, fixed fields, validated shapes."""

    if len(payload) > MAX_PLAN_BYTES:
        raise RuntimeAuthorityStageError("plan.json exceeds its bounded size")
    try:
        data = strict_json_loads(payload)
    except StrictJsonError as exc:
        raise RuntimeAuthorityStageError(f"plan.json is not strict JSON: {exc}") from exc
    if type(data) is not dict or set(data) != _PLAN_FIELDS:
        raise RuntimeAuthorityStageError("plan.json has unexpected fields")
    if plan_bytes(data) != payload:
        raise RuntimeAuthorityStageError("plan.json is not canonical")
    if data["schema_id"] != PLAN_SCHEMA_ID or data["schema_version"] != PLAN_SCHEMA_VERSION:
        raise RuntimeAuthorityStageError("plan.json schema is unsupported")
    if data["mode"] not in _PLAN_MODES:
        raise RuntimeAuthorityStageError("plan.json mode is invalid")
    try:
        authority._require_operation_id(data["operation_id"])
    except RuntimeAuthorityRecordError as exc:
        raise RuntimeAuthorityStageError(str(exc)) from exc
    previous = data["previous_operation_id"]
    if previous is not None:
        try:
            authority._require_operation_id(previous)
        except RuntimeAuthorityRecordError as exc:
            raise RuntimeAuthorityStageError(str(exc)) from exc
    commit = data["producer_commit"]
    if type(commit) is not str or len(commit) != 40 or any(c not in _SHA256_HEX for c in commit):
        raise RuntimeAuthorityStageError("plan.json producer commit is not a commit sha")
    for field in ("profile_id", "generation_id", "runtime_pyz_sha256", "deploy_pyz_sha256"):
        require_sha256(data[field], f"plan.json {field}")
    require_sha256(data["system_python_sha256"], "plan.json system_python_sha256")
    if type(data["sequence"]) is not int or data["sequence"] < 1:
        raise RuntimeAuthorityStageError("plan.json sequence is invalid")
    if (data["sequence"] == 1) is not (previous is None):
        raise RuntimeAuthorityStageError("plan.json sequence disagrees with its previous operation")
    mapping = data["instance_mapping"]
    if type(mapping) is not dict or any(
        type(labels) is not list
        or any(type(label) is not str or authority._ROLE_INSTANCE.fullmatch(label) is None
               for label in labels)
        for labels in mapping.values()
    ):
        raise RuntimeAuthorityStageError("plan.json instance mapping is invalid")
    if type(data["service_manifests"]) is not dict or type(data["closure_summary"]) is not dict:
        raise RuntimeAuthorityStageError("plan.json summaries are invalid")
    staged = data["staged_files"]
    if type(staged) is not dict or not staged:
        raise RuntimeAuthorityStageError("plan.json staged files are invalid")
    for relative, entry in staged.items():
        validate_relative_path(relative, "plan.json staged path")
        if type(entry) is not dict or set(entry) != _STAGED_FILE_FIELDS:
            raise RuntimeAuthorityStageError(f"plan.json staged entry is invalid: {relative}")
        if entry["type"] == "directory":
            valid = (
                entry["mode"] == DIRECTORY_MODE
                and entry["size"] == 0
                and entry["sha256"] is None
            )
        elif entry["type"] == "file":
            valid = (
                entry["mode"] in (FILE_MODE, EXECUTABLE_MODE)
                and type(entry["size"]) is int
                and entry["size"] >= 0
                and type(entry["sha256"]) is str
            )
            if valid:
                require_sha256(entry["sha256"], f"plan.json staged digest {relative}")
        else:
            valid = False
        if not valid:
            raise RuntimeAuthorityStageError(f"plan.json staged entry is invalid: {relative}")
    manifest_relative = f"{GENERATION_NAME}/{authority.GENERATION_MANIFEST_NAME}"
    for required in (PROFILE_NAME, RECORD_NAME, manifest_relative):
        if staged.get(required, {}).get("type") != "file":
            raise RuntimeAuthorityStageError(f"plan.json does not stage {required}")
    if staged.get(GENERATION_NAME, {}).get("type") != "directory":
        raise RuntimeAuthorityStageError("plan.json does not stage a generation directory")
    return data


def staged_files_for(
    layout_entries: Sequence[Mapping[str, object]],
    *,
    profile_payload: bytes,
    record_payload: bytes,
    manifest_payload: bytes,
) -> dict[str, dict[str, object]]:
    """`plan.json` `staged_files`: the staging directory relative to itself, `plan.json`
    excluded (it cannot carry its own digest; the operator carries that out of band)."""

    def file_entry(payload: bytes, mode: int = FILE_MODE) -> dict[str, object]:
        return {"type": "file", "mode": mode, "size": len(payload), "sha256": sha256_bytes(payload)}

    staged: dict[str, dict[str, object]] = {
        PROFILE_NAME: file_entry(profile_payload),
        RECORD_NAME: file_entry(record_payload),
        GENERATION_NAME: {"type": "directory", "mode": DIRECTORY_MODE, "size": 0, "sha256": None},
        f"{GENERATION_NAME}/{authority.GENERATION_MANIFEST_NAME}": file_entry(manifest_payload),
    }
    for entry in layout_entries:
        staged[f"{GENERATION_NAME}/{entry['path']}"] = {
            "type": entry["type"],
            "mode": entry["mode"],
            "size": entry["size"],
            "sha256": entry["sha256"],
        }
    return dict(sorted(staged.items()))


# ---------------------------------------------------------------------------------------
# The root transaction
# ---------------------------------------------------------------------------------------


@dataclass(frozen=True)
class _StagedGeneration:
    plan: dict[str, object]
    plan_payload: bytes
    plan_sha256: str
    profile: RuntimeClosureProfile
    profile_payload: bytes
    manifest_payload: bytes
    record_payload: bytes
    slot: RuntimeGenerationSlot


def _read_staged(staging: Path, relative: str, *, max_bytes: int) -> bytes:
    path = staging / relative
    try:
        descriptor = os.open(path, _READ_FLAGS)
    except OSError as exc:
        raise RuntimeAuthorityPublishError(f"staged {relative} is not readable: {exc}") from exc
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise RuntimeAuthorityPublishError(f"staged {relative} is not a regular file")
        if info.st_size > max_bytes:
            raise RuntimeAuthorityPublishError(f"staged {relative} exceeds its bounded size")
        chunks: list[bytes] = []
        while chunk := os.read(descriptor, _HASH_CHUNK):
            chunks.append(chunk)
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _load_staging(staging: Path, expected_plan_sha256: str) -> _StagedGeneration:
    staging = Path(os.path.abspath(staging))
    try:
        expected = require_sha256(expected_plan_sha256, "--expect-plan-sha256")
    except RuntimeAuthorityStageError as exc:
        raise RuntimeAuthorityPublishError(str(exc)) from exc
    plan_payload = _read_staged(staging, PLAN_NAME, max_bytes=MAX_PLAN_BYTES)
    plan_sha256 = sha256_bytes(plan_payload)
    if plan_sha256 != expected:
        raise RuntimeAuthorityPublishError(
            f"plan.json sha256 {plan_sha256} does not match the confirmed {expected}"
        )
    try:
        plan = parse_plan(plan_payload)
    except RuntimeAuthorityStageError as exc:
        raise RuntimeAuthorityPublishError(f"plan.json is invalid: {exc}") from exc
    staged = plan["staged_files"]
    assert type(staged) is dict

    def staged_bytes(relative: str, max_bytes: int) -> bytes:
        payload = _read_staged(staging, relative, max_bytes=max_bytes)
        declared = staged[relative]
        if sha256_bytes(payload) != declared["sha256"] or len(payload) != declared["size"]:
            raise RuntimeAuthorityPublishError(f"staged {relative} does not match plan.json")
        return payload

    profile_payload = staged_bytes(PROFILE_NAME, authority.MAX_PROFILE_BYTES)
    try:
        profile = parse_runtime_closure_profile(profile_payload)
    except RuntimeAuthorityError as exc:
        raise RuntimeAuthorityPublishError(f"staged profile is invalid: {exc}") from exc
    if profile.profile_id != plan["profile_id"]:
        raise RuntimeAuthorityPublishError("staged profile id does not match plan.json")
    if profile.runtime_pyz.sha256 != plan["runtime_pyz_sha256"]:
        raise RuntimeAuthorityPublishError("staged profile runtime pyz digest disagrees with plan")
    manifest_relative = f"{GENERATION_NAME}/{authority.GENERATION_MANIFEST_NAME}"
    manifest_payload = staged_bytes(manifest_relative, authority.MAX_GENERATION_MANIFEST_BYTES)
    generation_id = sha256_bytes(manifest_payload)
    if generation_id != plan["generation_id"]:
        raise RuntimeAuthorityPublishError("staged full manifest hash does not match plan.json")
    commit = plan["producer_commit"]
    assert type(commit) is str
    try:
        slot = generation_slot(
            generation_id=generation_id, commit=commit, profile_id=profile.profile_id
        )
        authority._validate_generation_manifest(manifest_payload, slot, profile)
    except RuntimeAuthorityError as exc:
        raise RuntimeAuthorityPublishError(f"staged full manifest is invalid: {exc}") from exc
    record_payload = staged_bytes(RECORD_NAME, authority.MAX_RECORD_BYTES)
    return _StagedGeneration(
        plan=plan,
        plan_payload=plan_payload,
        plan_sha256=plan_sha256,
        profile=profile,
        profile_payload=profile_payload,
        manifest_payload=manifest_payload,
        record_payload=record_payload,
        slot=slot,
    )


@dataclass(frozen=True)
class _AuthorityState:
    """The three root-owned inputs a publication decides on, as bytes read off the disk."""

    profile_payload: bytes | None
    record_payload: bytes | None
    runtime_pyz_sha256: str


def _read_root_bytes(path: Path, *, profile: bool) -> bytes | None:
    if not os.path.lexists(path):
        return None
    if profile:
        return authority._read_trusted_file(
            path,
            directory_policy=authority._PRODUCTION_PROFILE_DIRECTORY_POLICY,
            owner_uid=authority.PRODUCTION_PROFILE_OWNER_UID,
            file_mode=authority.PRODUCTION_PROFILE_MODE,
            max_bytes=authority.MAX_PROFILE_BYTES,
            error_type=authority.ProductionRuntimeProfileError,
            label="installed profile",
        )
    return authority._read_trusted_file(
        path,
        directory_policy=authority._PRODUCTION_RUNTIME_DIRECTORY_POLICY,
        owner_uid=authority.RUNTIME_AUTHORITY_OWNER_UID,
        file_mode=authority.RUNTIME_AUTHORITY_RECORD_MODE,
        max_bytes=authority.MAX_RECORD_BYTES,
        error_type=RuntimeAuthorityRecordError,
        label="installed authority record",
    )


def _authority_state() -> _AuthorityState:
    try:
        profile_payload = _read_root_bytes(authority.PRODUCTION_PROFILE_PATH, profile=True)
        record_payload = _read_root_bytes(authority.RUNTIME_AUTHORITY_PATH, profile=False)
    except RuntimeAuthorityError as exc:
        raise RuntimeAuthorityPublishError(
            f"installed authority state is unreadable: {exc}"
        ) from exc
    try:
        runtime_pyz_sha256, _size = sha256_file(INSTALLED_RUNTIME_PYZ)
    except (OSError, RuntimeAuthorityError) as exc:
        raise RuntimeAuthorityPublishError(
            f"installed runtime pyz {INSTALLED_RUNTIME_PYZ} is unreadable: {exc}"
        ) from exc
    return _AuthorityState(
        profile_payload=profile_payload,
        record_payload=record_payload,
        runtime_pyz_sha256=runtime_pyz_sha256,
    )


def _require_state_unchanged(snapshot: _AuthorityState) -> None:
    """S-1: the preflight read outside the lock; nothing it saw may have moved since."""

    current = _authority_state()
    for label, before, after in (
        ("profile", snapshot.profile_payload, current.profile_payload),
        ("record", snapshot.record_payload, current.record_payload),
        ("runtime pyz", snapshot.runtime_pyz_sha256, current.runtime_pyz_sha256),
    ):
        if before != after:
            raise RuntimeAuthorityPublishError(
                f"authority state changed since preflight: the installed {label} differs"
            )


def read_previous_record(profile: RuntimeClosureProfile) -> RuntimeAuthorityRecord | None:
    """The installed `current.json` parsed under `profile`, or `None` when none exists."""

    if not os.path.lexists(authority.RUNTIME_AUTHORITY_PATH):
        return None
    payload = authority._read_trusted_file(
        authority.RUNTIME_AUTHORITY_PATH,
        directory_policy=authority._PRODUCTION_RUNTIME_DIRECTORY_POLICY,
        owner_uid=authority.RUNTIME_AUTHORITY_OWNER_UID,
        file_mode=authority.RUNTIME_AUTHORITY_RECORD_MODE,
        max_bytes=authority.MAX_RECORD_BYTES,
        error_type=RuntimeAuthorityRecordError,
        label="runtime authority record",
    )
    record = authority._parse_runtime_authority_record(payload, profile)
    if payload != canonical_runtime_authority_bytes(record):
        raise RuntimeAuthorityRecordError("runtime authority record is not canonical")
    return record


@dataclass(frozen=True)
class _Preflight:
    installed: RuntimeClosureProfile | None
    previous: RuntimeAuthorityRecord | None
    #: The installed record already is this operation's record: nothing to do but prove it.
    idempotent: bool
    #: What the disk held when these decisions were made; re-read inside the lock (S-1).
    state: _AuthorityState


def _preflight(staged: _StagedGeneration) -> _Preflight:
    """Read-only checks that refuse before any root path is touched."""

    if os.geteuid() != authority.RUNTIME_AUTHORITY_OWNER_UID:
        raise RuntimeAuthorityPublishError(
            f"publish must run as uid {authority.RUNTIME_AUTHORITY_OWNER_UID}"
        )
    state = _authority_state()
    installed: RuntimeClosureProfile | None = None
    previous: RuntimeAuthorityRecord | None = None
    try:
        if state.profile_payload is not None:
            installed = parse_runtime_closure_profile(state.profile_payload)
        if state.record_payload is not None:
            if installed is None:
                raise RuntimeAuthorityPublishError(
                    "an authority record is installed without a profile"
                )
            previous = authority._parse_runtime_authority_record(state.record_payload, installed)
            if state.record_payload != canonical_runtime_authority_bytes(previous):
                raise RuntimeAuthorityRecordError("runtime authority record is not canonical")
    except RuntimeAuthorityPublishError:
        raise
    except RuntimeAuthorityError as exc:
        raise RuntimeAuthorityPublishError(f"installed authority state is invalid: {exc}") from exc
    operation_id = staged.plan["operation_id"]
    assert type(operation_id) is str
    if state.runtime_pyz_sha256 != staged.profile.runtime_pyz.sha256:
        raise RuntimeAuthorityPublishError(
            f"installed runtime pyz {INSTALLED_RUNTIME_PYZ} sha256 {state.runtime_pyz_sha256} "
            f"does not match the profile's {staged.profile.runtime_pyz.sha256}"
        )
    if previous is not None and previous.operation_id == operation_id:
        if canonical_runtime_authority_bytes(previous) != staged.record_payload:
            raise RuntimeAuthorityPublishError("authority operation id conflicts")
        assert installed is not None
        if installed.profile_id != staged.profile.profile_id:
            raise RuntimeAuthorityPublishError("authority operation id conflicts")
        return _Preflight(installed=installed, previous=previous, idempotent=True, state=state)
    if (
        installed is not None
        and previous is not None
        and installed.profile_id != staged.profile.profile_id
    ):
        raise RuntimeAuthorityPublishError(
            "an authority record exists under profile "
            f"{installed.profile_id}; the staged profile {staged.profile.profile_id} "
            "cannot replace it — the publish primitives validate every record against "
            "the installed profile, so a profile change is not a supported transition"
        )
    previous_operation = None if previous is None else previous.operation_id
    if staged.plan["previous_operation_id"] != previous_operation:
        raise RuntimeAuthorityPublishError(
            "the authority record advanced since staging: plan expects previous operation "
            f"{staged.plan['previous_operation_id']}, installed is {previous_operation}"
        )
    try:
        expected_record = candidate_record(previous, staged.slot, operation_id=operation_id)
    except RuntimeAuthorityRecordError as exc:
        raise RuntimeAuthorityPublishError(f"candidate record is invalid: {exc}") from exc
    if canonical_runtime_authority_bytes(expected_record) != staged.record_payload:
        raise RuntimeAuthorityPublishError(
            "staged current.json is not the record this publication would write"
        )
    return _Preflight(installed=installed, previous=previous, idempotent=False, state=state)


def _ensure_directory(path: Path, *, mode: int) -> None:
    if os.path.lexists(path):
        return
    os.mkdir(path, mode)
    os.chmod(path, mode)
    info = os.lstat(path)
    if info.st_uid != authority.RUNTIME_AUTHORITY_OWNER_UID or info.st_gid != PUBLISH_OWNER_GID:
        os.chown(
            path,
            authority.RUNTIME_AUTHORITY_OWNER_UID,
            PUBLISH_OWNER_GID,
            follow_symlinks=False,
        )


def _ensure_authority_directories() -> None:
    """Create the root-owned anchors a first installation lacks, with their policy modes."""

    for policy in (
        authority._PRODUCTION_PROFILE_DIRECTORY_POLICY,
        authority._PRODUCTION_RUNTIME_DIRECTORY_POLICY,
    ):
        for path, (_owner, mode) in policy.items():
            if path.parent == path or path in (Path("/etc"), Path("/var"), Path("/var/lib")):
                continue
            _ensure_directory(path, mode=mode)
    _ensure_directory(authority.PRODUCTION_INBOX_ROOT, mode=INBOX_DIRECTORY_MODE)
    _ensure_directory(authority.PRODUCTION_QUARANTINE_ROOT, mode=INBOX_DIRECTORY_MODE)


def _fsync_path(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _own_descriptor(descriptor: int) -> None:
    info = os.fstat(descriptor)
    if info.st_uid != authority.RUNTIME_AUTHORITY_OWNER_UID or info.st_gid != PUBLISH_OWNER_GID:
        os.fchown(descriptor, authority.RUNTIME_AUTHORITY_OWNER_UID, PUBLISH_OWNER_GID)


def _make_writable(root: Path) -> None:
    for directory, _subdirectories, _files in os.walk(root):
        os.chmod(directory, 0o700)


def _quarantine(inbox: Path, reason: str) -> None:
    target = authority.PRODUCTION_QUARANTINE_ROOT / inbox.name
    with suppress(OSError):
        os.chmod(inbox, INBOX_DIRECTORY_MODE)
    try:
        os.rename(inbox, target)
        _fsync_path(target.parent)
    except OSError as exc:
        raise RuntimeAuthorityPublishError(
            f"{reason}; the inbox {inbox} could not be quarantined: {exc}"
        ) from exc
    _log(f"quarantined {inbox} -> {target}: {reason}")


def _copy_staging_into_inbox(staging: Path, staged: _StagedGeneration, inbox: Path) -> None:
    """Copy every planned path, file by file, and refuse on the first byte that differs."""

    planned = staged.plan["staged_files"]
    assert type(planned) is dict
    _write_new_file(inbox / PLAN_NAME, staged.plan_payload)
    os.chmod(inbox / PLAN_NAME, FILE_MODE)
    # The staging tree is `lighthouse`-writable. `O_NOFOLLOW` guards each final component;
    # every directory on the way is `lstat`ed too, so a planted symlink cannot point root's
    # copy at a tree the plan never described.
    if not stat.S_ISDIR(os.lstat(staging).st_mode):
        raise RuntimeAuthorityPublishError("staging is not a directory")
    for relative, entry in planned.items():
        destination = inbox / relative
        source = staging / relative
        info = os.lstat(source)
        if entry["type"] == "directory":
            if not stat.S_ISDIR(info.st_mode):
                raise RuntimeAuthorityPublishError(f"staged {relative} is not a directory")
            os.mkdir(destination, 0o700)
            continue
        if not stat.S_ISREG(info.st_mode):
            raise RuntimeAuthorityPublishError(f"staged {relative} is not a regular file")
        digest, size = _copy_new_file(source, destination)
        if digest != entry["sha256"] or size != entry["size"]:
            raise RuntimeAuthorityPublishError(
                f"staged {relative} changed after staging (digest {digest}, expected "
                f"{entry['sha256']})"
            )
        descriptor = os.open(destination, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            _own_descriptor(descriptor)
            os.fchmod(descriptor, int(entry["mode"]))
        finally:
            os.close(descriptor)
    for relative, entry in sorted(planned.items(), reverse=True):
        if entry["type"] == "directory" and relative != GENERATION_NAME:
            path = inbox / relative
            os.chown(path, authority.RUNTIME_AUTHORITY_OWNER_UID, PUBLISH_OWNER_GID)
            os.chmod(path, int(entry["mode"]))
    os.sync()


def _install_profile(staged: _StagedGeneration, snapshot: _AuthorityState) -> bool:
    target = authority.PRODUCTION_PROFILE_PATH
    try:
        current = _read_root_bytes(target, profile=True)
    except RuntimeAuthorityError as exc:
        raise RuntimeAuthorityPublishError(f"installed profile is unreadable: {exc}") from exc
    # Never replace bytes the preflight did not decide on (S-1): the disk must still hold
    # exactly what the snapshot held, whether that is nothing or the profile being kept.
    if current != snapshot.profile_payload:
        raise RuntimeAuthorityPublishError(
            "authority state changed since preflight: the installed profile differs"
        )
    if current == staged.profile_payload:
        return False
    operation_id = staged.plan["operation_id"]
    temporary = target.with_name(f".{target.name}.{operation_id}.tmp")
    with suppress(FileNotFoundError):
        os.unlink(temporary)
    descriptor = _open_new(temporary, 0o600)
    try:
        _write_all(descriptor, staged.profile_payload)
        _own_descriptor(descriptor)
        os.fchmod(descriptor, authority.PRODUCTION_PROFILE_MODE)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, target)
    _fsync_path(target.parent)
    reloaded = authority.load_production_runtime_profile()
    if reloaded.profile_id != staged.profile.profile_id:
        raise RuntimeAuthorityPublishError("installed profile does not read back as staged")
    return True


def _place_generation(inbox: Path, staged: _StagedGeneration) -> bool:
    """Rename the inbox generation into the content-addressed slot, or accept an identical
    one that is already there. Returns whether a new directory was placed."""

    target = authority.PRODUCTION_GENERATION_ROOT / staged.slot.generation_id
    source = inbox / GENERATION_NAME
    if os.path.lexists(target):
        try:
            authority._revalidate_generation_slot(staged.slot, staged.profile)
        except RuntimeAuthorityError as exc:
            raise RuntimeAuthorityPublishError(
                f"generation {staged.slot.generation_id} already exists and does not "
                f"validate: {exc}"
            ) from exc
        _log(f"generation {staged.slot.generation_id} already in place and identical")
        return False
    os.rename(source, target)
    os.chown(target, authority.RUNTIME_AUTHORITY_OWNER_UID, PUBLISH_OWNER_GID)
    os.chmod(target, authority.GENERATION_DIRECTORY_MODE)
    _fsync_path(target.parent)
    try:
        authority._revalidate_generation_slot(staged.slot, staged.profile)
    except RuntimeAuthorityError as exc:
        _make_writable(target)
        os.rename(target, source)
        raise RuntimeAuthorityPublishError(f"placed generation does not validate: {exc}") from exc
    return True


def wrapper_preflight(
    profile: RuntimeClosureProfile,
    *,
    authority_path: Path,
) -> dict[str, dict[str, object]]:
    """Run the real wrapper resolution for every (role, instance) the profile authorises."""

    launches: dict[str, dict[str, object]] = {}
    for name, role in profile.roles.items():
        for instance in role.instances or (None,):
            try:
                launch = _verify.resolve_launch(
                    name,
                    instance=instance,
                    profile_path=str(authority.PRODUCTION_PROFILE_PATH),
                    authority_path=str(authority_path),
                    generation_root=str(authority.PRODUCTION_GENERATION_ROOT),
                    trusted_root=WRAPPER_TRUSTED_ROOT,
                    expected_owner_uid=authority.RUNTIME_AUTHORITY_OWNER_UID,
                    source_environment={},
                )
            except _verify.RuntimeExecError as exc:
                raise RuntimeAuthorityPublishError(
                    f"wrapper preflight refused role {name} instance {instance}: {exc}"
                ) from exc
            key = name if instance is None else f"{name}@{instance}"
            launches[key] = {"module_argv": list(launch["module_argv"])}
    return launches


def _append_publication(receipt: Mapping[str, object]) -> None:
    path = authority.RUNTIME_AUTHORITY_ANCHOR / PUBLICATIONS_NAME
    flags = (
        os.O_WRONLY
        | os.O_APPEND
        | os.O_CREAT
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    descriptor = os.open(path, flags, PUBLICATIONS_MODE)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise RuntimeAuthorityPublishError("publications log is not a regular file")
        _own_descriptor(descriptor)
        os.fchmod(descriptor, PUBLICATIONS_MODE)
        _write_all(descriptor, canonical_json_bytes(dict(receipt), trailing_newline=True))
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _log(message: str) -> None:
    sys.stderr.write(f"{message}\n")
    sys.stderr.flush()


def publish_staging(
    staging: Path,
    *,
    expect_plan_sha256: str,
    dry_run: bool = False,
) -> dict[str, object]:
    """The root transaction of S1 §1.3.3. Returns the receipt; raises on any refusal."""

    staging = Path(os.path.abspath(staging))
    staged = _load_staging(staging, expect_plan_sha256)
    preflight = _preflight(staged)
    previous = preflight.previous
    operation_id = staged.plan["operation_id"]
    assert type(operation_id) is str
    if preflight.idempotent:
        return _publish_idempotent(staged, dry_run=dry_run)
    inbox = authority.PRODUCTION_INBOX_ROOT / operation_id
    generation_target = authority.PRODUCTION_GENERATION_ROOT / staged.slot.generation_id
    steps = [
        f"copy {staging} into {inbox} and recompute every digest against plan.json",
        f"install {authority.PRODUCTION_PROFILE_PATH} (profile {staged.profile.profile_id})",
        f"rename {inbox / GENERATION_NAME} to {generation_target}",
        "run the wrapper preflight for every (role, instance) of the profile",
        f"publish {authority.RUNTIME_AUTHORITY_PATH} (operation {operation_id}, "
        f"sequence {staged.plan['sequence']})",
        f"append a receipt to {authority.RUNTIME_AUTHORITY_ANCHOR / PUBLICATIONS_NAME}",
    ]
    if dry_run:
        for step in steps:
            _log(f"dry-run: {step}")
        return {
            "dry_run": True,
            "operation_id": operation_id,
            "profile_id": staged.profile.profile_id,
            "generation_id": staged.slot.generation_id,
            "sequence": staged.plan["sequence"],
            "plan_sha256": staged.plan_sha256,
            "steps": steps,
        }

    try:
        _ensure_authority_directories()
    except OSError as exc:
        raise RuntimeAuthorityPublishError(f"authority directories unavailable: {exc}") from exc
    lock = authority.acquire_runtime_deployment_lock()
    placed = False
    profile_written = False
    try:
        with lock:
            # The preflight read outside the lock (`publish_runtime_authority` takes it
            # itself, so it cannot be held across the whole transaction). Re-read the same
            # three inputs here and refuse if any moved, before a single root path is touched.
            _require_state_unchanged(preflight.state)
            if os.path.lexists(inbox):
                raise RuntimeAuthorityPublishError(
                    f"inbox {inbox} already exists; inspect and remove it before retrying"
                )
            os.mkdir(inbox, INBOX_DIRECTORY_MODE)
            os.chown(inbox, authority.RUNTIME_AUTHORITY_OWNER_UID, PUBLISH_OWNER_GID)
            try:
                _log(f"copying staging into {inbox}")
                _copy_staging_into_inbox(staging, staged, inbox)
                profile_written = _install_profile(staged, preflight.state)
                _log(
                    f"profile {authority.PRODUCTION_PROFILE_PATH} "
                    f"{'installed' if profile_written else 'already current'}"
                )
                placed = _place_generation(inbox, staged)
            except RuntimeAuthorityError:
                _quarantine(inbox, "publication refused")
                raise
            except OSError as exc:
                _quarantine(inbox, f"publication failed: {exc}")
                raise RuntimeAuthorityPublishError(f"publication failed: {exc}") from exc
        # `publish_runtime_authority` takes the deployment lock itself; flock is per open
        # file description, so holding ours across the call would deadlock (S1 §1.3.3).
        try:
            _log("running the wrapper preflight")
            launches = wrapper_preflight(staged.profile, authority_path=inbox / RECORD_NAME)
            record = authority.prepare_runtime_authority_publish(
                previous, staged.slot, operation_id=operation_id
            )
            if canonical_runtime_authority_bytes(record) != staged.record_payload:
                raise RuntimeAuthorityPublishError(
                    "the prepared record differs from the staged current.json"
                )
            result = authority.publish_runtime_authority(record)
        except RuntimeAuthorityError:
            _quarantine(inbox, "publication refused after the generation was placed")
            raise
    finally:
        lock.close()
    receipt: dict[str, object] = {
        "published_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "operation_id": operation_id,
        "sequence": record.sequence,
        "state": record.state.value,
        "profile_id": record.current.profile_id,
        "generation_id": record.current.generation_id,
        "generation_path": str(record.current.generation_path),
        "producer_commit": record.current.commit,
        "prior_generation_id": None if record.prior is None else record.prior.generation_id,
        "result": result.value,
        "profile_installed": profile_written,
        "generation_placed": placed,
        "plan_sha256": staged.plan_sha256,
        "wrapper_preflight": len(launches),
    }
    try:
        _append_publication(receipt)
        _make_writable(inbox)
        shutil.rmtree(inbox)
    except OSError as exc:
        raise RuntimeAuthorityPublishError(
            f"published, but the receipt or inbox cleanup failed: {exc}"
        ) from exc
    return receipt


def _publish_idempotent(staged: _StagedGeneration, *, dry_run: bool) -> dict[str, object]:
    """The installed record already names this operation: re-prove, write nothing."""

    assert type(staged.plan["operation_id"]) is str
    if dry_run:
        _log("dry-run: the installed record already is this operation; nothing to do")
    else:
        try:
            authority._revalidate_generation_slot(staged.slot, staged.profile)
        except RuntimeAuthorityError as exc:
            raise RuntimeAuthorityPublishError(
                f"the published generation no longer validates: {exc}"
            ) from exc
    launches = wrapper_preflight(staged.profile, authority_path=authority.RUNTIME_AUTHORITY_PATH)
    receipt: dict[str, object] = {
        "published_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "operation_id": staged.plan["operation_id"],
        "sequence": staged.plan["sequence"],
        "state": RuntimeAuthorityState.ACTIVE.value,
        "profile_id": staged.profile.profile_id,
        "generation_id": staged.slot.generation_id,
        "generation_path": str(staged.slot.generation_path),
        "producer_commit": staged.slot.commit,
        "prior_generation_id": staged.plan.get("prior_generation_id"),
        "result": authority.RuntimeAuthorityPublishResult.IDEMPOTENT.value,
        "profile_installed": False,
        "generation_placed": False,
        "plan_sha256": staged.plan_sha256,
        "wrapper_preflight": len(launches),
        "dry_run": dry_run,
    }
    if not dry_run:
        _append_publication(receipt)
    return receipt


def rollback_authority(*, operation_id: str) -> dict[str, object]:
    """Single-level rollback through the existing primitives (S1 §1.3.3)."""

    if os.geteuid() != authority.RUNTIME_AUTHORITY_OWNER_UID:
        raise RuntimeAuthorityPublishError(
            f"rollback must run as uid {authority.RUNTIME_AUTHORITY_OWNER_UID}"
        )
    previous = authority.load_runtime_authority()
    record = authority.prepare_runtime_authority_rollback(previous, operation_id=operation_id)
    result = authority.publish_runtime_authority(record)
    receipt: dict[str, object] = {
        "published_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "operation_id": operation_id,
        "sequence": record.sequence,
        "state": record.state.value,
        "profile_id": record.current.profile_id,
        "generation_id": record.current.generation_id,
        "generation_path": str(record.current.generation_path),
        "producer_commit": record.current.commit,
        "prior_generation_id": None if record.prior is None else record.prior.generation_id,
        "result": result.value,
        "rolled_back_operation_id": previous.operation_id,
    }
    _append_publication(receipt)
    return receipt


# ---------------------------------------------------------------------------------------
# Command line (`rquant-production-deploy.pyz`)
# ---------------------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="rquant-production-deploy.pyz",
        description="Root-side runtime authority publication.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    publish = commands.add_parser("publish", help="publish a staging directory")
    publish.add_argument("--staging", type=Path, required=True)
    publish.add_argument("--expect-plan-sha256", required=True)
    publish.add_argument("--dry-run", action="store_true")
    rollback = commands.add_parser("rollback", help="single-level rollback")
    rollback.add_argument("--operation-id", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        if arguments.command == "publish":
            receipt = publish_staging(
                arguments.staging,
                expect_plan_sha256=arguments.expect_plan_sha256,
                dry_run=arguments.dry_run,
            )
        else:
            receipt = rollback_authority(operation_id=arguments.operation_id)
    except RuntimeAuthorityError as error:
        sys.stderr.write(f"refused: {error}\n")
        return 1
    except OSError as error:
        sys.stderr.write(f"failed: {error}\n")
        return 1
    sys.stdout.write(json.dumps(receipt, sort_keys=True, indent=2) + "\n")
    return 0


__all__ = [
    "ClosurePolicy",
    "GenerationLayout",
    "RuntimeAuthorityStageError",
    "StagedFile",
    "candidate_record",
    "detect_directory_link_convention",
    "full_manifest_bytes",
    "generation_slot",
    "main",
    "materialize_layout",
    "parse_plan",
    "plan_bytes",
    "predict_manifest_entries",
    "profile_document",
    "publish_staging",
    "read_previous_record",
    "relative_role_payloads",
    "rollback_authority",
    "role_specs",
    "scan_frozen_tree",
    "staged_files_for",
    "wrapper_preflight",
]
