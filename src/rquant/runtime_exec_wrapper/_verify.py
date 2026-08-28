"""The whole of the runtime wrapper's verification, in one self-contained stdlib module.

`authority.md` L1707-1743 asks for the same strict validation twice: once in the root-owned
wrapper before it execs, and once in the child bootstrap after it. Writing it twice would
guarantee the two copies drift, so it is written once, here, with no imports of its own
beyond the standard library and no relative imports at all. The build embeds this exact
source text as `FROZEN_BOOTSTRAP`, appends a fixed trailer that calls `child_main`, and the
wrapper hands the result to the generation interpreter through `-c`. The role travels in
`argv`; no record path, manifest path or environment value is ever interpolated into it.

What the wrapper trusts, in order:

1. `/etc/rquant/production-runtime-profile.json` — root-owned, `0444`, single link, reached
   through an anchored no-follow walk whose every component is a canonical root-owned
   directory. Its `profile_id` is recomputed from its own body, so the whole document is
   integrity-checked rather than spot-checked.
2. `/var/lib/rquant/runtime-authority/current.json` — same treatment. The `current_*` slot
   is authoritative; `commit` is read as untrusted audit metadata and nothing branches on it.
3. `<generation>/full-manifest.json` — hashed against the slot's `full_manifest_hash`, then
   every entry it declares is checked on disk: type, owner, mode, link count and SHA-256.

What it never does: dereference the `data/runtime/current` symlink, read
`data/runtime/current/runtime.env`, accept a manifest path from a unit, search `PATH`, take
a module or path override, or let any environment variable reach an authority decision.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import stat
import sys
from collections.abc import Sequence
from typing import Any

SCHEMA_VERSION = 1

PROFILE_PATH = "/etc/rquant/production-runtime-profile.json"
AUTHORITY_PATH = "/var/lib/rquant/runtime-authority/current.json"
GENERATION_ROOT = "/var/lib/rquant/runtime-authority/generations"
GENERATION_MANIFEST_NAME = "full-manifest.json"
RUNTIME_PYZ_PATH = "/usr/local/libexec/rquant-runtime-exec.pyz"

TRUSTED_ROOT = "/"
OWNER_UID = 0
OWNER_GID = 0
PROFILE_FILE_MODE = 0o444
RECORD_FILE_MODE = 0o444
MANIFEST_FILE_MODE = 0o444

MAX_PROFILE_BYTES = 16 * 1024 * 1024
MAX_RECORD_BYTES = 4 * 1024 * 1024
MAX_MANIFEST_BYTES = 16 * 1024 * 1024
MAX_JSON_DEPTH = 64
MAX_MANIFEST_ENTRIES = 100_000
HASH_CHUNK_BYTES = 65536

#: Every role a unit is allowed to name, as a frozen literal set. A unit passes one of these
#: and nothing else; the record and the profile must both agree that the role exists before
#: anything is executed. `%i` never reaches this list — a template instance name is a systemd
#: label, not an authority value (ruling D-2).
PROTECTED_ROLES = (
    "artifact_retention",
    "auction_match_source",
    "auction_universe_publisher",
    "candidate_publisher",
    "daily",
    "daily_close_source",
    "daily_pipeline_orchestrator",
    "feature_live",
    "lab_artifact_catalog",
    # Amended per Codex round-3 verdict 2026-08-28, item RQ-WI-R2-P1-02: the formal claim
    # finalizer stops proving its own trustworthiness with the checkout it is proving things
    # about, and reaches `rquant.lab_formal_runtime_entry` through this wrapper instead.
    "lab_claim_finalizer",
    "lab_jobs_publisher",
    "market_minute_source",
    "notifier",
    "page_control",
    "paper_broker",
    "paper_constraint_publisher",
    "promotions_publisher",
    "reference_slow_publisher",
    "reference_slow_source",
    "runtime_health_publisher",
    "runtime_recovery",
    "runtime_recovery_rehearsal",
    "serving_publisher",
    "shadow_session",
    "signal_router",
    "strategy_live",
    "watchlist_quote_source",
    # Amended per Codex round-3 verdict 2026-08-28, item RQ-WI-R2-P1-01: the first role that
    # no unit names. `deploy/libexec/rquant-workload-arbiter` execs it itself, before the
    # unit's own child, so the research admission probe stops being checkout code that runs
    # ahead of any verification.
    "workload_admission",
)

_PROFILE_FIELDS = frozenset(
    {
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
)
_SLOT_FIELDS = (
    "lifecycle",
    "generation_id",
    "generation_path",
    "commit",
    "full_manifest_hash",
    "profile_id",
    "roles",
)
_RECORD_FIELDS = frozenset(
    {"schema_version", "operation_id", "sequence", "state"}
    | {f"{prefix}_{field}" for prefix in ("current", "prior") for field in _SLOT_FIELDS}
)
_ROLE_FIELDS = frozenset(
    {"python_path", "module", "working_directory", "app_source", "site_packages"}
)
_MANIFEST_DOCUMENT_FIELDS = frozenset({"schema_id", "profile_id", "roles", "entries"})
_MANIFEST_ENTRY_FIELDS = frozenset(
    {"path", "type", "owner_uid", "mode", "nlink", "size", "sha256"}
)

#: Where a generation keeps its per-instance service manifests. The path is derived, never
#: taken from a unit: the old units interpolated `%i` into
#: `data/runtime/current/manifests/%i.json`, reached through a `lighthouse`-owned symlink.
GENERATION_MANIFEST_DIRECTORY = "manifests"
_COMMIT_SHA_LENGTH = 40
_HEX = "0123456789abcdef"

_FORBIDDEN_BASENAMES = ("sitecustomize.py", "usercustomize.py")
_REQUIRED_PYVENV_LINE = "include-system-site-packages = false"


class RuntimeExecError(RuntimeError):
    """One bounded wrapper rejection. The wrapper never degrades; it refuses."""


def _reject(detail: str) -> RuntimeExecError:
    return RuntimeExecError(detail)


# ---------------------------------------------------------------------------------------
# Strict canonical JSON
# ---------------------------------------------------------------------------------------


def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _reject("a JSON object repeats a key")
        result[key] = value
    return result


def _reject_non_finite(_value: str) -> float:
    raise _reject("a JSON document holds a non-finite number")


def _depth(value: Any, level: int = 0) -> int:
    if level > MAX_JSON_DEPTH:
        raise _reject("a JSON document is nested too deeply")
    if isinstance(value, dict):
        return max((_depth(item, level + 1) for item in value.values()), default=level)
    if isinstance(value, list):
        return max((_depth(item, level + 1) for item in value), default=level)
    return level


def canonical_bytes(value: Any, *, trailing_newline: bool = False) -> bytes:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return payload + b"\n" if trailing_newline else payload


def strict_load(payload: bytes, *, max_bytes: int, label: str) -> Any:
    """Decode strict JSON: bounded, no duplicate keys, no NaN, no control-character abuse."""

    if len(payload) > max_bytes:
        raise _reject(f"the {label} exceeds its bounded size")
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as error:
        raise _reject(f"the {label} is not UTF-8") from error
    try:
        value = json.loads(
            text,
            object_pairs_hook=_no_duplicate_keys,
            parse_constant=_reject_non_finite,
        )
    except json.JSONDecodeError as error:
        raise _reject(f"the {label} is not strict JSON") from error
    _depth(value)
    return value


def strict_canonical_load(payload: bytes, *, max_bytes: int, label: str) -> Any:
    """As `strict_load`, and the bytes must be the document's one canonical spelling."""

    value = strict_load(payload, max_bytes=max_bytes, label=label)
    for candidate in (canonical_bytes(value), canonical_bytes(value, trailing_newline=True)):
        if candidate == payload:
            return value
    raise _reject(f"the {label} is not canonical")


# ---------------------------------------------------------------------------------------
# Anchored, no-follow filesystem reads
# ---------------------------------------------------------------------------------------


def _close_quietly(descriptor: int) -> None:
    with contextlib.suppress(OSError):
        os.close(descriptor)


def _open_anchored_chain(
    trusted_root: str,
    parts: tuple[str, ...],
    *,
    expected_owner_uid: int = OWNER_UID,
) -> list[int]:
    descriptors: list[int] = []
    try:
        descriptors.append(
            os.open(trusted_root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        )
    except OSError as error:
        raise _reject(f"the trusted root {trusted_root} is not an openable directory") from error
    try:
        _require_trusted_directory(
            descriptors[-1], trusted_root, expected_owner_uid=expected_owner_uid
        )
        for name in parts:
            try:
                descriptors.append(
                    os.open(
                        name,
                        os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                        dir_fd=descriptors[-1],
                    )
                )
            except OSError as error:
                raise _reject(f"the ancestor {name!r} is not a canonical directory") from error
            _require_trusted_directory(
                descriptors[-1], name, expected_owner_uid=expected_owner_uid
            )
    except RuntimeExecError:
        for opened in descriptors:
            _close_quietly(opened)
        raise
    return descriptors


def _require_trusted_directory(
    descriptor: int,
    label: str,
    *,
    expected_owner_uid: int = OWNER_UID,
) -> None:
    info = os.fstat(descriptor)
    if not stat.S_ISDIR(info.st_mode):
        raise _reject(f"the ancestor {label!r} is not a directory")
    if info.st_uid != expected_owner_uid:
        raise _reject(f"the ancestor {label!r} is not owned by the expected owner")
    if stat.S_IMODE(info.st_mode) & (stat.S_IWGRP | stat.S_IWOTH):
        raise _reject(f"the ancestor {label!r} is group or world writable")


def _relative_parts(path: str, trusted_root: str) -> tuple[str, ...]:
    if not path.startswith("/") or path != os.path.abspath(path):
        raise _reject(f"{path!r} is not one absolute canonical path")
    root = trusted_root.rstrip("/") or "/"
    if root != "/" and not path.startswith(root + "/"):
        raise _reject(f"{path!r} is outside the trusted root {trusted_root!r}")
    remainder = path[len(root):] if root != "/" else path
    parts = tuple(part for part in remainder.split("/") if part)
    if not parts:
        raise _reject(f"{path!r} is the trusted root itself")
    return parts


def read_root_owned_file(
    path: str,
    *,
    expected_mode: int,
    max_bytes: int,
    trusted_root: str = TRUSTED_ROOT,
    expected_owner_uid: int = OWNER_UID,
    label: str = "file",
) -> bytes:
    """Read a fixed root-owned file, or refuse. Never follows a symlink, ever."""

    parts = _relative_parts(path, trusted_root)
    descriptors = _open_anchored_chain(
        trusted_root, parts[:-1], expected_owner_uid=expected_owner_uid
    )
    try:
        try:
            file_fd = os.open(
                parts[-1],
                os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=descriptors[-1],
            )
        except OSError as error:
            raise _reject(f"the {label} is not an openable regular file") from error
        try:
            info = os.fstat(file_fd)
            if not stat.S_ISREG(info.st_mode):
                raise _reject(f"the {label} is not a regular file")
            if info.st_nlink != 1:
                raise _reject(f"the {label} is not a single link")
            if info.st_uid != expected_owner_uid:
                raise _reject(f"the {label} is not owned by the expected owner")
            if stat.S_IMODE(info.st_mode) != expected_mode:
                raise _reject(f"the {label} mode is not {expected_mode:04o}")
            if info.st_size > max_bytes:
                raise _reject(f"the {label} exceeds its bounded size")
            with os.fdopen(os.dup(file_fd), "rb") as stream:
                return stream.read(max_bytes + 1)[:max_bytes]
        finally:
            _close_quietly(file_fd)
    finally:
        for opened in reversed(descriptors):
            _close_quietly(opened)


def _file_digest(path: str) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with open(path, "rb") as stream:  # noqa: PTH123 - stdlib-only wrapper
        while chunk := stream.read(HASH_CHUNK_BYTES):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


# ---------------------------------------------------------------------------------------
# The profile, the authority record and the generation
# ---------------------------------------------------------------------------------------


def parse_profile(
    payload: bytes,
    *,
    generation_root: str = GENERATION_ROOT,
    runtime_pyz_path: str = RUNTIME_PYZ_PATH,
) -> dict[str, Any]:
    """Decode the fixed profile and prove its `profile_id` matches its own body."""

    data = strict_canonical_load(payload, max_bytes=MAX_PROFILE_BYTES, label="runtime profile")
    if type(data) is not dict or set(data) != _PROFILE_FIELDS:
        raise _reject("the runtime profile schema is invalid")
    body = {key: value for key, value in data.items() if key != "profile_id"}
    expected = hashlib.sha256(canonical_bytes(body)).hexdigest()
    if data["profile_id"] != expected:
        raise _reject("the runtime profile id does not match its content")
    if data["schema_version"] != SCHEMA_VERSION:
        raise _reject("the runtime profile schema version is unsupported")
    if data["generation_root"] != generation_root:
        raise _reject("the runtime profile generation root is not the fixed root")
    runtime_pyz = data["runtime_pyz"]
    if type(runtime_pyz) is not dict or runtime_pyz.get("path") != runtime_pyz_path:
        raise _reject("the runtime profile does not name this wrapper")
    if runtime_pyz.get("mode") != 0o555:
        raise _reject("the runtime profile wrapper mode is not 0555")
    roles = data["roles"]
    if type(roles) is not dict or not roles:
        raise _reject("the runtime profile declares no role")
    return data


def parse_record(payload: bytes) -> dict[str, Any]:
    """Decode `current.json`. `commit` is carried through but never decides anything."""

    data = strict_canonical_load(payload, max_bytes=MAX_RECORD_BYTES, label="authority record")
    if type(data) is not dict or set(data) != _RECORD_FIELDS:
        raise _reject("the authority record schema is invalid")
    if data["schema_version"] != SCHEMA_VERSION:
        raise _reject("the authority record schema version is unsupported")
    if type(data["sequence"]) is not int or data["sequence"] < 1:
        raise _reject("the authority record sequence is invalid")
    if data["state"] not in ("active", "rolled_back"):
        raise _reject("the authority record state is not a running state")
    return data


def current_slot(record: dict[str, Any]) -> dict[str, Any]:
    """The `current_*` slot, and only it. The wrapper never selects `prior_*` itself."""

    slot = {field: record[f"current_{field}"] for field in _SLOT_FIELDS}
    if slot["lifecycle"] != "active":
        raise _reject("the current generation slot is not active")
    for field in ("generation_id", "full_manifest_hash", "profile_id"):
        value = slot[field]
        if type(value) is not str or len(value) != 64:
            raise _reject(f"the current slot {field} is not a digest")
    if slot["generation_id"] != slot["full_manifest_hash"]:
        raise _reject("the generation identity differs from its full manifest hash")
    roles = slot["roles"]
    if type(roles) is not dict or not roles:
        raise _reject("the current slot declares no role")
    for spec in roles.values():
        if type(spec) is not dict or set(spec) != _ROLE_FIELDS:
            raise _reject("a current slot role schema is invalid")
    return slot


def select_role(
    role: str,
    *,
    profile: dict[str, Any],
    slot: dict[str, Any],
    instance: str | None = None,
) -> dict[str, Any]:
    """Bind the unit's literal role to the one mapping both the record and profile declare.

    `instance` is the systemd template label. It never names a path, a manifest or a
    generation: it is looked up in the root-owned profile's per-role instance allowlist and
    is refused unless it is already there. A caller can therefore only choose *among* the
    instances the root-owned policy already authorised, which is what keeps `%i` a label
    rather than an authority value (ruling D-2).
    """

    if type(role) is not str or role not in PROTECTED_ROLES:
        raise _reject("the requested role is not an allowlisted unit-owned literal")
    if role not in slot["roles"]:
        raise _reject("the current generation does not declare the requested role")
    if role not in profile["roles"]:
        raise _reject("the runtime profile does not declare the requested role")
    spec = slot["roles"][role]
    profile_role = profile["roles"][role]
    if type(profile_role) is not dict or spec["module"] != profile_role.get("module"):
        raise _reject("the role module differs between the record and the profile")
    declared = profile_role.get("instances")
    if type(declared) is not list:
        raise _reject("the runtime profile role declares no instance allowlist")
    if declared != sorted(set(declared)) or any(type(item) is not str for item in declared):
        raise _reject("the runtime profile role instance allowlist is not canonical")
    if declared:
        if instance is None:
            raise _reject("the requested role requires an instance label")
        if instance not in declared:
            raise _reject("the requested instance is not in the root-owned allowlist")
    elif instance is not None:
        raise _reject("the requested role accepts no instance label")
    return spec


def generation_paths(slot: dict[str, Any], *, generation_root: str) -> str:
    """The one generation directory this record authorises, derived, never read from a unit."""

    expected = os.path.join(generation_root, slot["generation_id"])
    if slot["generation_path"] != expected:
        raise _reject("the generation path is outside the fixed generation root")
    return expected


def load_generation_manifest(
    *,
    generation_path: str,
    slot: dict[str, Any],
    profile: dict[str, Any],
    trusted_root: str = TRUSTED_ROOT,
    expected_owner_uid: int = OWNER_UID,
) -> tuple[dict[str, Any], ...]:
    """Read the full manifest, hash it against the slot, and return its entries."""

    payload = read_root_owned_file(
        os.path.join(generation_path, GENERATION_MANIFEST_NAME),
        expected_mode=MANIFEST_FILE_MODE,
        max_bytes=MAX_MANIFEST_BYTES,
        trusted_root=trusted_root,
        expected_owner_uid=expected_owner_uid,
        label="generation manifest",
    )
    if hashlib.sha256(payload).hexdigest() != slot["full_manifest_hash"]:
        raise _reject("the generation manifest hash does not match the current slot")
    data = strict_load(payload, max_bytes=MAX_MANIFEST_BYTES, label="generation manifest")
    if type(data) is not dict or set(data) != _MANIFEST_DOCUMENT_FIELDS:
        raise _reject("the generation manifest schema is invalid")
    if data["profile_id"] != profile["profile_id"] or data["profile_id"] != slot["profile_id"]:
        raise _reject("the generation manifest does not match the active profile")
    entries = data["entries"]
    if type(entries) is not list or not entries:
        raise _reject("the generation manifest declares no entry")
    if len(entries) > MAX_MANIFEST_ENTRIES:
        raise _reject("the generation manifest exceeds its bounded entry count")
    parsed: list[dict[str, Any]] = []
    seen: set[str] = set()
    for entry in entries:
        if type(entry) is not dict or set(entry) != _MANIFEST_ENTRY_FIELDS:
            raise _reject("a generation manifest entry schema is invalid")
        relative = entry["path"]
        if type(relative) is not str or not relative or relative.startswith("/"):
            raise _reject("a generation manifest entry path is invalid")
        if ".." in relative.split("/"):
            raise _reject("a generation manifest entry path escapes its generation")
        if relative in seen:
            raise _reject("the generation manifest repeats a path")
        seen.add(relative)
        if entry["type"] not in ("directory", "file"):
            raise _reject("a generation manifest entry type is invalid")
        parsed.append(entry)
    return tuple(parsed)


def verify_code_identity(
    *,
    generation_path: str,
    entries: tuple[dict[str, Any], ...],
    expected_owner_uid: int = OWNER_UID,
) -> None:
    """Every manifested node, on disk, exactly as declared. This is the code identity check."""

    for entry in entries:
        target = os.path.join(generation_path, entry["path"])
        try:
            info = os.lstat(target)
        except OSError as error:
            raise _reject(f"a manifested generation node is missing: {entry['path']}") from error
        if stat.S_ISLNK(info.st_mode):
            raise _reject(f"a manifested generation node is a symlink: {entry['path']}")
        if entry["type"] == "directory":
            if not stat.S_ISDIR(info.st_mode):
                raise _reject(f"a manifested generation node is not a directory: {entry['path']}")
        else:
            if not stat.S_ISREG(info.st_mode):
                raise _reject(
                    f"a manifested generation node is not a regular file: {entry['path']}"
                )
            if info.st_nlink != entry["nlink"]:
                raise _reject(f"a manifested generation node link count changed: {entry['path']}")
            basename = entry["path"].rsplit("/", 1)[-1]
            if basename in _FORBIDDEN_BASENAMES or basename.endswith(".pth"):
                raise _reject(f"the generation holds an import escape: {entry['path']}")
            digest, size = _file_digest(target)
            if digest != entry["sha256"] or size != entry["size"]:
                raise _reject(f"a manifested generation node changed: {entry['path']}")
        if info.st_uid != entry["owner_uid"] or info.st_uid != expected_owner_uid:
            raise _reject(f"a manifested generation node has a foreign owner: {entry['path']}")
        if stat.S_IMODE(info.st_mode) != entry["mode"]:
            raise _reject(f"a manifested generation node mode changed: {entry['path']}")


def verify_pyvenv_configuration(generation_path: str) -> None:
    """`pyvenv.cfg` must switch off system site-packages; nothing else may switch it on."""

    path = os.path.join(generation_path, "pyvenv.cfg")
    try:
        with open(path, encoding="utf-8") as stream:  # noqa: PTH123 - stdlib-only wrapper
            text = stream.read(4096)
    except OSError as error:
        raise _reject("the generation has no readable pyvenv.cfg") from error
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if _REQUIRED_PYVENV_LINE not in lines:
        raise _reject("the generation pyvenv.cfg does not disable system site-packages")


def build_child_environment(
    *,
    profile: dict[str, Any],
    role: str,
    spec: dict[str, Any],
    source_environment: dict[str, str],
) -> dict[str, str]:
    """A fresh environment built from the profile's per-role allowlist, and nothing else.

    `PATH`, every `PYTHON*`, every `LD_*`, user-site controls and caller import paths are
    absent by construction: this dictionary starts empty and only allowlisted names are ever
    copied into it. An `EnvironmentFile` cannot reach it, because the wrapper reads no
    environment file at all.
    """

    allowlist = profile["roles"][role].get("environment_allowlist")
    if type(allowlist) is not list:
        raise _reject("the profile role declares no environment allowlist")
    environment: dict[str, str] = {}
    for name in sorted(set(allowlist)):
        if type(name) is not str or not name.isascii() or not name.isupper():
            raise _reject("a profile environment name is invalid")
        if name.startswith(("PYTHON", "LD_")) or name in ("PATH", "PYTHONPATH", "PYTHONHOME"):
            raise _reject("a profile environment name would override interpreter behaviour")
        value = source_environment.get(name)
        if value is None:
            continue
        if "\n" in value or "\x00" in value:
            raise _reject("a profile environment value holds a control character")
        environment[name] = value
    environment["PWD"] = spec["working_directory"]
    return environment


def derive_module_argv(
    *,
    profile_role: dict[str, Any],
    slot: dict[str, Any],
    instance: str | None,
    generation_path: str,
    manifest_entries: tuple[dict[str, Any], ...],
) -> tuple[str, ...]:
    """Build the module's argv out of the two root-owned documents, and nothing else.

    The units used to carry these values themselves: `--manifest` interpolated `%i` into a
    path under the `current` symlink, and `--expected-commit` / `--expected-generation` came
    from `runtime.env`, a file the application it configures writes. Both are now derived
    here — the manifest from the selected generation and the authorised instance label, the
    generation id from the slot — and the manifest must be a file the generation's own full
    manifest covers, so it is hash-verified along with the rest of the code.

    A role whose policy declares no control root receives no derived argv: there is no
    instance label to resolve a service manifest against. It still receives its own frozen
    `module_arguments`, which are literals of the root-owned profile rather than anything
    derived, and are the only way an arbiter-invoked role such as `workload_admission` names
    the entry it wants. A role with neither is the `daily` HYBRID adapter of `authority.md`
    production mapping, whose declared shape is "caller argv count 0". (The reference is to
    the mapping by name rather than by line: independent review R30-SPEC-04 found the old
    `authority.md L200` had drifted off it.)
    """

    extra = profile_role.get("module_arguments")
    if type(extra) is not list or any(type(item) is not str or not item for item in extra):
        raise _reject("the runtime profile role module arguments are invalid")
    # Independent review R30-SPEC-01, aligned with the environment values above. These are
    # literals of the root-owned profile that become the child's argv verbatim: a newline or
    # NUL splits or truncates it downstream, and `%i` / `${` are systemd and shell expansions
    # that are supposed to have been resolved before anything reaches this file. A profile
    # still carrying one is a profile that was written to be interpolated somewhere else.
    if any(
        "\n" in item or "\x00" in item or "%i" in item or "${" in item for item in extra
    ):
        raise _reject("a runtime profile role module argument holds an unresolved expansion")
    control_root = profile_role.get("control_root")
    if type(control_root) is not str:
        raise _reject("the runtime profile role control root is invalid")
    if not control_root:
        return tuple(extra)
    if not control_root.startswith("/") or ".." in control_root.split("/"):
        raise _reject("the runtime profile role control root is not one absolute path")
    if instance is None:
        raise _reject("a role with a control root requires an instance label")

    relative = f"{GENERATION_MANIFEST_DIRECTORY}/{instance}.json"
    covered = {
        entry["path"]: entry for entry in manifest_entries if entry["type"] == "file"
    }
    if relative not in covered:
        raise _reject(
            "the derived service manifest is not covered by the generation full manifest"
        )
    manifest_path = os.path.join(generation_path, relative)
    if manifest_path != os.path.abspath(manifest_path):
        raise _reject("the derived service manifest path is not canonical")

    commit = slot["commit"]
    if (
        type(commit) is not str
        or len(commit) != _COMMIT_SHA_LENGTH
        or any(character not in _HEX for character in commit)
    ):
        # The wrapper still branches on nothing here: the value is forwarded verbatim to the
        # legacy module, which is the one that compares it. But the module's parser rejects
        # anything that is not a commit sha, so a record carrying audit prose fails closed
        # in the wrapper rather than after the exec.
        raise _reject("the authority record commit is not a forwardable commit sha")

    argv = [
        "--manifest",
        manifest_path,
        "--control-root",
        os.path.join(control_root, instance),
        "--expected-commit",
        commit,
        "--expected-generation",
        slot["generation_id"],
    ]
    service_kind = profile_role.get("service_kind")
    if type(service_kind) is not str:
        raise _reject("the runtime profile role service kind is invalid")
    if service_kind:
        argv.extend(("--expected-kind", service_kind))
    once = profile_role.get("once")
    if type(once) is not bool:
        raise _reject("the runtime profile role once flag is invalid")
    if once:
        argv.append("--once")
    argv.extend(extra)
    return tuple(argv)


def resolve_launch(
    role: str,
    *,
    instance: str | None = None,
    profile_path: str = PROFILE_PATH,
    authority_path: str = AUTHORITY_PATH,
    generation_root: str = GENERATION_ROOT,
    trusted_root: str = TRUSTED_ROOT,
    expected_owner_uid: int = OWNER_UID,
    source_environment: dict[str, str] | None = None,
) -> dict[str, Any]:
    """The complete validated launch: interpreter, module, cwd and environment.

    Both the wrapper and the child bootstrap call this. The child repeats every step from
    the same fixed absolute paths rather than from anything the parent told it, which is what
    `authority.md` L1735-1743 means by an independent repeat.
    """

    profile = parse_profile(
        read_root_owned_file(
            profile_path,
            expected_mode=PROFILE_FILE_MODE,
            max_bytes=MAX_PROFILE_BYTES,
            trusted_root=trusted_root,
            expected_owner_uid=expected_owner_uid,
            label="runtime profile",
        ),
        generation_root=generation_root,
    )
    record = parse_record(
        read_root_owned_file(
            authority_path,
            expected_mode=RECORD_FILE_MODE,
            max_bytes=MAX_RECORD_BYTES,
            trusted_root=trusted_root,
            expected_owner_uid=expected_owner_uid,
            label="authority record",
        )
    )
    slot = current_slot(record)
    if slot["profile_id"] != profile["profile_id"]:
        raise _reject("the current generation was published under another profile")
    spec = select_role(role, profile=profile, slot=slot, instance=instance)
    generation_path = generation_paths(slot, generation_root=generation_root)
    entries = load_generation_manifest(
        generation_path=generation_path,
        slot=slot,
        profile=profile,
        trusted_root=trusted_root,
        expected_owner_uid=expected_owner_uid,
    )
    verify_code_identity(
        generation_path=generation_path,
        entries=entries,
        expected_owner_uid=expected_owner_uid,
    )
    verify_pyvenv_configuration(generation_path)
    for label in ("python_path", "working_directory", "app_source"):
        value = spec[label]
        if type(value) is not str or not value.startswith(generation_path + "/"):
            raise _reject(f"the role {label} is outside the selected generation")
    site_packages = spec["site_packages"]
    if type(site_packages) is not list or not site_packages:
        raise _reject("the role declares no site-packages root")
    for path in site_packages:
        if type(path) is not str or not path.startswith(generation_path + "/"):
            raise _reject("a role site-packages root is outside the selected generation")
    environment = build_child_environment(
        profile=profile,
        role=role,
        spec=spec,
        source_environment=dict(source_environment or {}),
    )
    module_argv = derive_module_argv(
        profile_role=profile["roles"][role],
        slot=slot,
        instance=instance,
        generation_path=generation_path,
        manifest_entries=entries,
    )
    module_source = module_source_relative_path(
        spec["module"],
        app_source=spec["app_source"],
        generation_path=generation_path,
    )
    if module_source not in {
        entry["path"] for entry in entries if entry["type"] == "file"
    }:
        raise _reject("the role module source is not covered by the generation full manifest")
    with open(  # noqa: PTH123 - stdlib-only wrapper
        os.path.join(generation_path, module_source), encoding="utf-8"
    ) as stream:
        assert_module_entry_contract(
            stream.read(MAX_MANIFEST_BYTES),
            expects_argv=bool(module_argv),
        )
    return {
        "role": role,
        "instance": instance,
        "module_argv": module_argv,
        "service_manifest": (
            module_argv[module_argv.index("--manifest") + 1]
            if "--manifest" in module_argv
            else None
        ),
        "generation_id": slot["generation_id"],
        "generation_path": generation_path,
        "profile_id": profile["profile_id"],
        "operation_id": record["operation_id"],
        "sequence": record["sequence"],
        "python_path": spec["python_path"],
        "module": spec["module"],
        "working_directory": spec["working_directory"],
        "app_source": spec["app_source"],
        "site_packages": list(site_packages),
        "module_source": module_source,
        "environment": environment,
    }


def assert_isolated_startup(flags: Any) -> None:
    """Refuse unless this interpreter was started with `-I -S`.

    This is what makes the inherited `sys.path` trustworthy rather than merely present.
    With isolated mode and site processing both off, `PYTHONPATH`, the user site directory,
    the working directory and every `.pth` hook are gone before the first byte of this
    module runs, so the baseline the interpreter contributes is its own standard library and
    nothing else. `child_argv` always passes both flags; this asserts the promise held.
    """

    if not getattr(flags, "isolated", 0):
        raise _reject("the runtime child must run under an isolated interpreter (-I)")
    if not getattr(flags, "no_site", 0):
        raise _reject("the runtime child must run with site processing disabled (-S)")


def child_import_paths(
    launch: dict[str, Any],
    *,
    baseline: Sequence[str],
    interpreter_roots: Sequence[str],
) -> tuple[str, ...]:
    """Insert the manifest-covered generation paths ahead of the interpreter's own.

    `authority.md` L1735-1743 says the bootstrap *inserts* the generation's canonical source
    and site-packages paths. Replacing `sys.path` outright would also delete the standard
    library: the generation is a venv, and a venv holds no `textwrap`, no `lib-dynload`, no
    `_sqlite3`. The child would then die on its first uncached stdlib import.

    So the baseline stays, and it is checked rather than trusted: with `-I -S` in force every
    surviving entry must live under one of the interpreter's own roots (`sys.base_prefix` and
    `sys.base_exec_prefix` — two, because a relocatable build puts `lib-dynload` under the
    second). A checkout path, an application data path or an empty (working-directory) entry
    is refused, not filtered: if one is present the isolation promise has already been broken
    somewhere upstream and continuing would import unverified code.
    """

    roots = tuple(root.rstrip("/") for root in interpreter_roots if root)
    if not roots:
        raise _reject("the runtime child has no interpreter root to anchor its import path")
    generation = launch["generation_path"]
    kept: list[str] = []
    for entry in baseline:
        if not entry:
            raise _reject("the runtime child inherited the working directory on its path")
        if entry == generation or entry.startswith(generation + "/"):
            # Re-added below, in the exact manifest-covered order.
            continue
        if any(entry == root or entry.startswith(root + "/") for root in roots):
            kept.append(entry)
            continue
        raise _reject(f"the runtime child inherited a foreign import path: {entry}")
    return (launch["app_source"], *launch["site_packages"], *kept)


# ---------------------------------------------------------------------------------------
# The module's own entry point
# ---------------------------------------------------------------------------------------


def module_source_relative_path(module: str, *, app_source: str, generation_path: str) -> str:
    """Where the role's module has to live, derived from the manifest-covered import root."""

    if type(module) is not str or not module or module.startswith("."):
        raise _reject("the role module name is invalid")
    parts = module.split(".")
    if any(not part.isidentifier() for part in parts):
        raise _reject("the role module name is invalid")
    prefix = generation_path.rstrip("/") + "/"
    if not app_source.startswith(prefix):
        raise _reject("the role application source is outside the selected generation")
    return "/".join((app_source[len(prefix):].strip("/"), *parts)) + ".py"


def assert_module_entry_contract(source: str, *, expects_argv: bool) -> None:
    """Refuse a module that has no usable entry point, or one that ignores its arguments.

    `runpy.run_module(..., run_name="__main__")` imports the module and returns. A module
    with no `if __name__ == "__main__":` block therefore *succeeds silently* — a oneshot
    unit would report success without having done anything, which is worse than the exit 78
    a missing role produces. A module whose `main` takes only keyword arguments is the same
    hazard one step later: the derived argv is built, handed over, and quietly dropped.

    Both are caught here, statically, from the generation's own manifest-covered source. No
    import happens: the check must not execute generation code in order to decide whether
    generation code may be executed.
    """

    import ast

    try:
        tree = ast.parse(source)
    except SyntaxError as error:
        raise _reject("the role module source does not parse") from error

    entry: ast.FunctionDef | ast.AsyncFunctionDef | None = None
    for node in tree.body:
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and node.name == "main":
            entry = node
    if entry is None:
        raise _reject("the role module defines no main entry point")
    if expects_argv and not (entry.args.posonlyargs or entry.args.args):
        raise _reject("the role module main entry point accepts no positional arguments")

    for node in tree.body:
        if not isinstance(node, ast.If):
            continue
        test = ast.dump(node.test)
        if "__name__" not in test or "__main__" not in test:
            continue
        if any(
            isinstance(inner, ast.Name) and inner.id == "main"
            for inner in ast.walk(node)
        ):
            return
    raise _reject("the role module has no __main__ entry that invokes main")


def child_argv(launch: dict[str, Any], bootstrap: str) -> tuple[str, ...]:
    """The exact child argv of `authority.md` L1729-1734. The role is data, never code."""

    argv = [launch["python_path"], "-I", "-S", "-c", bootstrap, launch["role"]]
    if launch["instance"] is not None:
        argv.append(launch["instance"])
    return tuple(argv)


def child_main(
    role: str,
    instance: str | None = None,
    *,
    profile_path: str = PROFILE_PATH,
    authority_path: str = AUTHORITY_PATH,
    generation_root: str = GENERATION_ROOT,
    trusted_root: str = TRUSTED_ROOT,
    expected_owner_uid: int = OWNER_UID,
) -> int:
    """The frozen bootstrap's entry point, executed inside the generation interpreter.

    It reopens the fixed profile and record and repeats every validation from scratch, then
    inserts only manifest-covered canonical paths inside the current generation and invokes
    the profile-selected module through `runpy`.

    The five keyword arguments are the same injection seam the root verifier's anchors use
    (ruling O5): the offline suite drives this function against a world it built and owns,
    and the frozen trailer — the only production caller — passes nothing, so the fixed
    literals are the only values a production child can ever see.
    """

    import runpy

    assert_isolated_startup(sys.flags)
    baseline = tuple(sys.path)
    launch = resolve_launch(
        role,
        instance=instance,
        profile_path=profile_path,
        authority_path=authority_path,
        generation_root=generation_root,
        trusted_root=trusted_root,
        expected_owner_uid=expected_owner_uid,
        source_environment=dict(os.environ),
    )
    os.chdir(launch["working_directory"])
    sys.path = list(
        child_import_paths(
            launch,
            baseline=baseline,
            interpreter_roots=(sys.base_prefix, sys.base_exec_prefix),
        )
    )
    sys.argv = [launch["module"], *launch["module_argv"]]
    runpy.run_module(launch["module"], run_name="__main__", alter_sys=True)
    return 0


#: The trailer appended to this module's own source to make the child bootstrap. It carries
#: no interpolated value: the role arrives in `argv`, exactly as `authority.md` L1735-1743
#: requires, and no record path, manifest path or environment value is ever spliced in.
CHILD_TRAILER = (
    "\n\nimport sys as _sys\n\n"
    "raise SystemExit(child_main(*_sys.argv[1:3]))\n"
)


def frozen_bootstrap() -> str:
    """This module's own verified source, plus the trailer, as the `-c` payload.

    The source is read back through the import loader rather than off the filesystem, so it
    is the same bytes the already-verified root-owned pyz was built from — `zipimport` serves
    it out of the archive, a checkout run serves it out of the file. The child therefore
    repeats exactly the validation the parent just performed, and there is no second copy of
    it to drift.
    """

    loader = globals().get("__loader__")
    source = None
    if loader is not None and hasattr(loader, "get_source"):
        source = loader.get_source(__name__)
    if source is None:
        with open(__file__, encoding="utf-8") as stream:  # noqa: PTH123 - stdlib-only wrapper
            source = stream.read()
    return source + CHILD_TRAILER
