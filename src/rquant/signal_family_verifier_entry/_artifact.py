"""Content addressing, install-location policy and startup verification for the artifact.

The artifact is a pair: a directory tree whose every file is root-owned and read-only, and
a fixed entry pyz that carries the tree's manifest frozen inside it. The tree's identity is
the SHA-256 of that manifest's canonical bytes, so the pair is self-checking — the entry
knows exactly which bytes it expects to find, at exactly which absolute path, owned by
exactly whom.

Two properties are load-bearing:

* Nothing in the running process may import from anywhere but the verified tree. The pyz
  runs under `-I -S`, so `PYTHONPATH`, user site and the current directory are already out;
  what remains is checked explicitly, both before the import (`sys.path`) and after it
  (`module.__file__`).
* The expected owner is a parameter, not a literal. Production passes `0`; the offline suite
  passes its own uid so the same predicates run without root. There is no environment
  variable or flag that reaches this decision — the production bootstrap is the only caller
  that supplies the production values.

This module is stdlib-only: it runs before any site-packages directory exists on the path.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Final, Literal

#: The content-addressed install root. Each generation of the artifact lives under its own
#: content id, so installing a new one never overwrites the old and rollback is a matter of
#: pointing the entry at the previous id.
ARTIFACT_INSTALL_ROOT: Final[Path] = Path("/usr/local/lib/rquant-signal-family-verifier")
#: The fixed entry the operator invokes. It is not a symlink into the tree: it is a regular
#: root-owned archive whose frozen manifest names the tree it belongs to.
ARTIFACT_ENTRY_PATH: Final[Path] = Path(
    "/usr/local/libexec/rquant-signal-family-verifier-v1.pyz"
)

MANIFEST_SCHEMA_ID: Final[str] = "rquant-signal-family-verifier-artifact/v1"

TREE_DIRECTORY_MODE: Final[int] = 0o555
TREE_FILE_MODE: Final[int] = 0o444
TREE_EXECUTABLE_MODE: Final[int] = 0o555
ENTRY_FILE_MODE: Final[int] = 0o555

SITE_PACKAGES_RELATIVE: Final[PurePosixPath] = PurePosixPath("lib/python3.11/site-packages")

MAX_TREE_ENTRIES: Final[int] = 20_000
MAX_TREE_FILE_BYTES: Final[int] = 64 * 1024 * 1024
_HASH_CHUNK_BYTES: Final[int] = 65536
_MANIFEST_FIELDS: Final[frozenset[str]] = frozenset(
    {"relative_path", "type", "mode", "sha256", "size"}
)
_FORBIDDEN_NAMES: Final[tuple[str, ...]] = ("sitecustomize.py", "usercustomize.py")


class VerifierArtifactError(RuntimeError):
    """One bounded artifact rejection. There is no partial start."""


@dataclass(frozen=True)
class TreeEntry:
    """One node of the installed tree, bound to its exact identity."""

    relative_path: str
    entry_type: Literal["directory", "file"]
    mode: int
    sha256: str | None
    size: int | None

    def payload(self) -> dict[str, Any]:
        return {
            "relative_path": self.relative_path,
            "type": self.entry_type,
            "mode": self.mode,
            "sha256": self.sha256,
            "size": self.size,
        }


@dataclass(frozen=True)
class InstallPlan:
    """Every target the root installation transaction is allowed to write, and how."""

    tree_root: Path
    entry_path: Path
    owner_uid: int
    owner_gid: int
    directory_mode: int
    file_mode: int
    executable_mode: int
    entry_mode: int


def _reject(detail: str) -> VerifierArtifactError:
    return VerifierArtifactError(detail)


def _require_content_id(value: str) -> str:
    if type(value) is not str or len(value) != 64:
        raise _reject("the artifact content id must be one 64 character digest")
    if any(character not in "0123456789abcdef" for character in value):
        raise _reject("the artifact content id must be one lowercase hex digest")
    return value


def install_plan(
    *,
    content_id: str,
    install_root: Path = ARTIFACT_INSTALL_ROOT,
    entry_path: Path = ARTIFACT_ENTRY_PATH,
    owner_uid: int = 0,
    owner_gid: int = 0,
) -> InstallPlan:
    """The exact paths, owner and modes an installation must produce. It installs nothing."""

    identifier = _require_content_id(content_id)
    return InstallPlan(
        tree_root=install_root / identifier,
        entry_path=entry_path,
        owner_uid=owner_uid,
        owner_gid=owner_gid,
        directory_mode=TREE_DIRECTORY_MODE,
        file_mode=TREE_FILE_MODE,
        executable_mode=TREE_EXECUTABLE_MODE,
        entry_mode=ENTRY_FILE_MODE,
    )


def import_root(tree_root: Path) -> Path:
    """The single directory the entry is allowed to put on `sys.path`."""

    return tree_root / SITE_PACKAGES_RELATIVE


def freeze_tree_modes(root: Path) -> None:
    """Set the read-only tree modes an installed artifact must present.

    The build applies this before hashing so the manifest records the installed modes rather
    than whatever the source venv happened to carry.
    """

    for path in sorted(root.rglob("*"), reverse=True):
        if path.is_symlink():
            raise _reject(f"the artifact tree must hold no symbolic link: {path}")
        if path.is_dir():
            path.chmod(TREE_DIRECTORY_MODE)
        else:
            executable = bool(path.stat().st_mode & stat.S_IXUSR)
            path.chmod(TREE_EXECUTABLE_MODE if executable else TREE_FILE_MODE)
    root.chmod(TREE_DIRECTORY_MODE)


def relocate_frozen_tree(source: Path, target: Path) -> None:
    """Move a frozen tree, atomically, without loosening any node beneath its root.

    Renaming a directory rewrites its own `..` entry, so a `0555` root cannot be moved as
    it stands. Only the root is briefly made writable; every node beneath it keeps the mode
    the manifest recorded, and the root is refrozen before the function returns.
    """

    if target.exists():
        raise _reject(f"the artifact target already exists: {target}")
    source.chmod(0o700)
    source.replace(target)
    target.chmod(TREE_DIRECTORY_MODE)


def remove_frozen_tree(root: Path) -> None:
    """Remove a frozen tree, restoring directory write permission on the way down."""

    for path in sorted(root.rglob("*"), reverse=True):
        if path.is_dir() and not path.is_symlink():
            path.chmod(0o755)
    root.chmod(0o755)
    shutil.rmtree(root)


def _file_digest(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        while chunk := stream.read(_HASH_CHUNK_BYTES):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def build_tree_manifest(root: Path) -> tuple[TreeEntry, ...]:
    """Every node beneath `root`, sorted, with its exact type, mode and content digest."""

    if not root.is_dir() or root.is_symlink():
        raise _reject("the artifact tree root is not a directory")
    entries: list[TreeEntry] = []
    for path in sorted(root.rglob("*"), key=lambda item: item.relative_to(root).as_posix()):
        relative = path.relative_to(root).as_posix()
        if path.is_symlink():
            raise _reject(f"the artifact tree must hold no symbolic link: {relative}")
        mode = stat.S_IMODE(path.stat().st_mode)
        if path.is_dir():
            entries.append(
                TreeEntry(
                    relative_path=relative,
                    entry_type="directory",
                    mode=mode,
                    sha256=None,
                    size=None,
                )
            )
            continue
        if not path.is_file():
            raise _reject(f"the artifact tree must hold only regular files: {relative}")
        digest, size = _file_digest(path)
        if size > MAX_TREE_FILE_BYTES:
            raise _reject(f"the artifact tree file exceeds its bounded size: {relative}")
        entries.append(
            TreeEntry(
                relative_path=relative,
                entry_type="file",
                mode=mode,
                sha256=digest,
                size=size,
            )
        )
    if len(entries) > MAX_TREE_ENTRIES:
        raise _reject("the artifact tree exceeds its bounded entry count")
    if not entries:
        raise _reject("the artifact tree is empty")
    return tuple(entries)


def canonical_manifest_bytes(entries: tuple[TreeEntry, ...]) -> bytes:
    """The manifest's one canonical serialization. The content id is its digest."""

    payload = {
        "schema_id": MANIFEST_SCHEMA_ID,
        "entries": [entry.payload() for entry in entries],
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def content_id(entries: tuple[TreeEntry, ...]) -> str:
    return hashlib.sha256(canonical_manifest_bytes(entries)).hexdigest()


def parse_manifest(payload: bytes) -> tuple[TreeEntry, ...]:
    """Strictly decode a frozen manifest, refusing anything but its canonical bytes."""

    try:
        data = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise _reject("the artifact manifest is not strict JSON") from error
    if type(data) is not dict or set(data) != {"schema_id", "entries"}:
        raise _reject("the artifact manifest schema is invalid")
    if data["schema_id"] != MANIFEST_SCHEMA_ID:
        raise _reject("the artifact manifest schema is unsupported")
    rows = data["entries"]
    if type(rows) is not list or not rows:
        raise _reject("the artifact manifest entries are invalid")
    entries: list[TreeEntry] = []
    for row in rows:
        if type(row) is not dict or set(row) != _MANIFEST_FIELDS:
            raise _reject("an artifact manifest entry schema is invalid")
        relative = row["relative_path"]
        kind = row["type"]
        mode = row["mode"]
        if type(relative) is not str or not relative or relative.startswith("/"):
            raise _reject("an artifact manifest entry path is invalid")
        if ".." in PurePosixPath(relative).parts:
            raise _reject("an artifact manifest entry path escapes the tree")
        if kind not in ("directory", "file") or type(mode) is not int:
            raise _reject("an artifact manifest entry type or mode is invalid")
        entries.append(
            TreeEntry(
                relative_path=relative,
                entry_type=kind,
                mode=mode,
                sha256=row["sha256"],
                size=row["size"],
            )
        )
    paths = [entry.relative_path for entry in entries]
    if paths != sorted(set(paths)):
        raise _reject("the artifact manifest entries are not canonical")
    frozen = tuple(entries)
    if canonical_manifest_bytes(frozen) != payload:
        raise _reject("the artifact manifest bytes are not canonical")
    return frozen


def _require_private_ancestry(root: Path, expected_owner_uid: int) -> None:
    for ancestor in (root, *root.parents):
        try:
            info = ancestor.lstat()
        except OSError as error:
            raise _reject(f"the artifact ancestor {ancestor} is unreadable") from error
        if stat.S_ISLNK(info.st_mode):
            raise _reject(f"the artifact ancestor {ancestor} is a symbolic link")
        if not stat.S_ISDIR(info.st_mode):
            raise _reject(f"the artifact ancestor {ancestor} is not a directory")
        if stat.S_IMODE(info.st_mode) & (stat.S_IWGRP | stat.S_IWOTH):
            raise _reject(f"the artifact ancestor {ancestor} is group or world writable")
        # An ancestor may be root even when the artifact itself is not: root can always
        # replace it anyway, so root ownership adds no reachable authority. Any *other*
        # foreign owner could chmod the directory writable and swap the tree underneath.
        if ancestor != root and info.st_uid not in (expected_owner_uid, 0):
            raise _reject(f"the artifact ancestor {ancestor} has an unexpected owner")


def verify_installed_tree(
    tree_root: Path,
    *,
    manifest: tuple[TreeEntry, ...],
    expected_content_id: str,
    expected_owner_uid: int,
    expected_owner_gid: int,
) -> None:
    """Refuse to start unless the installed tree is exactly the manifest, byte for byte."""

    _require_content_id(expected_content_id)
    if content_id(manifest) != expected_content_id:
        raise _reject("the frozen manifest does not match its content id")
    if not tree_root.is_absolute():
        raise _reject("the artifact tree root must be one absolute path")
    _require_private_ancestry(tree_root, expected_owner_uid)
    root_info = tree_root.lstat()
    if root_info.st_uid != expected_owner_uid or root_info.st_gid != expected_owner_gid:
        raise _reject("the artifact tree root has an unexpected owner")
    if stat.S_IMODE(root_info.st_mode) != TREE_DIRECTORY_MODE:
        raise _reject("the artifact tree root mode is not the installed mode")

    expected = {entry.relative_path: entry for entry in manifest}
    observed: set[str] = set()
    for path in tree_root.rglob("*"):
        relative = path.relative_to(tree_root).as_posix()
        observed.add(relative)
        entry = expected.get(relative)
        if entry is None:
            raise _reject(f"the artifact tree holds an unmanifested node: {relative}")
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode):
            raise _reject(f"the artifact tree node is not a regular file: {relative}")
        if entry.entry_type == "directory":
            if not stat.S_ISDIR(info.st_mode):
                raise _reject(f"the artifact tree node is not a directory: {relative}")
        elif not stat.S_ISREG(info.st_mode):
            raise _reject(f"the artifact tree node is not a regular file: {relative}")
        elif info.st_nlink != 1:
            raise _reject(f"the artifact tree node is not a single link: {relative}")
        if stat.S_IMODE(info.st_mode) != entry.mode:
            raise _reject(f"the artifact tree node mode changed: {relative}")
        if info.st_uid != expected_owner_uid or info.st_gid != expected_owner_gid:
            raise _reject(f"the artifact tree node has an unexpected owner: {relative}")
        if entry.entry_type == "file":
            if path.name in _FORBIDDEN_NAMES or path.name.endswith(".pth"):
                raise _reject(f"the artifact tree holds an import escape: {relative}")
            digest, size = _file_digest(path)
            if digest != entry.sha256 or size != entry.size:
                raise _reject(f"the artifact tree node hash changed: {relative}")
    missing = sorted(set(expected) - observed)
    if missing:
        raise _reject(f"the artifact tree is missing a manifested node: {missing[0]}")


def assert_isolated_startup(flags: Any) -> None:
    """Refuse unless the interpreter was started with `-I -S`.

    This is what makes the baseline `sys.path` trustworthy: with isolated mode and site
    processing both off, the interpreter contributes only its own standard library and the
    archive it was handed. `PYTHONPATH`, the user site directory, the working directory and
    every `.pth` hook are already gone before the first byte of this module runs, so the
    only thing left to police is what gets *added*.
    """

    if not getattr(flags, "isolated", 0):
        raise _reject("the verifier entry must run under an isolated interpreter (-I)")
    if not getattr(flags, "no_site", 0):
        raise _reject("the verifier entry must run with site processing disabled (-S)")


def assert_import_paths_are_confined(
    paths: list[str],
    *,
    tree_root: Path,
    entry_path: Path,
    interpreter_baseline: Sequence[str] = (),
) -> None:
    """Every `sys.path` entry must be the archive, the verified tree, or the `-I -S` baseline.

    `interpreter_baseline` is the path the isolated interpreter produced before anything was
    added to it. Production passes exactly that snapshot; the offline suite passes nothing,
    which is the strictest form and the one the checkout-refusal tests use.
    """

    allowed_root = import_root(tree_root)
    baseline = frozenset(interpreter_baseline)
    for raw in paths:
        if not raw:
            raise _reject("an import path entry is the empty current directory")
        if raw in baseline:
            continue
        candidate = Path(raw)
        if candidate == entry_path or candidate.is_relative_to(allowed_root):
            continue
        raise _reject(f"an import path entry is outside the verified artifact: {raw}")


def assert_module_is_from_tree(*, module_file: Path | str | None, tree_root: Path) -> None:
    """After the import, prove the module actually came out of the verified tree."""

    if module_file is None:
        raise _reject("an imported verifier module has no file of origin")
    resolved = Path(module_file)
    if not resolved.is_relative_to(import_root(tree_root)):
        raise _reject(f"an imported module is outside the verified artifact: {resolved}")


def relative_entry_paths(manifest: tuple[TreeEntry, ...]) -> tuple[str, ...]:
    return tuple(entry.relative_path for entry in manifest)


def current_process_owner() -> tuple[int, int]:
    return os.geteuid(), os.getegid()


__all__ = [
    "ARTIFACT_ENTRY_PATH",
    "ARTIFACT_INSTALL_ROOT",
    "ENTRY_FILE_MODE",
    "MANIFEST_SCHEMA_ID",
    "MAX_TREE_ENTRIES",
    "MAX_TREE_FILE_BYTES",
    "SITE_PACKAGES_RELATIVE",
    "TREE_DIRECTORY_MODE",
    "TREE_EXECUTABLE_MODE",
    "TREE_FILE_MODE",
    "InstallPlan",
    "TreeEntry",
    "VerifierArtifactError",
    "assert_import_paths_are_confined",
    "assert_module_is_from_tree",
    "build_tree_manifest",
    "canonical_manifest_bytes",
    "content_id",
    "current_process_owner",
    "freeze_tree_modes",
    "import_root",
    "install_plan",
    "parse_manifest",
    "relocate_frozen_tree",
    "remove_frozen_tree",
    "relative_entry_paths",
    "assert_isolated_startup",
    "verify_installed_tree",
]
