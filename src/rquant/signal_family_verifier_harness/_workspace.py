"""The per-vector scratch tree, and the read-only proof that goes with it.

Every vector gets three directories inside the child's own empty cwd:

* `state/` holds exactly what the vector declared, materialized before the builder runs.
* `scratch/` is a byte copy of `state/` handed to surfaces that write, so the pristine
  declaration is provably untouched no matter what the surface does.
* `runtime/` is where the production builder puts what it owns — SQLite files, locks,
  cursors. A read-only surface must leave it alone too.

`tree_digest` is what makes "left alone" checkable: it walks the tree in sorted order and
folds file mode, size, and content hash into one digest. The digests never leave the child
as raw values; only the before/after equality does.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import sqlite3
import stat
from collections.abc import Mapping, Sequence
from contextlib import suppress
from datetime import datetime
from pathlib import Path
from typing import Any, Final

from ._canonical import canonical_sha256
from ._request import AuthorizedGenerationFile

#: Vector paths are declared workspace-relative so the vector bytes never carry the child's
#: randomly named cwd, which would make every result nondeterministic. `@workspace/` names
#: the materialized declaration a surface reads; `@runtime/` names what the production
#: builder is allowed to create and own.
WORKSPACE_PREFIX: Final[str] = "@workspace/"
RUNTIME_PREFIX: Final[str] = "@runtime/"

#: One vector may not materialize an unbounded fixture tree.
MAX_MATERIALIZED_FILES: Final[int] = 512
MAX_MATERIALIZED_FILE_BYTES: Final[int] = 262_144
#: `signal_family_verification.MAX_GENERATION_FIXTURE_BYTES`.
MAX_GENERATION_FIXTURE_BYTES: Final[int] = 1_048_576

#: The exact key set of one `generation_files` declaration and one `sqlite_sources` entry.
_GENERATION_FILE_KEYS: Final[tuple[str, ...]] = (
    "mode",
    "modified_at",
    "path",
    "sha256",
    "source_relative_path",
)
_SQLITE_SOURCE_KEYS: Final[tuple[str, ...]] = ("mode", "path", "script_path")
_STATE_KEYS: Final[frozenset[str]] = frozenset(
    {
        "directories",
        "files",
        "generation_files",
        "serving_authorities",
        "spool",
        "sqlite_sources",
    }
)


class WorkspaceError(ValueError):
    """A vector asked the harness to touch something outside its own scratch tree."""


#: SQLite's shared-memory index. A store a production builder opened creates and unlinks it
#: on its own schedule — a connection closed by the garbage collector is enough — and it
#: carries no content, only the WAL index. Everything that does carry content, including the
#: `-wal` and `-journal` files themselves, stays in the digest, so a read-only surface that
#: wrote a single byte anywhere is still caught.
VOLATILE_SUFFIXES: Final[tuple[str, ...]] = ("-shm",)


def tree_digest(root: Path) -> str:
    """One order-independent digest over the durable regular files the tree holds."""

    if not root.exists():
        return canonical_sha256({"entries": [], "present": False})
    entries: list[dict[str, Any]] = []
    for directory, directory_names, file_names in os.walk(root):
        directory_names.sort()
        for name in sorted(file_names):
            if name.endswith(VOLATILE_SUFFIXES):
                continue
            path = Path(directory) / name
            try:
                observed = path.lstat()
                payload = path.read_bytes() if stat.S_ISREG(observed.st_mode) else b""
            except FileNotFoundError:
                # The same race in its other form: an entry `os.walk` listed is already gone.
                continue
            regular = stat.S_ISREG(observed.st_mode)
            entries.append(
                {
                    "mode": stat.S_IMODE(observed.st_mode),
                    "path": str(path.relative_to(root)),
                    "regular": regular,
                    "sha256": hashlib.sha256(payload).hexdigest() if regular else "",
                    "size": observed.st_size if regular else 0,
                }
            )
    entries.sort(key=lambda entry: entry["path"])
    return canonical_sha256({"entries": entries, "present": True})


class VectorWorkspace:
    """One vector's private corner of the child's cwd."""

    def __init__(
        self,
        root: Path,
        vector_id: str,
        *,
        generation_root: Path | None = None,
        authorized_fixtures: Mapping[str, AuthorizedGenerationFile] | None = None,
    ) -> None:
        resolved = Path(os.getcwd()).resolve() if root is None else Path(root).resolve()
        self.root = resolved / f"vector-{vector_id[:16]}"
        self.state = self.root / "state"
        self.scratch = self.root / "scratch"
        self.runtime = self.root / "runtime"
        self.generation_root = None if generation_root is None else Path(generation_root)
        self.authorized_fixtures: Mapping[str, AuthorizedGenerationFile] = (
            {} if authorized_fixtures is None else dict(authorized_fixtures)
        )
        for directory in (self.root, self.state, self.runtime):
            directory.mkdir(mode=0o700, parents=True, exist_ok=False)

    def live_root(self, *, writes: bool) -> Path:
        """The tree the surface actually runs against: a copy when the surface writes."""

        if not writes:
            return self.state
        if not self.scratch.exists():
            shutil.copytree(self.state, self.scratch)
        return self.scratch

    def _resolve_declared(self, value: str, *, base: Path) -> Path:
        prefix = RUNTIME_PREFIX if value.startswith(RUNTIME_PREFIX) else WORKSPACE_PREFIX
        if not value.startswith(prefix):
            raise WorkspaceError("vector paths must start with @workspace/ or @runtime/")
        relative = value[len(prefix) :]
        if not relative or relative.startswith("/"):
            raise WorkspaceError("vector paths must name something inside the workspace")
        parts = relative.split("/")
        if any(part in {"", ".", ".."} for part in parts):
            raise WorkspaceError("vector paths must not contain dot components")
        candidate = base.joinpath(*parts)
        if base not in candidate.parents:
            raise WorkspaceError("vector paths must stay inside the workspace")
        return candidate

    def declared_path(self, value: str, *, live: Path) -> Path:
        base = self.runtime if value.startswith(RUNTIME_PREFIX) else live
        return self._resolve_declared(value, base=base)

    def rebase(self, value: Any, *, live: Path) -> Any:
        """Rewrite every declared path string into an absolute path in this workspace."""

        if type(value) is str:
            if value.startswith(WORKSPACE_PREFIX) or value.startswith(RUNTIME_PREFIX):
                return str(self.declared_path(value, live=live))
            return value
        if type(value) is list:
            return [self.rebase(item, live=live) for item in value]
        if type(value) is dict:
            return {key: self.rebase(item, live=live) for key, item in value.items()}
        return value

    def materialize(self, state: Mapping[str, Any]) -> None:
        """Create exactly the directories, files, and spool the vector declared."""

        if type(state) is not dict:
            raise WorkspaceError("vector state must be a JSON object")
        unknown = set(state) - _STATE_KEYS
        if unknown:
            raise WorkspaceError(f"vector state carries unknown keys: {sorted(unknown)}")
        self._make_directories(state.get("directories", []))
        self._write_files(state.get("files", []))
        self._copy_generation_files(state.get("generation_files", []))
        self._build_sqlite_sources(state.get("sqlite_sources", []))

    def _make_directories(self, declared: Any) -> None:
        if type(declared) is not list:
            raise WorkspaceError("vector state directories must be an array")
        if len(declared) > MAX_MATERIALIZED_FILES:
            raise WorkspaceError("vector state declares too many directories")
        for entry in declared:
            if type(entry) is not str:
                raise WorkspaceError("each declared directory must be a string path")
            self._resolve_declared(entry, base=self.state).mkdir(mode=0o700, parents=True)

    def _write_files(self, declared: Any) -> None:
        if type(declared) is not list:
            raise WorkspaceError("vector state files must be an array")
        if len(declared) > MAX_MATERIALIZED_FILES:
            raise WorkspaceError("vector state declares too many files")
        for entry in declared:
            if type(entry) is not dict or tuple(sorted(entry)) != ("content", "path"):
                raise WorkspaceError("each declared file must be exactly {content, path}")
            if type(entry["content"]) is not str:
                raise WorkspaceError("declared file content must be a string")
            payload = entry["content"].encode("utf-8")
            if len(payload) > MAX_MATERIALIZED_FILE_BYTES:
                raise WorkspaceError("declared file exceeds its bounded size")
            target = self._resolve_declared(entry["path"], base=self.state)
            target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            target.write_bytes(payload)
            target.chmod(0o600)

    def _copy_generation_files(self, declared: Any) -> None:
        """Copy each authorized in-generation fixture into this vector's own state tree.

        Ruling E-1 is explicit that the child works on a copy: the generation itself is
        read-only root-owned state the run must leave byte-identical, and a production
        builder handed a path inside it would be reading the evidence the root is about to
        re-digest. So every fixture is read once, re-hashed against the *root-authorized*
        tuple rather than against the vector's own claim, and written into `state/` at the
        declared read-only mode and modification time.
        """

        if type(declared) is not list:
            raise WorkspaceError("vector state generation_files must be an array")
        if len(declared) > MAX_MATERIALIZED_FILES:
            raise WorkspaceError("vector state declares too many generation files")
        if declared and self.generation_root is None:
            raise WorkspaceError("this harness run carries no authorized generation root")
        for entry in declared:
            if type(entry) is not dict or tuple(sorted(entry)) != _GENERATION_FILE_KEYS:
                raise WorkspaceError(
                    "each declared generation file must be exactly "
                    "{mode, modified_at, path, sha256, source_relative_path}"
                )
            relative = entry["source_relative_path"]
            if type(relative) is not str:
                raise WorkspaceError("generation fixture source_relative_path must be a string")
            authorized = self.authorized_fixtures.get(relative)
            if authorized is None:
                raise WorkspaceError(
                    "vector names a generation fixture the root did not authorize"
                )
            if entry["sha256"] != authorized.sha256 or entry["mode"] != authorized.mode:
                raise WorkspaceError(
                    "vector fixture declaration disagrees with the authorized fixture"
                )
            assert self.generation_root is not None
            payload = _read_generation_bytes(self.generation_root, relative)
            if (
                hashlib.sha256(payload).hexdigest() != authorized.sha256
                or len(payload) != authorized.size
            ):
                raise WorkspaceError("a generation fixture does not hash to its authorized value")
            target = self._resolve_declared(entry["path"], base=self.state)
            target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            target.write_bytes(payload)
            target.chmod(authorized.mode)
            stamp = _declared_epoch_seconds(entry["modified_at"])
            os.utime(target, (stamp, stamp), follow_symlinks=False)

    def _build_sqlite_sources(self, declared: Any) -> None:
        """Rebuild one SQLite database from a canonical SQL text dump inside the child.

        Ruling E-2: the authoritative form of a producer database fixture is its canonical
        SQL dump, not its page bytes. The dump is what the generation carries and what the
        root hashes, and the database only ever exists inside this vector's own workspace,
        so no SQLite parser is pulled into the root and no page layout has to be reproduced.
        """

        if type(declared) is not list:
            raise WorkspaceError("vector state sqlite_sources must be an array")
        if len(declared) > MAX_MATERIALIZED_FILES:
            raise WorkspaceError("vector state declares too many sqlite sources")
        for entry in declared:
            if type(entry) is not dict or tuple(sorted(entry)) != _SQLITE_SOURCE_KEYS:
                raise WorkspaceError(
                    "each declared sqlite source must be exactly {mode, path, script_path}"
                )
            mode = entry["mode"]
            if type(mode) is not int or type(mode) is bool or not 0 <= mode <= 0o777:
                raise WorkspaceError("declared sqlite source mode must be a POSIX mode")
            script = self._resolve_declared(entry["script_path"], base=self.state)
            target = self._resolve_declared(entry["path"], base=self.state)
            if target.exists():
                raise WorkspaceError("a declared sqlite source would overwrite an existing file")
            try:
                payload = script.read_text(encoding="utf-8")
            except OSError as exc:
                raise WorkspaceError("a declared sqlite source script is unavailable") from exc
            target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            connection = sqlite3.connect(target, isolation_level=None)
            try:
                connection.executescript(payload)
            except sqlite3.Error as exc:
                raise WorkspaceError("a declared sqlite source script is not replayable") from exc
            finally:
                connection.close()
            target.chmod(mode)


def _declared_epoch_seconds(value: Any) -> int:
    """One declared RFC 3339 UTC instant, as whole seconds since the epoch.

    The modification time is part of the declaration because production readers check it:
    `runtime_routing_policy` refuses a frozen policy whose mtime is in the future relative
    to the observation instant, and a file this child just wrote is always newer than the
    vector's frozen `observed_at` unless the vector says what it should be.
    """

    if type(value) is not str:
        raise WorkspaceError("a declared modification time must be an RFC 3339 string")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise WorkspaceError("a declared modification time is not RFC 3339") from exc
    if parsed.tzinfo is None:
        raise WorkspaceError("a declared modification time must carry an offset")
    return int(parsed.timestamp())


def _read_generation_bytes(root: Path, relative: str) -> bytes:
    """Open one generation file with no traversal, no symlink, and a bounded read."""

    parts = relative.split("/")
    if not relative or relative.startswith("/") or any(part in {"", ".", ".."} for part in parts):
        raise WorkspaceError("a generation fixture path must be a normalized relative path")
    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    descriptors: list[int] = []
    try:
        try:
            parent = os.open(root, directory_flags)
        except OSError as exc:
            raise WorkspaceError("the generation root is unavailable to the child") from exc
        descriptors.append(parent)
        for component in parts[:-1]:
            try:
                child = os.open(component, directory_flags, dir_fd=parent)
            except OSError as exc:
                raise WorkspaceError("a generation fixture directory is unavailable") from exc
            descriptors.append(child)
            parent = child
        try:
            descriptor = os.open(
                parts[-1],
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
                dir_fd=parent,
            )
        except OSError as exc:
            raise WorkspaceError("a generation fixture is unavailable") from exc
        descriptors.append(descriptor)
        observed = os.fstat(descriptor)
        if not stat.S_ISREG(observed.st_mode) or observed.st_size > MAX_GENERATION_FIXTURE_BYTES:
            raise WorkspaceError("a generation fixture is not a bounded regular file")
        payload = b""
        while chunk := os.read(descriptor, 65536):
            payload += chunk
            if len(payload) > MAX_GENERATION_FIXTURE_BYTES:
                raise WorkspaceError("a generation fixture is oversized")
        return payload
    finally:
        for descriptor in reversed(descriptors):
            with suppress(OSError):  # pragma: no cover - descriptors are freshly opened
                os.close(descriptor)


def require_declared_sequence(value: Any, *, field: str) -> Sequence[Any]:
    if type(value) is not list:
        raise WorkspaceError(f"{field} must be a JSON array")
    return value


__all__ = [
    "MAX_GENERATION_FIXTURE_BYTES",
    "MAX_MATERIALIZED_FILES",
    "VOLATILE_SUFFIXES",
    "MAX_MATERIALIZED_FILE_BYTES",
    "RUNTIME_PREFIX",
    "WORKSPACE_PREFIX",
    "VectorWorkspace",
    "WorkspaceError",
    "require_declared_sequence",
    "tree_digest",
]
