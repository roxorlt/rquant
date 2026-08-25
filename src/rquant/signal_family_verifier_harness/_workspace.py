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
import stat
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Final

from ._canonical import canonical_sha256

#: Vector paths are declared workspace-relative so the vector bytes never carry the child's
#: randomly named cwd, which would make every result nondeterministic. `@workspace/` names
#: the materialized declaration a surface reads; `@runtime/` names what the production
#: builder is allowed to create and own.
WORKSPACE_PREFIX: Final[str] = "@workspace/"
RUNTIME_PREFIX: Final[str] = "@runtime/"

#: One vector may not materialize an unbounded fixture tree.
MAX_MATERIALIZED_FILES: Final[int] = 512
MAX_MATERIALIZED_FILE_BYTES: Final[int] = 262_144


class WorkspaceError(ValueError):
    """A vector asked the harness to touch something outside its own scratch tree."""


#: SQLite's own scratch files. A store that a production builder opened creates and unlinks
#: `-shm` / `-wal` / `-journal` on its own schedule — a connection being closed by the
#: garbage collector is enough — so they can appear and disappear between the two snapshots
#: without anything having written a byte of durable state. Digesting them would make the
#: read-only verdict, and therefore the canonical result, depend on that timing. The durable
#: database file itself is always digested, so a real write is still caught.
VOLATILE_SUFFIXES: Final[tuple[str, ...]] = ("-journal", "-shm", "-wal")


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

    def __init__(self, root: Path, vector_id: str) -> None:
        resolved = Path(os.getcwd()).resolve() if root is None else Path(root).resolve()
        self.root = resolved / f"vector-{vector_id[:16]}"
        self.state = self.root / "state"
        self.scratch = self.root / "scratch"
        self.runtime = self.root / "runtime"
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
        unknown = set(state) - {"directories", "files", "serving_authorities", "spool"}
        if unknown:
            raise WorkspaceError(f"vector state carries unknown keys: {sorted(unknown)}")
        self._make_directories(state.get("directories", []))
        self._write_files(state.get("files", []))

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


def require_declared_sequence(value: Any, *, field: str) -> Sequence[Any]:
    if type(value) is not list:
        raise WorkspaceError(f"{field} must be a JSON array")
    return value


__all__ = [
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
