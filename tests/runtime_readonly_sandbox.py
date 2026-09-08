"""The systemd sandbox the isolated runtime units run under, for a test process.

`rquant-runtime-strategy@.service` is `ProtectSystem=strict` plus
`ReadWritePaths=…/control/strategies/%i …/live/strategies/%i …/control/schema-rollouts`,
which systemd implements as a mount namespace: the whole tree is bind-mounted read-only
and those three subtrees are mounted back read-write. Every write outside them fails with
`EROFS`, which is how one strategy died on

    OSError: [Errno 30] Read-only file system:
        '/home/lighthouse/rquant/data/runtime/live/features/.feature-spool.lock'

A test process cannot create a mount namespace without privileges, so `readonly_runtime`
denies the same writes at the syscall wrappers instead, with the same errno and the same
message. It is a simulation of the mechanism, not of the outcome: the outcome — that the
producer's directory is byte-for-byte unchanged — is checked separately by `tree_state`,
which also covers the few writes that could reach the filesystem through a descriptor
this wrapper never sees.

`chmod`-based sandboxes are not usable here: `FeatureBatchSpool` refuses a producer root
whose mode is not 0700, exactly as it does on the host, where the mode stays 0700 and the
*mount* is what denies the write.
"""

from __future__ import annotations

import builtins
import errno
import io
import os
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

#: `os` functions that change something the sandbox may be protecting.
_WRITE_FUNCTIONS = (
    "mkdir",
    "makedirs",
    "rmdir",
    "remove",
    "unlink",
    "chmod",
    "chown",
    "utime",
    "truncate",
    "mknod",
    "mkfifo",
)
#: `os` functions that move an entry: both ends change.
_RENAME_FUNCTIONS = ("rename", "replace")
#: `os` functions whose *second* argument is the entry being created.
_DESTINATION_FUNCTIONS = ("symlink", "link")
_WRITE_FLAGS = os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND
#: `os` functions that merely look; only an `InaccessiblePaths=` entry denies these
_READ_FUNCTIONS = ("stat", "lstat", "listdir", "scandir", "readlink", "access")


@dataclass(frozen=True)
class SandboxViolation:
    """One write the sandbox refused, in the order the process attempted it."""

    operation: str
    path: str


class _ReadOnlyRuntime:
    def __init__(
        self,
        root: Path,
        writable: Sequence[Path],
        inaccessible: Sequence[Path] = (),
        outside_exempt: Sequence[Path] = (),
    ) -> None:
        self.root = Path(os.path.abspath(root))
        self.writable = tuple(Path(os.path.abspath(path)) for path in writable)
        #: `InaccessiblePaths=` in the unit. systemd over-mounts an empty, permission-less
        #: node there, so the path is not merely unwritable: reading it is `EACCES` too.
        #: The one every runtime unit carries is `.env`, and a role that reads it is
        #: reading a secret its own generation did not hand it.
        self.inaccessible = tuple(Path(os.path.abspath(path)) for path in inaccessible)
        self.violations: list[SandboxViolation] = []
        #: writes the host would also refuse, outside `root`. Recorded, never refused.
        self.outside: list[SandboxViolation] = []
        self.outside_exempt = tuple(Path(os.path.abspath(path)) for path in outside_exempt)

    def _hidden(self, target: object) -> Path | None:
        if isinstance(target, int) or not self.inaccessible:
            return None
        try:
            candidate = Path(os.path.abspath(os.fspath(target)))
        except TypeError:
            return None
        for hidden in self.inaccessible:
            if candidate == hidden or hidden in candidate.parents:
                return candidate
        return None

    def refuse_access(self, operation: str, target: object) -> None:
        hidden = self._hidden(target)
        if hidden is None:
            return
        self.violations.append(SandboxViolation(operation=operation, path=str(hidden)))
        raise OSError(errno.EACCES, "Permission denied", str(hidden))

    def _protects(self, target: object) -> Path | None:
        if isinstance(target, int):
            #: a descriptor-relative write; `tree_state` is what covers these
            return None
        try:
            candidate = Path(os.path.abspath(os.fspath(target)))
        except TypeError:
            return None
        if candidate != self.root and self.root not in candidate.parents:
            #: `ProtectSystem=strict` makes the *whole* filesystem read-only, not just the
            #: runtime root, so a write to `/home/lighthouse/rquant/logs`, `/var/lib/rquant`
            #: or anywhere else is `EROFS` on a host too. `outside` records those without
            #: refusing them, because a test process legitimately writes to its own pytest
            #: temporary directory, its venv and `TMPDIR`; a caller that wants the host's
            #: answer for a particular tree passes it in `root` or reads `outside`.
            for exempt in self.outside_exempt:
                if candidate == exempt or exempt in candidate.parents:
                    return None
            self.outside.append(SandboxViolation(operation="write", path=str(candidate)))
            return None
        for writable in self.writable:
            if candidate == writable or writable in candidate.parents:
                return None
        return candidate

    def refuse(self, operation: str, target: object) -> None:
        self.refuse_access(operation, target)
        protected = self._protects(target)
        if protected is None:
            return
        if operation in {"mkdir", "makedirs"} and protected.exists():
            # `mkdir` over an entry that is already there answers `EEXIST`, not `EROFS`,
            # and `Path.mkdir(exist_ok=True)` swallows that. The host proved the order:
            # the strategy under the real sandbox reached
            # `live/features/.feature-spool.lock` (#231), which it could only do after
            # `_ensure_private_directories` had "created" the three directories that
            # were already there.
            return
        self.violations.append(SandboxViolation(operation=operation, path=str(protected)))
        raise OSError(errno.EROFS, "Read-only file system", str(protected))


@contextmanager
def readonly_runtime(
    root: Path,
    *,
    writable: Sequence[Path] = (),
    inaccessible: Sequence[Path] = (),
    outside_exempt: Sequence[Path] = (),
    outside: list[SandboxViolation] | None = None,
) -> Iterator[list[SandboxViolation]]:
    """Make everything under `root` read-only except `writable`, as the unit does.

    `inaccessible` is the unit's `InaccessiblePaths=`, which is a stronger denial than
    the read-only default: reads of those paths fail too, wherever they live -- every
    runtime unit hides `/home/lighthouse/rquant/.env` that way, and that path is outside
    the runtime root, so it is checked against the list rather than against `root`.

    Writes *outside* `root` are recorded in `outside` rather than refused. On a host
    `ProtectSystem=strict` refuses those too, but a test process has to write to its own
    pytest temporary directory and venv, so the honest thing is to surface them and let
    the caller decide, not to pretend this simulation is the host's mount namespace.
    """

    guard = _ReadOnlyRuntime(root, writable, inaccessible, outside_exempt)
    if outside is not None:
        #: the caller's list, so a test can see the writes the host would refuse outside
        #: `root` without this simulation having to refuse them here
        guard.outside = outside
    originals: dict[str, object] = {}

    def install(name: str, replacement: object) -> None:
        originals[name] = getattr(os, name)
        setattr(os, name, replacement)

    for name in _WRITE_FUNCTIONS:
        if not hasattr(os, name):
            continue
        original = getattr(os, name)

        def guarded(path, *args, _original=original, _name=name, **kwargs):  # type: ignore[no-untyped-def]
            guard.refuse(_name, path)
            return _original(path, *args, **kwargs)

        install(name, guarded)

    for name in _RENAME_FUNCTIONS:
        original = getattr(os, name)

        def guarded_rename(src, dst, *args, _original=original, _name=name, **kwargs):  # type: ignore[no-untyped-def]
            guard.refuse(_name, dst)
            guard.refuse(_name, src)
            return _original(src, dst, *args, **kwargs)

        install(name, guarded_rename)

    for name in _DESTINATION_FUNCTIONS:
        original = getattr(os, name)

        def guarded_destination(src, dst, *args, _original=original, _name=name, **kwargs):  # type: ignore[no-untyped-def]
            guard.refuse(_name, dst)
            return _original(src, dst, *args, **kwargs)

        install(name, guarded_destination)

    original_open = os.open

    def guarded_open(path, flags, *args, **kwargs):  # type: ignore[no-untyped-def]
        guard.refuse_access("open", path)
        if flags & _WRITE_FLAGS:
            guard.refuse("open", path)
        return original_open(path, flags, *args, **kwargs)

    install("open", guarded_open)

    for name in _READ_FUNCTIONS:
        if not hasattr(os, name):
            continue
        original_read = getattr(os, name)

        def guarded_read(path, *args, _original=original_read, _name=name, **kwargs):  # type: ignore[no-untyped-def]
            guard.refuse_access(_name, path)
            return _original(path, *args, **kwargs)

        install(name, guarded_read)

    original_io_open = io.open
    original_builtin_open = builtins.open

    def guarded_io_open(file, mode="r", *args, **kwargs):  # type: ignore[no-untyped-def]
        guard.refuse_access("io.open", file)
        if any(character in mode for character in "wxa+"):
            guard.refuse("io.open", file)
        return original_io_open(file, mode, *args, **kwargs)

    io.open = guarded_io_open  # type: ignore[assignment]
    builtins.open = guarded_io_open  # type: ignore[assignment]
    try:
        yield guard.violations
    finally:
        for name, original_function in originals.items():
            setattr(os, name, original_function)
        io.open = original_io_open  # type: ignore[assignment]
        builtins.open = original_builtin_open  # type: ignore[assignment]


def tree_state(root: Path) -> tuple[tuple[str, int, int, int], ...]:
    """Name, mode, size and modification time of everything under `root`.

    Compared before and after a role runs, this is the end-state proof that the role
    wrote nothing in a directory it does not own — independent of which syscall wrapper
    the write would have gone through.
    """

    entries = []
    for path in sorted(root.rglob("*")):
        observed = path.lstat()
        entries.append(
            (
                str(path.relative_to(root)),
                observed.st_mode,
                observed.st_size,
                observed.st_mtime_ns,
            )
        )
    return tuple(entries)


__all__ = ["SandboxViolation", "readonly_runtime", "tree_state"]
