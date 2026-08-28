"""The narrow root-owned launcher that performs every production privilege drop.

Codex round-2 P1-5 removes the Python `preexec_fn` from the production paths. Anything
that runs between `fork` and `exec` runs in a process that holds a copy of the parent's
allocator and GIL state without the parent's threads, so a single allocation inside it can
deadlock the child forever; `authority.md` L1841-1843 already records that risk as the
reason the superseded implementation's `preexec_fn` had to go.

The replacement is `/usr/bin/setpriv` from util-linux: one root-owned binary, invoked with
an exact frozen flag sequence, that changes identity, clears the supplementary groups and
sets `no-new-privs` in C before executing the target. It is TCB, so it is identified the
same way every other TCB file is — regular, single-link, expected owner, no group/world
write bit, owner-executable, non-writable ancestry, optional SHA-256 pin.

Descriptor closure is no longer this repository's job either: `subprocess` already sweeps
every inherited descriptor except `pass_fds` in `_posixsubprocess`'s C helper, after the
point where a `preexec_fn` would have run. What survives here is the arithmetic that used
to drive the sweep, kept as a pure assertion so a launch whose retained set disagrees with
the descriptors actually passed fails closed instead of silently leaking one.

This module is stdlib-only on purpose: it is imported by the root verifier, whose whole
import closure is part of the root-owned verifier artifact.
"""

from __future__ import annotations

import hashlib
import os
import stat
from collections.abc import Sequence
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Final

#: The fixed util-linux launcher. There is no environment variable, flag, or configuration
#: file that can move it; a caller that wants another path passes it explicitly and owns
#: the consequences, which is what the offline suite does and production never does.
PRODUCTION_PRIVILEGE_LAUNCHER: Final[Path] = Path("/usr/bin/setpriv")

#: The exact flags a privilege drop uses, in order. `--clear-groups` removes every
#: supplementary group; `--no-new-privs` makes the drop irreversible across the exec.
PRIVILEGE_DROP_FLAGS: Final[tuple[str, ...]] = (
    "--reuid",
    "--regid",
    "--clear-groups",
    "--no-new-privs",
)

#: The parent-death signal the workload arbiter needs. `setpriv --pdeathsig` performs the
#: same `PR_SET_PDEATHSIG` the arbiter used to reach through a foreign-function call
#: inside its `preexec` callable.
PARENT_DEATH_SIGNAL_NAME: Final[str] = "SIGKILL"

_FILE_HASH_CHUNK_BYTES: Final[int] = 65536
_MAX_LAUNCHER_BYTES: Final[int] = 8 * 1024 * 1024


class PrivilegeLauncherError(RuntimeError):
    """One bounded launcher rejection. Nothing here is recoverable in place."""


@dataclass(frozen=True)
class TrustedFileEntry:
    """One TCB file, in the field shape `authority.md` L162-170 freezes for the profile."""

    path: Path
    owner_uid: int
    owner_gid: int
    exact_mode: int | None
    forbidden_mode_bits: int
    required_mode_bits: int
    description: str


#: The launcher's Trusted Computing Base entry. `setpriv` has no fixed mode across
#: distributions (`0755` on OpenCloudOS, `0555` elsewhere), so the entry pins the bits that
#: matter — no group or world write, owner-executable — rather than an exact mode.
PRIVILEGE_LAUNCHER_TCB_ENTRY: Final[TrustedFileEntry] = TrustedFileEntry(
    path=PRODUCTION_PRIVILEGE_LAUNCHER,
    owner_uid=0,
    owner_gid=0,
    exact_mode=None,
    forbidden_mode_bits=stat.S_IWGRP | stat.S_IWOTH,
    required_mode_bits=stat.S_IXUSR,
    description="util-linux setpriv: the narrow root-owned privilege-drop launcher",
)


@dataclass(frozen=True)
class LauncherIdentity:
    """What the launcher actually presented, read once through an anchored no-follow walk."""

    path: Path
    sha256: str
    mode: int
    owner_uid: int
    owner_gid: int
    size: int


def _reject(detail: str) -> PrivilegeLauncherError:
    return PrivilegeLauncherError(detail)


def _require_absolute(path: Path, label: str) -> Path:
    if not isinstance(path, Path) or not path.is_absolute():
        raise _reject(f"{label} must be one absolute path")
    if path != Path(os.path.abspath(path)):
        raise _reject(f"{label} must be one absolute canonical path")
    return path


def build_privilege_drop_argv(
    *,
    launcher_path: Path,
    target_uid: int,
    target_gid: int,
    command: Sequence[str],
) -> tuple[str, ...]:
    """The exact argv that drops to one unprivileged identity and executes `command`."""

    _require_absolute(launcher_path, "the privilege launcher")
    for label, value in (("target uid", target_uid), ("target gid", target_gid)):
        if type(value) is not int or value < 0:
            raise _reject(f"the {label} must be a non-negative integer")
    if target_uid == 0 or target_gid == 0:
        raise _reject("the launched child may never run as root")
    argv = tuple(str(item) for item in command)
    if not argv or not argv[0].startswith("/"):
        raise _reject("the launcher needs one absolute command to execute")
    return (
        str(launcher_path),
        "--reuid",
        str(target_uid),
        "--regid",
        str(target_gid),
        "--clear-groups",
        "--no-new-privs",
        "--",
        *argv,
    )


def build_parent_death_argv(
    *,
    launcher_path: Path,
    command: Sequence[str],
) -> tuple[str, ...]:
    """The exact argv that binds the child's parent-death signal and executes `command`."""

    _require_absolute(launcher_path, "the privilege launcher")
    argv = tuple(str(item) for item in command)
    if not argv or not argv[0].startswith("/"):
        raise _reject("the launcher needs one absolute command to execute")
    return (
        str(launcher_path),
        "--pdeathsig",
        PARENT_DEATH_SIGNAL_NAME,
        "--",
        *argv,
    )


def _open_directory_chain(trusted_root: Path, relative: Sequence[str]) -> list[int]:
    """Walk from an anchored root FD, refusing symlinks and writable directories."""

    descriptors: list[int] = []
    try:
        root_fd = os.open(str(trusted_root), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    except OSError as error:
        raise _reject("the launcher trusted root is not an openable directory") from error
    descriptors.append(root_fd)
    _require_directory(root_fd, str(trusted_root))
    for name in relative:
        try:
            child = os.open(
                name,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                dir_fd=descriptors[-1],
            )
        except OSError as error:
            for opened in descriptors:
                _close_quietly(opened)
            raise _reject(f"the launcher ancestor {name!r} is not a canonical directory") from error
        descriptors.append(child)
        try:
            _require_directory(child, name)
        except PrivilegeLauncherError:
            for opened in descriptors:
                _close_quietly(opened)
            raise
    return descriptors


def _require_directory(descriptor: int, label: str) -> None:
    info = os.fstat(descriptor)
    if not stat.S_ISDIR(info.st_mode):
        raise _reject(f"the launcher ancestor {label!r} is not a directory")
    if stat.S_IMODE(info.st_mode) & (stat.S_IWGRP | stat.S_IWOTH):
        raise _reject(f"the launcher ancestor {label!r} is group or world writable")


def _close_quietly(descriptor: int) -> None:
    with suppress(OSError):  # pragma: no cover - descriptors are freshly opened here
        os.close(descriptor)


def verify_privilege_launcher(
    launcher_path: Path,
    *,
    expected_owner_uid: int,
    expected_owner_gid: int,
    trusted_root: Path = Path("/"),
    expected_sha256: str | None = None,
) -> LauncherIdentity:
    """Identify the launcher binary, or refuse to launch anything at all.

    The expected owner is injected rather than hard-coded as `0` so the offline suite can
    verify the same predicates against a launcher it owns. Production supplies `0`.
    """

    _require_absolute(launcher_path, "the privilege launcher")
    _require_absolute(trusted_root, "the launcher trusted root")
    try:
        relative = launcher_path.relative_to(trusted_root).parts
    except ValueError as error:
        raise _reject("the privilege launcher is outside its trusted root") from error
    if not relative:
        raise _reject("the privilege launcher is outside its trusted root")

    descriptors = _open_directory_chain(trusted_root, relative[:-1])
    try:
        try:
            file_fd = os.open(
                relative[-1],
                os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=descriptors[-1],
            )
        except FileNotFoundError as error:
            raise _reject("the privilege launcher is not present") from error
        except OSError as error:
            raise _reject("the privilege launcher is not a regular file") from error
        try:
            info = os.fstat(file_fd)
            if not stat.S_ISREG(info.st_mode):
                raise _reject("the privilege launcher is not a regular file")
            if info.st_nlink != 1:
                raise _reject("the privilege launcher is not a single link")
            if info.st_uid != expected_owner_uid or info.st_gid != expected_owner_gid:
                raise _reject("the privilege launcher has an unexpected owner")
            mode = stat.S_IMODE(info.st_mode)
            if mode & PRIVILEGE_LAUNCHER_TCB_ENTRY.forbidden_mode_bits:
                raise _reject("the privilege launcher is group or world writable")
            if not mode & PRIVILEGE_LAUNCHER_TCB_ENTRY.required_mode_bits:
                raise _reject("the privilege launcher is not owner executable")
            if info.st_size > _MAX_LAUNCHER_BYTES:
                raise _reject("the privilege launcher exceeds its bounded size")
            digest = hashlib.sha256()
            with os.fdopen(os.dup(file_fd), "rb") as stream:
                while chunk := stream.read(_FILE_HASH_CHUNK_BYTES):
                    digest.update(chunk)
            observed = digest.hexdigest()
            if expected_sha256 is not None and observed != expected_sha256:
                raise _reject("the privilege launcher hash does not match its pinned value")
            return LauncherIdentity(
                path=launcher_path,
                sha256=observed,
                mode=mode,
                owner_uid=info.st_uid,
                owner_gid=info.st_gid,
                size=info.st_size,
            )
        finally:
            _close_quietly(file_fd)
    finally:
        for opened in reversed(descriptors):
            _close_quietly(opened)


def retained_descriptors(pass_fds: Sequence[int]) -> tuple[int, ...]:
    """Every descriptor the child is allowed to inherit: 0/1/2 plus the IPC pipes."""

    retained = tuple(sorted(set(pass_fds)))
    if any(type(descriptor) is not int or descriptor < 3 for descriptor in retained):
        raise _reject("a retained descriptor is a standard stream or is not a descriptor")
    return (0, 1, 2, *retained)


def assert_descriptor_closure(*, pass_fds: Sequence[int], limit: int) -> tuple[int, ...]:
    """Check that `close_fds=True` plus `pass_fds` leaves exactly the retained set.

    `subprocess` performs the sweep itself, in C, after the point a `preexec_fn` would have
    run. What used to be a second Python-level `closerange` loop is now this assertion: the
    arithmetic still has to agree with the descriptors actually handed to `Popen`, and a
    launch whose bound does not cover them refuses instead of leaking one.
    """

    retained = retained_descriptors(pass_fds)
    if type(limit) is not int or limit <= retained[-1]:
        raise _reject("the descriptor bound does not exceed every retained descriptor")
    return retained


def descriptor_limit() -> int:
    """The upper bound of the descriptor space on this host."""

    configured = os.sysconf("SC_OPEN_MAX") if hasattr(os, "sysconf") else 0
    return max(int(configured or 0), 4096)


__all__ = [
    "PARENT_DEATH_SIGNAL_NAME",
    "PRIVILEGE_DROP_FLAGS",
    "PRIVILEGE_LAUNCHER_TCB_ENTRY",
    "PRODUCTION_PRIVILEGE_LAUNCHER",
    "LauncherIdentity",
    "PrivilegeLauncherError",
    "TrustedFileEntry",
    "assert_descriptor_closure",
    "build_parent_death_argv",
    "build_privilege_drop_argv",
    "descriptor_limit",
    "retained_descriptors",
    "verify_privilege_launcher",
]
