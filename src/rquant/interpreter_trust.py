"""Descriptor-bound interpreter trust for bootstrap entry points.

The shell/OS launcher remains the initial trust root.  Once Python is running,
this module binds a preselected interpreter without ever selecting a fallback
from ``sys._base_executable`` or a PATH search.
"""

from __future__ import annotations

import hashlib
import os
import stat
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path


class InterpreterTrustError(RuntimeError):
    """A preselected interpreter cannot be bound to a trusted descriptor."""


class InterpreterTrustState(StrEnum):
    UNBOUND = "UNBOUND"
    CANONICAL = "CANONICAL"
    ANCESTORS_BOUND = "ANCESTORS_BOUND"
    FD_BOUND = "FD_BOUND"
    ATTESTED = "ATTESTED"
    READY = "READY"
    PRE_EXEC_REVALIDATED = "PRE_EXEC_REVALIDATED"
    EXECUTED = "EXECUTED"
    REJECTED = "REJECTED"


@dataclass(frozen=True)
class InterpreterTrustPolicy:
    profile: str
    canonical_interpreter: Path
    trusted_anchor: Path
    owner_uid: int
    allowed_mode: int
    sha256: str | None = None
    certificate: Callable[[int], None] | None = None

    def __post_init__(self) -> None:
        if not self.profile:
            raise ValueError("interpreter trust profile is required")
        if type(self.owner_uid) is not int or self.owner_uid < 0:
            raise ValueError("interpreter owner UID must be one explicit integer")
        if type(self.allowed_mode) is not int or not 0 <= self.allowed_mode <= 0o777:
            raise ValueError("interpreter mode must be one exact mode")
        if not self.allowed_mode & 0o111:
            raise ValueError("interpreter mode must be executable")
        target = _canonical_path(self.canonical_interpreter, label="interpreter")
        anchor = _canonical_path(self.trusted_anchor, label="interpreter trust anchor")
        try:
            relative = target.relative_to(anchor)
        except ValueError as exc:
            raise ValueError("interpreter must be contained by its trusted anchor") from exc
        if not relative.parts:
            raise ValueError("interpreter must be below its trusted anchor")
        if self.sha256 is not None and (
            len(self.sha256) != 64
            or any(character not in "0123456789abcdef" for character in self.sha256)
        ):
            raise ValueError("interpreter SHA256 is invalid")


@dataclass(frozen=True)
class _Identity:
    device: int
    inode: int
    mode: int
    owner: int
    links: int
    size: int
    mtime_ns: int
    ctime_ns: int

    @classmethod
    def capture(cls, observed: os.stat_result) -> _Identity:
        return cls(
            device=observed.st_dev,
            inode=observed.st_ino,
            mode=observed.st_mode,
            owner=observed.st_uid,
            links=observed.st_nlink,
            size=observed.st_size,
            mtime_ns=observed.st_mtime_ns,
            ctime_ns=observed.st_ctime_ns,
        )


@dataclass(frozen=True)
class _Ancestor:
    parent_fd: int
    name: str
    descriptor: int
    identity: _Identity


class InterpreterTrustBinding:
    def __init__(
        self,
        policy: InterpreterTrustPolicy,
        *,
        anchor_fd: int,
        anchor_identity: _Identity,
        ancestors: tuple[_Ancestor, ...],
        parent_fd: int,
        target_name: str,
        descriptor: int,
        target_identity: _Identity,
    ) -> None:
        self.policy = policy
        self.descriptor = descriptor
        self._anchor_fd = anchor_fd
        self._anchor_identity = anchor_identity
        self._ancestors = ancestors
        self._parent_fd = parent_fd
        self._target_name = target_name
        self._target_identity = target_identity
        self.state = InterpreterTrustState.FD_BOUND
        self.closed = False

    def attest(self) -> None:
        self._require_state(InterpreterTrustState.FD_BOUND)
        try:
            self._revalidate()
            if self.policy.sha256 is not None:
                digest = _hash_descriptor(self.descriptor)
                if digest != self.policy.sha256:
                    raise InterpreterTrustError("interpreter FD SHA256 does not match policy")
            if self.policy.certificate is not None:
                self.policy.certificate(self.descriptor)
            self._revalidate()
            self.state = InterpreterTrustState.ATTESTED
            self.state = InterpreterTrustState.READY
        except BaseException:
            self._reject()
            raise

    def prepare_exec(self) -> int:
        self._require_state(InterpreterTrustState.READY)
        try:
            self._revalidate()
            self.state = InterpreterTrustState.PRE_EXEC_REVALIDATED
            return self.descriptor
        except BaseException:
            self._reject()
            raise

    def exec(
        self,
        argv: tuple[str, ...],
        environment: dict[str, str],
    ) -> object:
        descriptor = self.prepare_exec()
        if os.execve not in os.supports_fd:
            self._reject()
            raise InterpreterTrustError("descriptor interpreter execution is unavailable")
        try:
            self.state = InterpreterTrustState.EXECUTED
            return os.execve(descriptor, argv, environment)
        except BaseException:
            self.close()
            raise

    def launch(
        self,
        runner: Callable[..., object],
        arguments: tuple[str, ...],
        **kwargs: object,
    ) -> object:
        """Start one contained child from this binding's revalidated descriptor."""

        descriptor = self.prepare_exec()
        inherited = kwargs.pop("pass_fds", ())
        if not isinstance(inherited, tuple) or not all(type(fd) is int for fd in inherited):
            self._reject()
            raise InterpreterTrustError("contained launch descriptors are invalid")
        return runner(
            arguments,
            executable_fd=descriptor,
            pass_fds=(*inherited, descriptor),
            **kwargs,
        )

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        descriptors = [self.descriptor, *(ancestor.descriptor for ancestor in self._ancestors)]
        descriptors.append(self._anchor_fd)
        for descriptor in reversed(descriptors):
            with suppress(OSError):
                os.close(descriptor)

    def _require_state(self, expected: InterpreterTrustState) -> None:
        if self.closed or self.state is not expected:
            raise InterpreterTrustError("interpreter trust state is not ready")

    def _reject(self) -> None:
        self.state = InterpreterTrustState.REJECTED
        self.close()

    def _revalidate(self) -> None:
        _require_directory(
            os.stat(self.policy.trusted_anchor, follow_symlinks=False),
            self.policy,
            label="interpreter trust anchor",
        )
        if _Identity.capture(os.fstat(self._anchor_fd)) != self._anchor_identity:
            raise InterpreterTrustError("interpreter trust anchor identity changed")
        active_anchor = _Identity.capture(
            os.stat(self.policy.trusted_anchor, follow_symlinks=False)
        )
        if active_anchor != self._anchor_identity:
            raise InterpreterTrustError("interpreter trust anchor identity changed")
        for ancestor in self._ancestors:
            named = os.stat(ancestor.name, dir_fd=ancestor.parent_fd, follow_symlinks=False)
            _require_directory(named, self.policy, label="interpreter ancestor")
            if (
                _Identity.capture(named) != ancestor.identity
                or _Identity.capture(os.fstat(ancestor.descriptor)) != ancestor.identity
            ):
                raise InterpreterTrustError("interpreter ancestor identity changed")
        named_target = os.stat(self._target_name, dir_fd=self._parent_fd, follow_symlinks=False)
        _require_target(named_target, self.policy)
        if (
            _Identity.capture(named_target) != self._target_identity
            or _Identity.capture(os.fstat(self.descriptor)) != self._target_identity
        ):
            raise InterpreterTrustError("interpreter identity changed")


def bind_interpreter(policy: InterpreterTrustPolicy) -> InterpreterTrustBinding:
    target = _canonical_path(policy.canonical_interpreter, label="interpreter")
    anchor = _canonical_path(policy.trusted_anchor, label="interpreter trust anchor")
    relative = target.relative_to(anchor)
    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    descriptors: list[int] = []
    ancestors: list[_Ancestor] = []
    try:
        named_anchor = os.stat(anchor, follow_symlinks=False)
        _require_directory(named_anchor, policy, label="interpreter trust anchor")
        anchor_fd = os.open(anchor, directory_flags)
        descriptors.append(anchor_fd)
        anchor_identity = _Identity.capture(os.fstat(anchor_fd))
        if anchor_identity != _Identity.capture(named_anchor):
            raise InterpreterTrustError("interpreter trust anchor identity changed")
        parent_fd = anchor_fd
        for component in relative.parts[:-1]:
            named = os.stat(component, dir_fd=parent_fd, follow_symlinks=False)
            _require_directory(named, policy, label="interpreter ancestor")
            child_fd = os.open(component, directory_flags, dir_fd=parent_fd)
            descriptors.append(child_fd)
            identity = _Identity.capture(os.fstat(child_fd))
            if identity != _Identity.capture(named):
                raise InterpreterTrustError("interpreter ancestor identity changed")
            ancestors.append(_Ancestor(parent_fd, component, child_fd, identity))
            parent_fd = child_fd
        target_name = relative.parts[-1]
        named_target = os.stat(target_name, dir_fd=parent_fd, follow_symlinks=False)
        _require_target(named_target, policy)
        descriptor = os.open(
            target_name,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
            dir_fd=parent_fd,
        )
        descriptors.append(descriptor)
        target_identity = _Identity.capture(os.fstat(descriptor))
        if target_identity != _Identity.capture(named_target):
            raise InterpreterTrustError("interpreter identity changed while opening")
        binding = InterpreterTrustBinding(
            policy,
            anchor_fd=anchor_fd,
            anchor_identity=anchor_identity,
            ancestors=tuple(ancestors),
            parent_fd=parent_fd,
            target_name=target_name,
            descriptor=descriptor,
            target_identity=target_identity,
        )
        descriptors.clear()
        return binding
    except InterpreterTrustError:
        raise
    except OSError as exc:
        raise InterpreterTrustError("interpreter cannot be bound") from exc
    finally:
        for descriptor in reversed(descriptors):
            with suppress(OSError):
                os.close(descriptor)


def _canonical_path(path: Path, *, label: str) -> Path:
    candidate = Path(os.path.abspath(path))
    if not path.is_absolute() or path != candidate:
        raise ValueError(f"{label} must be an absolute canonical path")
    return candidate


def _require_directory(
    observed: os.stat_result,
    policy: InterpreterTrustPolicy,
    *,
    label: str,
) -> None:
    if (
        not stat.S_ISDIR(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != policy.owner_uid
        or observed.st_mode & 0o022
    ):
        raise InterpreterTrustError(f"{label} is unsafe")


def _require_target(observed: os.stat_result, policy: InterpreterTrustPolicy) -> None:
    if (
        not stat.S_ISREG(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != policy.owner_uid
        or stat.S_IMODE(observed.st_mode) != policy.allowed_mode
        or observed.st_nlink != 1
    ):
        raise InterpreterTrustError("interpreter target is unsafe")


def _hash_descriptor(descriptor: int) -> str:
    digest = hashlib.sha256()
    try:
        os.lseek(descriptor, 0, os.SEEK_SET)
        while chunk := os.read(descriptor, 64 * 1024):
            digest.update(chunk)
        os.lseek(descriptor, 0, os.SEEK_SET)
    except OSError as exc:
        raise InterpreterTrustError("interpreter FD cannot be hashed") from exc
    return digest.hexdigest()


__all__ = [
    "InterpreterTrustBinding",
    "InterpreterTrustError",
    "InterpreterTrustPolicy",
    "InterpreterTrustState",
    "bind_interpreter",
]
