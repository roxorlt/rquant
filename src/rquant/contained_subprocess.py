"""Bounded subprocess execution with descendant containment.

The module is deliberately standard-library only so the deployment bootstrap can load
the exact immutable generation copy before importing the rest of :mod:`rquant`.
"""

from __future__ import annotations

import ctypes
import errno
import os
import secrets
import select
import signal
import struct
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


class ContainedProcessError(RuntimeError):
    """A process tree could not be started, stopped, or proven contained."""


class _ContainedSignal(BaseException):
    def __init__(self, signum: int) -> None:
        self.signum = signum


class _ContainedSignalLatch:
    def __init__(self) -> None:
        self.first_signum: int | None = None
        self._raised = False
        self._deferred = False

    def defer(self) -> None:
        self._deferred = True

    def handle(self, signum: int, _frame: object) -> None:
        if self.first_signum is None:
            self.first_signum = signum
        if self._deferred or self._raised:
            return
        self._raised = True
        raise _ContainedSignal(self.first_signum)


@dataclass(frozen=True, order=True)
class ProcessIdentity:
    pid: int
    started: tuple[int, int]
    kernel_unique_id: int = 0


@dataclass(frozen=True)
class _ProcessObservation:
    identity: ProcessIdentity
    parent_pid: int
    containment_token: bool = False
    parent_kernel_unique_id: int = 0


Clock = Callable[[], float]
Sleep = Callable[[float], None]
Inventory = Callable[[float], dict[int, _ProcessObservation]]

_CONTAINMENT_ENVIRONMENT_KEY = "RQUANT_CONTAINMENT_TOKEN"
_MAX_PROCESS_ARGUMENT_BYTES = 4 * 1024 * 1024
_LINUX_SUBREAPER_LOCK = threading.Lock()
_DARWIN_UNIQUE_IDENTITY_FLAVOR = 17
_DARWIN_UNIQUE_IDENTITY_SIZE = 56
_DARWIN_LIST_FDS_FLAVOR = 1
_DARWIN_PIPE_FD_TYPE = 6
_DARWIN_PIPE_INFO_FLAVOR = 6
_DARWIN_PIPE_INFO_SIZE = 184
_DARWIN_PIPE_HANDLE_OFFSET = 160
_DARWIN_FD_ENTRY_SIZE = 8
_MAX_DARWIN_FD_LIST_BYTES = 4 * 1024 * 1024
DarwinPipeMarker = tuple[int, int]


def _close_file_descriptors(descriptors: list[int]) -> None:
    while descriptors:
        with suppress(OSError):
            os.close(descriptors.pop())


def _cleanup_reserve_seconds(remaining: float) -> float:
    fraction = 0.75 if remaining <= 0.25 else 0.5
    return min(1.0, max(0.1, remaining * fraction))


class _KernelProcessTracker(Protocol):
    def register_root(self, pid: int, *, deadline: float) -> ProcessIdentity: ...

    def poll(self, *, deadline: float) -> dict[int, ProcessIdentity]: ...

    def close(self) -> None: ...


KernelTrackerFactory = Callable[[], _KernelProcessTracker]


def _darwin_unique_process_identity(
    libproc: ctypes.CDLL,
    pid: int,
) -> tuple[int, int] | None:
    buffer = ctypes.create_string_buffer(_DARWIN_UNIQUE_IDENTITY_SIZE)
    size = libproc.proc_pidinfo(
        pid,
        _DARWIN_UNIQUE_IDENTITY_FLAVOR,
        0,
        buffer,
        len(buffer),
    )
    if size != _DARWIN_UNIQUE_IDENTITY_SIZE:
        return None
    unique_id, parent_unique_id = struct.unpack_from("=QQ", buffer.raw, 16)
    if unique_id <= 0:
        return None
    return unique_id, parent_unique_id


def _darwin_process_observation(pid: int) -> _ProcessObservation | None:
    libproc = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
    libproc.proc_pidinfo.argtypes = [
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_uint64,
        ctypes.c_void_p,
        ctypes.c_int,
    ]
    libproc.proc_pidinfo.restype = ctypes.c_int
    buffer = ctypes.create_string_buffer(256)
    size = libproc.proc_pidinfo(pid, 3, 0, buffer, len(buffer))
    if size < 136:
        return None
    _flags, _status, _xstatus, observed_pid, parent, effective_uid = struct.unpack_from(
        "=IIIIII", buffer.raw
    )
    start_seconds, start_microseconds = struct.unpack_from("=QQ", buffer.raw, 120)
    if observed_pid != pid or start_seconds <= 0 or effective_uid != os.getuid():
        return None
    unique_identity = _darwin_unique_process_identity(libproc, pid)
    if unique_identity is None:
        return None
    unique_id, parent_unique_id = unique_identity
    return _ProcessObservation(
        identity=ProcessIdentity(
            pid,
            (start_seconds, start_microseconds),
            kernel_unique_id=unique_id,
        ),
        parent_pid=parent,
        parent_kernel_unique_id=parent_unique_id,
    )


def _darwin_libproc() -> ctypes.CDLL:
    libproc = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
    libproc.proc_pidinfo.argtypes = [
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_uint64,
        ctypes.c_void_p,
        ctypes.c_int,
    ]
    libproc.proc_pidinfo.restype = ctypes.c_int
    libproc.proc_pidfdinfo.argtypes = [
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.c_int,
    ]
    libproc.proc_pidfdinfo.restype = ctypes.c_int
    return libproc


def _darwin_pipe_marker_for_fd(pid: int, fd: int) -> DarwinPipeMarker:
    libproc = _darwin_libproc()
    buffer = ctypes.create_string_buffer(_DARWIN_PIPE_INFO_SIZE)
    size = libproc.proc_pidfdinfo(
        pid,
        fd,
        _DARWIN_PIPE_INFO_FLAVOR,
        buffer,
        len(buffer),
    )
    if size != _DARWIN_PIPE_INFO_SIZE:
        raise ContainedProcessError("Darwin containment pipe identity is unavailable")
    handle, peer_handle = struct.unpack_from(
        "=QQ",
        buffer.raw,
        _DARWIN_PIPE_HANDLE_OFFSET,
    )
    if handle <= 0 or peer_handle <= 0 or handle == peer_handle:
        raise ContainedProcessError("Darwin containment pipe identity is invalid")
    return tuple(sorted((handle, peer_handle)))


def _darwin_process_has_pipe_marker(
    pid: int,
    markers: frozenset[DarwinPipeMarker],
    *,
    deadline: float,
) -> bool:
    if not markers:
        return False
    if time.monotonic() >= deadline:
        raise TimeoutError("process pipe inventory timed out")
    libproc = _darwin_libproc()
    required = libproc.proc_pidinfo(pid, _DARWIN_LIST_FDS_FLAVOR, 0, None, 0)
    if required <= 0:
        return False
    if required > _MAX_DARWIN_FD_LIST_BYTES:
        raise ContainedProcessError("process file descriptor inventory exceeds budget")
    buffer = ctypes.create_string_buffer(required)
    size = libproc.proc_pidinfo(
        pid,
        _DARWIN_LIST_FDS_FLAVOR,
        0,
        buffer,
        len(buffer),
    )
    if size <= 0:
        return False
    if size % _DARWIN_FD_ENTRY_SIZE != 0:
        raise ContainedProcessError("process file descriptor inventory is malformed")
    pipe_buffer = ctypes.create_string_buffer(_DARWIN_PIPE_INFO_SIZE)
    for offset in range(0, size, _DARWIN_FD_ENTRY_SIZE):
        if time.monotonic() >= deadline:
            raise TimeoutError("process pipe inventory timed out")
        fd, fd_type = struct.unpack_from("=iI", buffer.raw, offset)
        if fd_type != _DARWIN_PIPE_FD_TYPE:
            continue
        pipe_size = libproc.proc_pidfdinfo(
            pid,
            fd,
            _DARWIN_PIPE_INFO_FLAVOR,
            pipe_buffer,
            len(pipe_buffer),
        )
        if pipe_size != _DARWIN_PIPE_INFO_SIZE:
            continue
        handle, peer_handle = struct.unpack_from(
            "=QQ",
            pipe_buffer.raw,
            _DARWIN_PIPE_HANDLE_OFFSET,
        )
        if tuple(sorted((handle, peer_handle))) in markers:
            return True
    return False


def _linux_process_observation(pid: int) -> _ProcessObservation | None:
    try:
        raw = (Path("/proc") / str(pid) / "stat").read_text(encoding="ascii")
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return None
    close = raw.rfind(")")
    fields = raw[close + 2 :].split()
    if close < 0 or len(fields) < 20:
        return None
    return _ProcessObservation(
        identity=ProcessIdentity(pid, (int(fields[19]), 0)),
        parent_pid=int(fields[1]),
    )


def _process_observation(pid: int) -> _ProcessObservation | None:
    if sys.platform == "darwin":
        return _darwin_process_observation(pid)
    if sys.platform.startswith("linux"):
        return _linux_process_observation(pid)
    return None


class _DarwinKqueueProcessTracker:
    def __init__(self) -> None:
        self._queue = select.kqueue()
        self._known: dict[int, ProcessIdentity] = {}
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._error: BaseException | None = None
        self._root_pid: int | None = None
        self._root_started: tuple[int, int] | None = None
        self._deadline = 0.0
        self._registered: set[int] = set()
        self._poll_generation = 0

    def _register_process(self, identity: ProcessIdentity) -> bool:
        if identity.pid in self._registered:
            return True
        before = _darwin_process_observation(identity.pid)
        if before is None:
            return False
        if before.identity != identity:
            raise ContainedProcessError("kernel child registration identity changed")
        event = select.kevent(
            identity.pid,
            filter=select.KQ_FILTER_PROC,
            flags=select.KQ_EV_ADD | select.KQ_EV_ENABLE | select.KQ_EV_CLEAR,
            fflags=select.KQ_NOTE_FORK | select.KQ_NOTE_EXIT,
        )
        try:
            self._queue.control([event], 0, 0)
        except OSError as exc:
            if exc.errno == errno.ESRCH:
                return False
            raise ContainedProcessError("kernel child registration failed") from exc
        after = _darwin_process_observation(identity.pid)
        if after is not None and after.identity != identity:
            raise ContainedProcessError("kernel child identity changed during registration")
        self._registered.add(identity.pid)
        return True

    def register_root(self, pid: int, *, deadline: float) -> ProcessIdentity:
        if time.monotonic() >= deadline:
            raise TimeoutError("kernel tracker registration deadline expired")
        before = _darwin_process_observation(pid)
        if before is None:
            raise ContainedProcessError("kernel root registration failed")
        # Darwin exposes NOTE_TRACK constants through Python but rejects that
        # FreeBSD extension with ENOTSUP. NOTE_FORK is the supported kernel edge;
        # every discovered child is registered before it becomes trusted.
        if not self._register_process(before.identity):
            raise ContainedProcessError("kernel root registration failed")
        self._root_pid = pid
        self._root_started = before.identity.started
        self._known[pid] = before.identity
        self._deadline = deadline
        self._thread = threading.Thread(
            target=self._track,
            name=f"rquant-kqueue-{pid}",
            daemon=True,
        )
        self._thread.start()
        return before.identity

    def _track(self) -> None:
        try:
            while not self._stop.is_set():
                if time.monotonic() >= self._deadline:
                    raise TimeoutError("kernel tracker deadline expired")
                events = self._queue.control(None, 256, 0.01)
                fork_observed = False
                for event in events:
                    if event.fflags & select.KQ_NOTE_TRACKERR:
                        raise ContainedProcessError("kernel process tracker NOTE_TRACKERR")
                    fork_observed = fork_observed or bool(event.fflags & select.KQ_NOTE_FORK)
                if fork_observed:
                    if self._root_pid is None or self._root_started is None:
                        raise ContainedProcessError("kernel root is not registered")
                    inventory = _darwin_process_inventory(
                        self._deadline,
                        started_at_or_after=self._root_started,
                    )
                with self._condition:
                    if fork_observed:
                        assert self._root_pid is not None
                        descendants = _discover_descendants(
                            self._root_pid,
                            inventory,
                            self._known,
                        )
                        for pid, identity in descendants.items():
                            prior = self._known.get(pid)
                            if prior is not None and prior != identity:
                                raise ContainedProcessError(
                                    "kernel tracker observed PID identity reuse"
                                )
                            self._register_process(identity)
                            self._known[pid] = identity
                    self._poll_generation += 1
                    self._condition.notify_all()
        except BaseException as exc:
            if self._stop.is_set() and isinstance(exc, OSError) and exc.errno == errno.EBADF:
                return
            with self._condition:
                self._error = exc
                self._condition.notify_all()
        finally:
            with suppress(OSError):
                self._queue.close()

    def poll(self, *, deadline: float) -> dict[int, ProcessIdentity]:
        with self._condition:
            observed_generation = self._poll_generation
            while self._error is None and self._poll_generation == observed_generation:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("kernel process tracker poll timed out")
                self._condition.wait(timeout=remaining)
            if self._error is not None:
                raise ContainedProcessError("kernel process tracking failed") from self._error
            return dict(self._known)

    def close(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(0.0, self._deadline - time.monotonic()))
            if self._thread.is_alive():
                raise ContainedProcessError("kernel process tracker did not stop")
            self._thread = None
        else:
            with suppress(OSError):
                self._queue.close()


class _LinuxSubreaperProcessTracker:
    _PR_SET_CHILD_SUBREAPER = 36
    _PR_GET_CHILD_SUBREAPER = 37

    def __init__(self) -> None:
        self._known: dict[int, ProcessIdentity] = {}
        self._pidfds: dict[int, int] = {}
        self._root_pid: int | None = None
        self._root_started: tuple[int, int] | None = None
        self._previous_subreaper = 0
        self._owns_subreaper_lock = False
        self._subreaper_changed = False

    def _enable_subreaper(self, deadline: float) -> None:
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not _LINUX_SUBREAPER_LOCK.acquire(timeout=remaining):
            raise TimeoutError("subreaper registration deadline expired")
        self._owns_subreaper_lock = True
        libc = ctypes.CDLL(None, use_errno=True)
        current = ctypes.c_int()
        if libc.prctl(self._PR_GET_CHILD_SUBREAPER, ctypes.byref(current), 0, 0, 0) != 0:
            raise ContainedProcessError("could not read child subreaper state")
        self._previous_subreaper = int(current.value)
        if self._previous_subreaper != 1:
            if libc.prctl(self._PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) != 0:
                raise ContainedProcessError("could not enable child subreaper")
            self._subreaper_changed = True

    def _bind_pid(self, identity: ProcessIdentity) -> None:
        prior = self._known.get(identity.pid)
        if prior is not None and prior != identity:
            raise ContainedProcessError("kernel tracker observed PID identity reuse")
        self._known[identity.pid] = identity
        if identity.pid not in self._pidfds and hasattr(os, "pidfd_open"):
            with suppress(ProcessLookupError):
                self._pidfds[identity.pid] = os.pidfd_open(identity.pid, 0)

    def register_root(self, pid: int, *, deadline: float) -> ProcessIdentity:
        self._enable_subreaper(deadline)
        observed = _linux_process_observation(pid)
        if observed is None:
            raise ContainedProcessError("kernel root registration failed")
        self._root_pid = pid
        self._root_started = observed.identity.started
        self._bind_pid(observed.identity)
        return observed.identity

    def poll(self, *, deadline: float) -> dict[int, ProcessIdentity]:
        if self._root_pid is None or self._root_started is None:
            raise ContainedProcessError("kernel root is not registered")
        inventory = _linux_process_inventory(deadline)
        descendants = _discover_descendants(self._root_pid, inventory, self._known)
        for observation in inventory.values():
            if (
                observation.parent_pid == os.getpid()
                and observation.identity.started >= self._root_started
            ):
                descendants[observation.identity.pid] = observation.identity
        for identity in descendants.values():
            self._bind_pid(identity)
        for pid, identity in sorted(self._known.items()):
            if pid == self._root_pid:
                continue
            observation = inventory.get(pid)
            if (
                observation is None
                or observation.identity != identity
                or observation.parent_pid != os.getpid()
            ):
                continue
            try:
                reaped_pid, _status = os.waitpid(pid, os.WNOHANG)
            except (ChildProcessError, ProcessLookupError):
                continue
            if reaped_pid not in {0, pid}:
                raise ContainedProcessError("subreaper reaped an unexpected process identity")
        return dict(self._known)

    def close(self) -> None:
        for descriptor in self._pidfds.values():
            with suppress(OSError):
                os.close(descriptor)
        self._pidfds.clear()
        restore_failed = False
        if self._owns_subreaper_lock:
            try:
                if self._subreaper_changed:
                    libc = ctypes.CDLL(None, use_errno=True)
                    restore_failed = (
                        libc.prctl(
                            self._PR_SET_CHILD_SUBREAPER,
                            self._previous_subreaper,
                            0,
                            0,
                            0,
                        )
                        != 0
                    )
            finally:
                self._subreaper_changed = False
                self._owns_subreaper_lock = False
                _LINUX_SUBREAPER_LOCK.release()
        if restore_failed:
            raise ContainedProcessError("could not restore child subreaper state")


def _create_kernel_tracker() -> _KernelProcessTracker:
    if sys.platform == "darwin":
        return _DarwinKqueueProcessTracker()
    if sys.platform.startswith("linux"):
        return _LinuxSubreaperProcessTracker()
    raise ContainedProcessError("kernel process tracking is unsupported on this platform")


def _darwin_process_has_token(pid: int, token: str, *, deadline: float) -> bool:
    if time.monotonic() >= deadline:
        raise TimeoutError("process environment inventory timed out")
    libc = ctypes.CDLL(None, use_errno=True)
    libc.sysctl.argtypes = [
        ctypes.POINTER(ctypes.c_int),
        ctypes.c_uint,
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_size_t),
        ctypes.c_void_p,
        ctypes.c_size_t,
    ]
    libc.sysctl.restype = ctypes.c_int
    mib = (ctypes.c_int * 3)(1, 49, pid)  # CTL_KERN, KERN_PROCARGS2, pid
    size = ctypes.c_size_t()
    if libc.sysctl(mib, 3, None, ctypes.byref(size), None, 0) != 0:
        error = ctypes.get_errno()
        if error in {errno.ESRCH, errno.EPERM, errno.EACCES, errno.EIO, errno.EINVAL}:
            return False
        raise OSError(error, "sysctl KERN_PROCARGS2 size")
    if size.value <= 0 or size.value > _MAX_PROCESS_ARGUMENT_BYTES:
        return False
    buffer = ctypes.create_string_buffer(size.value)
    if libc.sysctl(mib, 3, buffer, ctypes.byref(size), None, 0) != 0:
        error = ctypes.get_errno()
        if error in {errno.ESRCH, errno.EPERM, errno.EACCES, errno.EIO, errno.EINVAL}:
            return False
        raise OSError(error, "sysctl KERN_PROCARGS2 payload")
    expected = f"{_CONTAINMENT_ENVIRONMENT_KEY}={token}".encode()
    return expected in buffer.raw[: size.value].split(b"\0")


def _linux_process_has_token(pid: int, token: str, *, deadline: float) -> bool:
    if time.monotonic() >= deadline:
        raise TimeoutError("process environment inventory timed out")
    expected = f"{_CONTAINMENT_ENVIRONMENT_KEY}={token}".encode()
    try:
        payload = (Path("/proc") / str(pid) / "environ").read_bytes()
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return False
    if len(payload) > _MAX_PROCESS_ARGUMENT_BYTES:
        raise ContainedProcessError("process environment exceeds containment budget")
    return expected in payload.split(b"\0")


def _darwin_process_inventory(
    deadline: float,
    *,
    containment_token: str | None = None,
    started_at_or_after: tuple[int, int] | None = None,
    pipe_markers: frozenset[DarwinPipeMarker] = frozenset(),
) -> dict[int, _ProcessObservation]:
    try:
        libproc = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
        libproc.proc_listallpids.argtypes = [ctypes.c_void_p, ctypes.c_int]
        libproc.proc_listallpids.restype = ctypes.c_int
        libproc.proc_pidinfo.argtypes = [
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_uint64,
            ctypes.c_void_p,
            ctypes.c_int,
        ]
        libproc.proc_pidinfo.restype = ctypes.c_int
        capacity = max(256, libproc.proc_listallpids(None, 0) * 2)
        pids = (ctypes.c_int * capacity)()
        count = libproc.proc_listallpids(pids, ctypes.sizeof(pids))
        if count < 0:
            raise OSError(ctypes.get_errno(), "proc_listallpids")
        result: dict[int, _ProcessObservation] = {}
        for pid in pids[:count]:
            if time.monotonic() >= deadline:
                raise TimeoutError("process inventory timed out")
            buffer = ctypes.create_string_buffer(256)
            size = libproc.proc_pidinfo(pid, 3, 0, buffer, len(buffer))
            if size < 136:
                continue
            _flags, _status, _xstatus, observed_pid, parent, effective_uid = struct.unpack_from(
                "=IIIIII", buffer.raw
            )
            start_seconds, start_microseconds = struct.unpack_from("=QQ", buffer.raw, 120)
            if observed_pid != pid or start_seconds <= 0 or effective_uid != os.getuid():
                continue
            unique_identity = _darwin_unique_process_identity(libproc, pid)
            if unique_identity is None:
                continue
            unique_id, parent_unique_id = unique_identity
            result[pid] = _ProcessObservation(
                identity=ProcessIdentity(
                    pid,
                    (start_seconds, start_microseconds),
                    kernel_unique_id=unique_id,
                ),
                parent_pid=parent,
                containment_token=(
                    (
                        started_at_or_after is None
                        or (start_seconds, start_microseconds) >= started_at_or_after
                    )
                    and (
                        (
                            containment_token is not None
                            and _darwin_process_has_token(
                                pid,
                                containment_token,
                                deadline=deadline,
                            )
                        )
                        or _darwin_process_has_pipe_marker(
                            pid,
                            pipe_markers,
                            deadline=deadline,
                        )
                    )
                ),
                parent_kernel_unique_id=parent_unique_id,
            )
        return result
    except TimeoutError:
        raise
    except (OSError, ValueError) as exc:
        raise ContainedProcessError("process inventory failed") from exc


def _linux_process_inventory(
    deadline: float,
    *,
    containment_token: str | None = None,
    started_at_or_after: tuple[int, int] | None = None,
) -> dict[int, _ProcessObservation]:
    result: dict[int, _ProcessObservation] = {}
    try:
        for entry in Path("/proc").iterdir():
            if time.monotonic() >= deadline:
                raise TimeoutError("process inventory timed out")
            if not entry.name.isdigit():
                continue
            raw = (entry / "stat").read_text(encoding="ascii")
            close = raw.rfind(")")
            fields = raw[close + 2 :].split()
            if close < 0 or len(fields) < 20:
                continue
            pid = int(entry.name)
            identity = ProcessIdentity(pid, (int(fields[19]), 0))
            result[pid] = _ProcessObservation(
                identity=identity,
                parent_pid=int(fields[1]),
                containment_token=(
                    containment_token is not None
                    and (started_at_or_after is None or identity.started >= started_at_or_after)
                    and _linux_process_has_token(pid, containment_token, deadline=deadline)
                ),
            )
    except TimeoutError:
        raise
    except (OSError, ValueError) as exc:
        raise ContainedProcessError("process inventory failed") from exc
    return result


def process_inventory(
    deadline: float,
    *,
    containment_token: str | None = None,
    started_at_or_after: tuple[int, int] | None = None,
    darwin_pipe_markers: frozenset[DarwinPipeMarker] = frozenset(),
) -> dict[int, _ProcessObservation]:
    if time.monotonic() >= deadline:
        raise TimeoutError("process inventory deadline expired")
    if sys.platform == "darwin":
        return _darwin_process_inventory(
            deadline,
            containment_token=containment_token,
            started_at_or_after=started_at_or_after,
            pipe_markers=darwin_pipe_markers,
        )
    if sys.platform.startswith("linux"):
        return _linux_process_inventory(
            deadline,
            containment_token=containment_token,
            started_at_or_after=started_at_or_after,
        )
    raise ContainedProcessError("process inventory is unsupported on this platform")


def _discover_descendants(
    root_pid: int,
    inventory: Mapping[int, _ProcessObservation],
    known: Mapping[int, ProcessIdentity],
) -> dict[int, ProcessIdentity]:
    parents: dict[int, set[int]] = {}
    birth_parents: dict[int, set[int]] = {}
    for observation in inventory.values():
        parents.setdefault(observation.parent_pid, set()).add(observation.identity.pid)
        if observation.parent_kernel_unique_id:
            birth_parents.setdefault(observation.parent_kernel_unique_id, set()).add(
                observation.identity.pid
            )
    pending = [root_pid, *known]
    descendants = dict(known)
    for observation in inventory.values():
        if not observation.containment_token:
            continue
        prior = descendants.get(observation.identity.pid)
        if prior is None or prior == observation.identity:
            descendants[observation.identity.pid] = observation.identity
    visited: set[int] = set()
    while pending:
        parent = pending.pop()
        if parent in visited:
            continue
        visited.add(parent)
        parent_observation = inventory.get(parent)
        parent_identity = (
            parent_observation.identity if parent_observation is not None else known.get(parent)
        )
        child_pids = set(parents.get(parent, ()))
        if parent_identity is not None and parent_identity.kernel_unique_id:
            child_pids.update(birth_parents.get(parent_identity.kernel_unique_id, ()))
        for pid in child_pids:
            observation = inventory.get(pid)
            if observation is None:
                continue
            prior = descendants.get(pid)
            if prior is not None and prior != observation.identity:
                # A reused PID is not evidence that the replacement process belongs
                # to the original tree. Do not signal it or traverse through it.
                continue
            descendants[pid] = observation.identity
            pending.append(pid)
    descendants.pop(root_pid, None)
    return descendants


def _signal_identity(
    identity: ProcessIdentity,
    signum: int,
    inventory: Mapping[int, _ProcessObservation],
) -> None:
    observation = inventory.get(identity.pid)
    if observation is None or observation.identity != identity:
        return
    try:
        os.kill(identity.pid, signum)
    except ProcessLookupError:
        return
    except PermissionError as exc:
        raise ContainedProcessError("process descendant containment is unverifiable") from exc


def _signal_bound_identity(identity: ProcessIdentity, signum: int) -> None:
    observed = _process_observation(identity.pid)
    if observed is None or observed.identity != identity:
        return
    try:
        os.kill(identity.pid, signum)
    except ProcessLookupError:
        return
    except PermissionError as exc:
        raise ContainedProcessError("kernel-tracked process cannot be signalled") from exc


def _merge_kernel_identities(
    known: dict[int, ProcessIdentity],
    tracker: _KernelProcessTracker,
    *,
    root_pid: int,
    deadline: float,
) -> None:
    for pid, identity in tracker.poll(deadline=deadline).items():
        if pid == root_pid:
            continue
        prior = known.get(pid)
        if prior is not None and prior != identity:
            raise ContainedProcessError("kernel tracker observed PID identity reuse")
        known[pid] = identity


def _terminate_blocked_root(process: subprocess.Popen[str], *, deadline: float) -> None:
    signal_error: PermissionError | None = None
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    except PermissionError as exc:
        signal_error = exc
        with suppress(ProcessLookupError):
            process.kill()
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ContainedProcessError("blocked subprocess cleanup deadline expired")
    try:
        process.communicate(timeout=remaining)
    except subprocess.TimeoutExpired as exc:
        raise ContainedProcessError("blocked subprocess could not be reaped") from exc
    if signal_error is not None:
        raise ContainedProcessError(
            "blocked process group could not be signalled"
        ) from signal_error


def _cleanup_process_tree(
    process: subprocess.Popen[str],
    known: dict[int, ProcessIdentity],
    *,
    root_identity: ProcessIdentity | None = None,
    deadline: float,
    inventory_provider: Inventory,
    clock: Clock,
    sleep: Sleep,
    kernel_tracker: _KernelProcessTracker | None = None,
    initial_inventory: Mapping[int, _ProcessObservation] | None = None,
) -> None:
    kernel_error: BaseException | None = None
    process_group_errors: list[PermissionError] = []

    def signal_process_group(signum: int) -> None:
        try:
            os.killpg(process.pid, signum)
        except ProcessLookupError:
            return
        except PermissionError as exc:
            process_group_errors.append(exc)

    def merge_kernel() -> None:
        nonlocal kernel_error
        if kernel_tracker is None or kernel_error is not None:
            return
        try:
            _merge_kernel_identities(
                known,
                kernel_tracker,
                root_pid=process.pid,
                deadline=deadline,
            )
        except BaseException as exc:
            kernel_error = exc

    signal_process_group(signal.SIGSTOP)
    merge_kernel()
    if initial_inventory is not None:
        if root_identity is not None:
            _signal_identity(root_identity, signal.SIGSTOP, initial_inventory)
            root = initial_inventory.get(process.pid)
            if root is not None and root.identity == root_identity:
                signal_process_group(signal.SIGSTOP)
        for identity in tuple(known.values()):
            _signal_identity(identity, signal.SIGSTOP, initial_inventory)
    stable = 0
    prior: frozenset[ProcessIdentity] = frozenset()
    while stable < 2:
        remaining = deadline - clock()
        if remaining <= 0:
            raise ContainedProcessError("process containment deadline expired")
        inventory = inventory_provider(deadline)
        merge_kernel()
        known.update(_discover_descendants(process.pid, inventory, known))
        if root_identity is not None:
            _signal_identity(root_identity, signal.SIGSTOP, inventory)
            root = inventory.get(process.pid)
            if root is not None and root.identity == root_identity:
                signal_process_group(signal.SIGSTOP)
        for identity in tuple(known.values()):
            _signal_identity(identity, signal.SIGSTOP, inventory)
            _signal_bound_identity(identity, signal.SIGSTOP)
        current = frozenset(known.values())
        stable = stable + 1 if current == prior else 0
        prior = current
        if stable < 2:
            sleep(min(0.01, max(0.0, deadline - clock())))

    inventory = inventory_provider(deadline)
    merge_kernel()
    for identity in sorted(known.values(), reverse=True):
        _signal_identity(identity, signal.SIGKILL, inventory)
        _signal_bound_identity(identity, signal.SIGKILL)
    if root_identity is not None:
        _signal_identity(root_identity, signal.SIGKILL, inventory)
    signal_process_group(signal.SIGKILL)

    remaining = deadline - clock()
    if remaining <= 0:
        raise ContainedProcessError("process containment deadline expired before reap")
    try:
        process.communicate(timeout=remaining)
    except subprocess.TimeoutExpired as exc:
        raise ContainedProcessError("process group could not be reaped") from exc

    while True:
        remaining = deadline - clock()
        if remaining <= 0:
            raise ContainedProcessError("detached process descendants survived cleanup")
        inventory = inventory_provider(deadline)
        merge_kernel()
        alive = {
            pid: identity
            for pid, identity in known.items()
            if (
                (pid in inventory and inventory[pid].identity == identity)
                or (
                    (observed := _process_observation(pid)) is not None
                    and observed.identity == identity
                )
            )
        }
        if not alive:
            if kernel_error is not None:
                raise ContainedProcessError(
                    "kernel process tracking failed during cleanup"
                ) from kernel_error
            if process_group_errors:
                raise ContainedProcessError(
                    "process group signalling failed during containment cleanup"
                ) from ExceptionGroup(
                    "process group signal failures",
                    process_group_errors,
                )
            return
        for identity in alive.values():
            _signal_identity(identity, signal.SIGKILL, inventory)
            _signal_bound_identity(identity, signal.SIGKILL)
        sleep(min(0.01, remaining))


def run_contained(
    args: Sequence[str],
    *,
    cwd: Path,
    deadline_monotonic: float,
    check: bool = False,
    pass_fds: tuple[int, ...] = (),
    env: Mapping[str, str] | None = None,
    text: bool = True,
    inventory_provider: Inventory = process_inventory,
    clock: Clock = time.monotonic,
    sleep: Sleep = time.sleep,
    cancellation_check: Callable[[], bool] | None = None,
    kernel_tracker_factory: KernelTrackerFactory = _create_kernel_tracker,
    may_spawn_background_descendants: bool,
) -> subprocess.CompletedProcess[str]:
    """Run one command within one absolute deadline and clean up its process tree.

    ``may_spawn_background_descendants`` is a required launch capability declaration.
    Darwin refuses ``True`` before spawning because its available process APIs cannot
    prove containment after a descendant reparents and discards inherited evidence.
    Passing ``False`` is therefore a caller guarantee that the command does not
    intentionally daemonize; it is not a stronger Darwin kernel guarantee.
    """

    remaining = deadline_monotonic - clock()
    if remaining <= 0:
        raise subprocess.TimeoutExpired(list(args), 0)
    cleanup_reserve = _cleanup_reserve_seconds(remaining)
    execution_deadline = deadline_monotonic - cleanup_reserve
    if execution_deadline <= clock():
        raise subprocess.TimeoutExpired(list(args), 0)
    if sys.platform == "darwin" and may_spawn_background_descendants:
        raise ContainedProcessError(
            "Darwin cannot prove containment for background-capable commands; startup refused"
        )
    containment_token = secrets.token_hex(32)
    process_environment = dict(os.environ if env is None else env)
    process_environment[_CONTAINMENT_ENVIRONMENT_KEY] = containment_token
    kernel_tracker: _KernelProcessTracker | None = None
    gate_read = gate_write = -1
    process: subprocess.Popen[str] | None = None
    darwin_pipe_markers: frozenset[DarwinPipeMarker] = frozenset()
    darwin_pipe_anchor_fds: list[int] = []

    try:
        kernel_tracker = kernel_tracker_factory()
        gate_read, gate_write = os.pipe()
        helper_command = [
            sys.executable,
            "-I",
            "-S",
            str(Path(__file__).resolve(strict=True)),
            "--contained-child",
            str(gate_read),
            "--",
            *args,
        ]
        process = subprocess.Popen(
            helper_command,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=text,
            start_new_session=True,
            pass_fds=(*pass_fds, gate_read),
            env=process_environment,
        )
        os.close(gate_read)
        gate_read = -1
        if sys.platform == "darwin":
            if process.stdout is None or process.stderr is None:
                raise ContainedProcessError("Darwin containment pipes are unavailable")
            for stream in (process.stdout, process.stderr):
                anchor = os.dup(stream.fileno())
                os.set_inheritable(anchor, False)
                darwin_pipe_anchor_fds.append(anchor)
            darwin_pipe_markers = frozenset(
                {_darwin_pipe_marker_for_fd(os.getpid(), fd) for fd in darwin_pipe_anchor_fds}
            )
        root_identity = kernel_tracker.register_root(
            process.pid,
            deadline=deadline_monotonic,
        )
        if root_identity.pid != process.pid:
            raise ContainedProcessError("kernel tracker root identity does not match subprocess")
        root_started: tuple[int, int] | None = root_identity.started
        if cancellation_check is not None and cancellation_check():
            raise ContainedProcessError("contained process was cancelled before startup")
        kernel_tracker.poll(deadline=deadline_monotonic)
    except BaseException as primary_exception:
        startup_cleanup_errors: list[BaseException] = []
        if process is not None:
            try:
                _terminate_blocked_root(process, deadline=deadline_monotonic)
            except BaseException as exc:
                startup_cleanup_errors.append(exc)
        for descriptor in (gate_read, gate_write):
            if descriptor < 0:
                continue
            try:
                os.close(descriptor)
            except BaseException as exc:
                startup_cleanup_errors.append(exc)
        if kernel_tracker is not None:
            try:
                kernel_tracker.close()
            except BaseException as exc:
                startup_cleanup_errors.append(exc)
        _close_file_descriptors(darwin_pipe_anchor_fds)
        if startup_cleanup_errors:
            cleanup_group = BaseExceptionGroup(
                "contained subprocess startup cleanup failures",
                startup_cleanup_errors,
            )
            primary_exception.cleanup_error_group = cleanup_group  # type: ignore[attr-defined]
            primary_exception.add_note("contained subprocess startup cleanup also failed")
        raise

    assert process is not None
    assert kernel_tracker is not None

    def observe(deadline: float) -> dict[int, _ProcessObservation]:
        if inventory_provider is process_inventory:
            return process_inventory(
                deadline,
                containment_token=containment_token,
                started_at_or_after=root_started,
                darwin_pipe_markers=darwin_pipe_markers,
            )
        return inventory_provider(deadline)

    initial_inventory: Mapping[int, _ProcessObservation] | None = None
    previous_handlers: dict[int, object] = {}

    signal_latch = _ContainedSignalLatch()

    for signum in (signal.SIGINT, signal.SIGTERM):
        previous = signal.getsignal(signum)
        if previous is signal.SIG_IGN:
            continue
        try:
            signal.signal(signum, signal_latch.handle)
        except ValueError:
            break
        previous_handlers[signum] = previous

    body_completed = False
    known: dict[int, ProcessIdentity] = {}
    known_lock = threading.Lock()
    tracker_stop = threading.Event()
    tracker_thread: threading.Thread | None = None
    tracker_errors: list[BaseException] = []
    cleanup_errors: list[BaseException] = []
    tracker_last_inventory: Mapping[int, _ProcessObservation] | None = None
    root_exit_observed_at: float | None = None
    last_inventory: Mapping[int, _ProcessObservation] | None = None
    stdout = stderr = ""

    def stop_tracker() -> None:
        nonlocal tracker_thread
        tracker_stop.set()
        if tracker_thread is not None:
            tracker_thread.join(timeout=max(0.0, deadline_monotonic - clock()))
            if tracker_thread.is_alive():
                cleanup_errors.append(
                    ContainedProcessError("process containment tracker did not stop")
                )
                return
            tracker_thread = None

    def track_process_tree() -> None:
        nonlocal tracker_last_inventory
        try:
            while not tracker_stop.is_set():
                tracker_deadline = min(execution_deadline, time.monotonic() + 0.05)
                try:
                    inventory = observe(tracker_deadline)
                except TimeoutError:
                    if time.monotonic() >= execution_deadline:
                        return
                    continue
                with known_lock:
                    known.update(_discover_descendants(process.pid, inventory, known))
                    tracked = tuple(known.values())
                    tracker_last_inventory = inventory
                if process.poll() is not None:
                    for identity in tracked:
                        _signal_identity(identity, signal.SIGSTOP, inventory)
                tracker_stop.wait(0.001)
        except BaseException as exc:
            tracker_errors.append(exc)

    try:
        initial_inventory = observe(execution_deadline)
        last_inventory = initial_inventory
        observed_root = initial_inventory.get(process.pid)
        if observed_root is None:
            raise ContainedProcessError("startup inventory cannot prove root identity")
        if observed_root.identity != root_identity:
            raise ContainedProcessError("inventory root identity differs from kernel tracker")
        known.update(_discover_descendants(process.pid, initial_inventory, known))
        _merge_kernel_identities(
            known,
            kernel_tracker,
            root_pid=process.pid,
            deadline=deadline_monotonic,
        )
        if inventory_provider is process_inventory:
            tracker_thread = threading.Thread(
                target=track_process_tree,
                name=f"rquant-containment-{process.pid}",
                daemon=True,
            )
            tracker_thread.start()
        os.write(gate_write, b"1")
        os.close(gate_write)
        gate_write = -1
        while True:
            if tracker_errors:
                raise ContainedProcessError(
                    "process containment tracker failed"
                ) from tracker_errors[0]
            if cancellation_check is not None and cancellation_check():
                raise ContainedProcessError("contained process was cancelled")
            _merge_kernel_identities(
                known,
                kernel_tracker,
                root_pid=process.pid,
                deadline=deadline_monotonic,
            )
            remaining = execution_deadline - clock()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(list(args), 0)
            try:
                inventory = observe(execution_deadline)
            except (ContainedProcessError, TimeoutError):
                if clock() >= execution_deadline:
                    raise subprocess.TimeoutExpired(list(args), 0) from None
                raise
            last_inventory = inventory
            observed_root = inventory.get(process.pid)
            if observed_root is not None:
                if root_identity is not None and observed_root.identity != root_identity:
                    raise ContainedProcessError("subprocess PID identity changed while running")
                root_identity = observed_root.identity
            with known_lock:
                known.update(_discover_descendants(process.pid, inventory, known))
            if process.poll() is not None:
                alive_descendants = {
                    pid: identity
                    for pid, identity in known.items()
                    if (
                        (pid in inventory and inventory[pid].identity == identity)
                        or (
                            (observed := _process_observation(pid)) is not None
                            and observed.identity == identity
                        )
                    )
                }
                if alive_descendants:
                    root_exit_observed_at = root_exit_observed_at or clock()
                    if clock() - root_exit_observed_at >= 0.05:
                        raise ContainedProcessError(
                            "subprocess root exited while descendants were still running"
                        )
                    sleep(min(0.005, max(0.0, execution_deadline - clock())))
                    continue
            try:
                stdout, stderr = process.communicate(timeout=min(0.02, remaining))
                _merge_kernel_identities(
                    known,
                    kernel_tracker,
                    root_pid=process.pid,
                    deadline=deadline_monotonic,
                )
                break
            except subprocess.TimeoutExpired:
                continue
        body_completed = True
    except subprocess.TimeoutExpired:
        tracker_stop.set()
        if tracker_last_inventory is not None:
            last_inventory = tracker_last_inventory
        try:
            _cleanup_process_tree(
                process,
                known,
                root_identity=root_identity,
                deadline=deadline_monotonic,
                inventory_provider=observe,
                clock=clock,
                sleep=sleep,
                kernel_tracker=kernel_tracker,
                initial_inventory=last_inventory,
            )
        except BaseException as exc:
            cleanup_errors.append(exc)
        raise
    except _ContainedSignal:
        tracker_stop.set()
        if tracker_last_inventory is not None:
            last_inventory = tracker_last_inventory
        try:
            _cleanup_process_tree(
                process,
                known,
                root_identity=root_identity,
                deadline=deadline_monotonic,
                inventory_provider=observe,
                clock=clock,
                sleep=sleep,
                kernel_tracker=kernel_tracker,
                initial_inventory=last_inventory,
            )
        except BaseException as cleanup_exc:
            cleanup_errors.append(cleanup_exc)
        body_completed = True
    except BaseException:
        tracker_stop.set()
        if tracker_last_inventory is not None:
            last_inventory = tracker_last_inventory
        try:
            _cleanup_process_tree(
                process,
                known,
                root_identity=root_identity,
                deadline=deadline_monotonic,
                inventory_provider=observe,
                clock=clock,
                sleep=sleep,
                kernel_tracker=kernel_tracker,
                initial_inventory=last_inventory,
            )
        except BaseException as exc:
            cleanup_errors.append(exc)
        raise
    finally:
        signal_latch.defer()
        primary_exception = sys.exception()
        if gate_write >= 0:
            try:
                os.close(gate_write)
            except BaseException as exc:
                cleanup_errors.append(exc)
        if tracker_thread is not None:
            try:
                stop_tracker()
            except BaseException as exc:
                cleanup_errors.append(exc)
        try:
            kernel_tracker.close()
        except BaseException as exc:
            cleanup_errors.append(exc)
        for signum, previous in previous_handlers.items():
            try:
                signal.signal(signum, previous)
            except BaseException as exc:
                cleanup_errors.append(exc)
        if not body_completed or cleanup_errors:
            _close_file_descriptors(darwin_pipe_anchor_fds)
        cleanup_group: BaseExceptionGroup | None = None
        if cleanup_errors:
            details = "; ".join(str(error) or type(error).__name__ for error in cleanup_errors)
            cleanup_group = BaseExceptionGroup(
                "contained subprocess cleanup failures",
                cleanup_errors,
            )
            if primary_exception is not None and signal_latch.first_signum is None:
                primary_exception.cleanup_error_group = cleanup_group  # type: ignore[attr-defined]
                primary_exception.add_note(f"contained subprocess cleanup also failed: {details}")
            elif signal_latch.first_signum is None:
                raise ContainedProcessError(
                    f"contained subprocess cleanup failed: {details}"
                ) from cleanup_group
        if signal_latch.first_signum is not None:
            _close_file_descriptors(darwin_pipe_anchor_fds)
            signum = signal_latch.first_signum
            previous = previous_handlers[signum]
            if callable(previous):
                try:
                    previous(signum, None)
                except BaseException as replay_error:
                    if cleanup_group is not None:
                        replay_error.cleanup_error_group = cleanup_group  # type: ignore[attr-defined]
                        replay_error.add_note("contained subprocess cleanup also failed")
                    raise
                interrupted = InterruptedError(f"process runner interrupted by signal {signum}")
                if cleanup_group is not None:
                    interrupted.cleanup_error_group = cleanup_group  # type: ignore[attr-defined]
                    interrupted.add_note("contained subprocess cleanup also failed")
                raise interrupted
            signal.signal(signum, signal.SIG_DFL)
            os.kill(os.getpid(), signum)
            terminated = SystemExit(128 + signum)
            if cleanup_group is not None:
                terminated.cleanup_error_group = cleanup_group  # type: ignore[attr-defined]
                terminated.add_note("contained subprocess cleanup also failed")
            raise terminated
    try:
        remaining = deadline_monotonic - clock()
        if remaining <= 0:
            raise subprocess.TimeoutExpired(list(args), 0)
        inventory = observe(deadline_monotonic)
        known.update(_discover_descendants(process.pid, inventory, known))
        alive = {
            pid: identity
            for pid, identity in known.items()
            if (
                (pid in inventory and inventory[pid].identity == identity)
                or (
                    (observed := _process_observation(pid)) is not None
                    and observed.identity == identity
                )
            )
        }
        if alive:
            _cleanup_process_tree(
                process,
                known,
                root_identity=root_identity,
                deadline=deadline_monotonic,
                inventory_provider=observe,
                clock=clock,
                sleep=sleep,
            )
            raise ContainedProcessError("subprocess descendants outlived their root")
        completed = subprocess.CompletedProcess(list(args), process.returncode, stdout, stderr)
        if check and completed.returncode != 0:
            raise subprocess.CalledProcessError(
                completed.returncode,
                list(args),
                output=stdout,
                stderr=stderr,
            )
        return completed
    finally:
        _close_file_descriptors(darwin_pipe_anchor_fds)


def _contained_child_main(arguments: list[str]) -> int:
    if len(arguments) < 3 or arguments[1] != "--":
        return 127
    gate_fd = int(arguments[0])
    command = arguments[2:]
    if not command:
        return 127
    signal_byte = os.read(gate_fd, 1)
    os.close(gate_fd)
    if signal_byte != b"1":
        return 127
    os.execvpe(command[0], command, os.environ)
    return 127


if __name__ == "__main__" and len(sys.argv) >= 2 and sys.argv[1] == "--contained-child":
    raise SystemExit(_contained_child_main(sys.argv[2:]))
