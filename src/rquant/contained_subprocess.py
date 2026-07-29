"""Bounded subprocess execution with descendant containment.

The module is deliberately standard-library only so the deployment bootstrap can load
the exact immutable generation copy before importing the rest of :mod:`rquant`.
"""

from __future__ import annotations

import ctypes
import os
import signal
import struct
import subprocess
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path


class ContainedProcessError(RuntimeError):
    """A process tree could not be started, stopped, or proven contained."""


class _ContainedSignal(BaseException):
    def __init__(self, signum: int) -> None:
        self.signum = signum


@dataclass(frozen=True, order=True)
class ProcessIdentity:
    pid: int
    started: tuple[int, int]


@dataclass(frozen=True)
class _ProcessObservation:
    identity: ProcessIdentity
    parent_pid: int


Clock = Callable[[], float]
Sleep = Callable[[float], None]
Inventory = Callable[[float], dict[int, _ProcessObservation]]


def _darwin_process_inventory(deadline: float) -> dict[int, _ProcessObservation]:
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
            _flags, _status, _xstatus, observed_pid, parent = struct.unpack_from(
                "=IIIII", buffer.raw
            )
            start_seconds, start_microseconds = struct.unpack_from("=QQ", buffer.raw, 120)
            if observed_pid != pid or start_seconds <= 0:
                continue
            result[pid] = _ProcessObservation(
                identity=ProcessIdentity(pid, (start_seconds, start_microseconds)),
                parent_pid=parent,
            )
        return result
    except (OSError, ValueError) as exc:
        raise ContainedProcessError("process inventory failed") from exc


def _linux_process_inventory(deadline: float) -> dict[int, _ProcessObservation]:
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
            result[pid] = _ProcessObservation(
                identity=ProcessIdentity(pid, (int(fields[19]), 0)),
                parent_pid=int(fields[1]),
            )
    except (OSError, ValueError) as exc:
        raise ContainedProcessError("process inventory failed") from exc
    return result


def process_inventory(deadline: float) -> dict[int, _ProcessObservation]:
    if time.monotonic() >= deadline:
        raise TimeoutError("process inventory deadline expired")
    if sys.platform == "darwin":
        return _darwin_process_inventory(deadline)
    if sys.platform.startswith("linux"):
        return _linux_process_inventory(deadline)
    raise ContainedProcessError("process inventory is unsupported on this platform")


def _discover_descendants(
    root_pid: int,
    inventory: Mapping[int, _ProcessObservation],
    known: Mapping[int, ProcessIdentity],
) -> dict[int, ProcessIdentity]:
    parents: dict[int, set[int]] = {}
    for observation in inventory.values():
        parents.setdefault(observation.parent_pid, set()).add(observation.identity.pid)
    pending = [root_pid, *known]
    descendants = dict(known)
    visited: set[int] = set()
    while pending:
        parent = pending.pop()
        if parent in visited:
            continue
        visited.add(parent)
        for pid in parents.get(parent, ()):
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


def _cleanup_process_tree(
    process: subprocess.Popen[str],
    known: dict[int, ProcessIdentity],
    *,
    root_identity: ProcessIdentity | None = None,
    deadline: float,
    inventory_provider: Inventory,
    clock: Clock,
    sleep: Sleep,
    initial_inventory: Mapping[int, _ProcessObservation] | None = None,
) -> None:
    if initial_inventory is not None:
        if root_identity is not None:
            _signal_identity(root_identity, signal.SIGSTOP, initial_inventory)
            root = initial_inventory.get(process.pid)
            if root is not None and root.identity == root_identity:
                with suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGSTOP)
        for identity in tuple(known.values()):
            _signal_identity(identity, signal.SIGSTOP, initial_inventory)
    stable = 0
    prior: frozenset[ProcessIdentity] = frozenset()
    while stable < 2:
        remaining = deadline - clock()
        if remaining <= 0:
            raise ContainedProcessError("process containment deadline expired")
        inventory = inventory_provider(deadline)
        known.update(_discover_descendants(process.pid, inventory, known))
        if root_identity is not None:
            _signal_identity(root_identity, signal.SIGSTOP, inventory)
            root = inventory.get(process.pid)
            if root is not None and root.identity == root_identity:
                with suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGSTOP)
        for identity in tuple(known.values()):
            _signal_identity(identity, signal.SIGSTOP, inventory)
        current = frozenset(known.values())
        stable = stable + 1 if current == prior else 0
        prior = current
        if stable < 2:
            sleep(min(0.01, max(0.0, deadline - clock())))

    inventory = inventory_provider(deadline)
    for identity in sorted(known.values(), reverse=True):
        _signal_identity(identity, signal.SIGKILL, inventory)
    if root_identity is not None:
        _signal_identity(root_identity, signal.SIGKILL, inventory)
        root = inventory.get(process.pid)
        if root is not None and root.identity == root_identity:
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)

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
        alive = {
            pid: identity
            for pid, identity in known.items()
            if pid in inventory and inventory[pid].identity == identity
        }
        if not alive:
            return
        for identity in alive.values():
            _signal_identity(identity, signal.SIGKILL, inventory)
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
) -> subprocess.CompletedProcess[str]:
    """Run one command and contain every observed descendant within one deadline."""

    remaining = deadline_monotonic - clock()
    if remaining <= 0:
        raise subprocess.TimeoutExpired(list(args), 0)
    cleanup_reserve = min(1.0, max(0.1, remaining * 0.5))
    execution_deadline = deadline_monotonic - cleanup_reserve
    if execution_deadline <= clock():
        raise subprocess.TimeoutExpired(list(args), 0)
    process = subprocess.Popen(
        list(args),
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=text,
        start_new_session=True,
        pass_fds=pass_fds,
        env=None if env is None else dict(env),
    )
    previous_handlers: dict[int, object] = {}

    def forward_signal(signum: int, _frame: object) -> None:
        raise _ContainedSignal(signum)

    for signum in (signal.SIGINT, signal.SIGTERM):
        previous = signal.getsignal(signum)
        if previous is signal.SIG_IGN:
            continue
        try:
            signal.signal(signum, forward_signal)
        except ValueError:
            break
        previous_handlers[signum] = previous

    caught_signal: _ContainedSignal | None = None
    known: dict[int, ProcessIdentity] = {}
    root_identity: ProcessIdentity | None = None
    root_exit_observed_at: float | None = None
    last_inventory: Mapping[int, _ProcessObservation] | None = None
    stdout = stderr = ""
    try:
        while True:
            if cancellation_check is not None and cancellation_check():
                raise ContainedProcessError("contained process was cancelled")
            remaining = execution_deadline - clock()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(list(args), 0)
            try:
                inventory = inventory_provider(execution_deadline)
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
            known.update(_discover_descendants(process.pid, inventory, known))
            if process.poll() is not None:
                alive_descendants = {
                    pid: identity
                    for pid, identity in known.items()
                    if pid in inventory and inventory[pid].identity == identity
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
                break
            except subprocess.TimeoutExpired:
                continue
    except subprocess.TimeoutExpired:
        _cleanup_process_tree(
            process,
            known,
            root_identity=root_identity,
            deadline=deadline_monotonic,
            inventory_provider=inventory_provider,
            clock=clock,
            sleep=sleep,
            initial_inventory=last_inventory,
        )
        raise
    except _ContainedSignal as exc:
        caught_signal = exc
        _cleanup_process_tree(
            process,
            known,
            root_identity=root_identity,
            deadline=deadline_monotonic,
            inventory_provider=inventory_provider,
            clock=clock,
            sleep=sleep,
            initial_inventory=last_inventory,
        )
    except BaseException:
        _cleanup_process_tree(
            process,
            known,
            root_identity=root_identity,
            deadline=deadline_monotonic,
            inventory_provider=inventory_provider,
            clock=clock,
            sleep=sleep,
            initial_inventory=last_inventory,
        )
        raise
    finally:
        for signum, previous in previous_handlers.items():
            signal.signal(signum, previous)

    if caught_signal is not None:
        previous = previous_handlers[caught_signal.signum]
        if callable(previous):
            previous(caught_signal.signum, None)
            raise InterruptedError(f"process runner interrupted by signal {caught_signal.signum}")
        signal.signal(caught_signal.signum, signal.SIG_DFL)
        os.kill(os.getpid(), caught_signal.signum)
        raise SystemExit(128 + caught_signal.signum)
    remaining = deadline_monotonic - clock()
    if remaining <= 0:
        raise subprocess.TimeoutExpired(list(args), 0)
    inventory = inventory_provider(deadline_monotonic)
    known.update(_discover_descendants(process.pid, inventory, known))
    alive = {
        pid: identity
        for pid, identity in known.items()
        if pid in inventory and inventory[pid].identity == identity
    }
    if alive:
        _cleanup_process_tree(
            process,
            known,
            root_identity=root_identity,
            deadline=deadline_monotonic,
            inventory_provider=inventory_provider,
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
