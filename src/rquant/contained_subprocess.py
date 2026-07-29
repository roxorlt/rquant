"""Bounded subprocess execution with descendant containment.

The module is deliberately standard-library only so the deployment bootstrap can load
the exact immutable generation copy before importing the rest of :mod:`rquant`.
"""

from __future__ import annotations

import ctypes
import errno
import os
import secrets
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
    containment_token: bool = False


Clock = Callable[[], float]
Sleep = Callable[[float], None]
Inventory = Callable[[float], dict[int, _ProcessObservation]]

_CONTAINMENT_ENVIRONMENT_KEY = "RQUANT_CONTAINMENT_TOKEN"
_MAX_PROCESS_ARGUMENT_BYTES = 4 * 1024 * 1024


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
            result[pid] = _ProcessObservation(
                identity=ProcessIdentity(pid, (start_seconds, start_microseconds)),
                parent_pid=parent,
                containment_token=(
                    containment_token is not None
                    and (
                        started_at_or_after is None
                        or (start_seconds, start_microseconds) >= started_at_or_after
                    )
                    and _darwin_process_has_token(pid, containment_token, deadline=deadline)
                ),
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
) -> dict[int, _ProcessObservation]:
    if time.monotonic() >= deadline:
        raise TimeoutError("process inventory deadline expired")
    if sys.platform == "darwin":
        return _darwin_process_inventory(
            deadline,
            containment_token=containment_token,
            started_at_or_after=started_at_or_after,
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
    for observation in inventory.values():
        parents.setdefault(observation.parent_pid, set()).add(observation.identity.pid)
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
    containment_token = secrets.token_hex(32)
    process_environment = dict(os.environ if env is None else env)
    process_environment[_CONTAINMENT_ENVIRONMENT_KEY] = containment_token
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

    root_started: tuple[int, int] | None = None

    def observe(deadline: float) -> dict[int, _ProcessObservation]:
        if inventory_provider is process_inventory:
            return process_inventory(
                deadline,
                containment_token=containment_token,
                started_at_or_after=root_started,
            )
        return inventory_provider(deadline)

    initial_inventory: Mapping[int, _ProcessObservation] | None = None
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
    known_lock = threading.Lock()
    tracker_stop = threading.Event()
    tracker_thread: threading.Thread | None = None
    tracker_errors: list[BaseException] = []
    tracker_last_inventory: Mapping[int, _ProcessObservation] | None = None
    root_identity: ProcessIdentity | None = None
    root_exit_observed_at: float | None = None
    last_inventory: Mapping[int, _ProcessObservation] | None = None
    stdout = stderr = ""

    def stop_tracker() -> None:
        nonlocal tracker_thread
        tracker_stop.set()
        if tracker_thread is not None:
            tracker_thread.join(timeout=max(0.0, min(0.2, deadline_monotonic - clock())))
            if tracker_thread.is_alive():
                raise ContainedProcessError("process containment tracker did not stop")
            tracker_thread = None

    def track_process_tree() -> None:
        nonlocal tracker_last_inventory
        try:
            while not tracker_stop.is_set():
                tracker_deadline = min(execution_deadline, time.monotonic() + 0.05)
                try:
                    inventory = process_inventory(tracker_deadline)
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
        initial_inventory = (
            process_inventory(execution_deadline)
            if inventory_provider is process_inventory
            else observe(execution_deadline)
        )
        last_inventory = initial_inventory
        observed_root = initial_inventory.get(process.pid)
        if observed_root is not None:
            root_identity = observed_root.identity
            root_started = root_identity.started
        known.update(_discover_descendants(process.pid, initial_inventory, known))
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
        stop_tracker()
        if tracker_last_inventory is not None:
            last_inventory = tracker_last_inventory
        _cleanup_process_tree(
            process,
            known,
            root_identity=root_identity,
            deadline=deadline_monotonic,
            inventory_provider=observe,
            clock=clock,
            sleep=sleep,
            initial_inventory=last_inventory,
        )
        raise
    except _ContainedSignal as exc:
        caught_signal = exc
        stop_tracker()
        if tracker_last_inventory is not None:
            last_inventory = tracker_last_inventory
        _cleanup_process_tree(
            process,
            known,
            root_identity=root_identity,
            deadline=deadline_monotonic,
            inventory_provider=observe,
            clock=clock,
            sleep=sleep,
            initial_inventory=last_inventory,
        )
    except BaseException:
        stop_tracker()
        if tracker_last_inventory is not None:
            last_inventory = tracker_last_inventory
        _cleanup_process_tree(
            process,
            known,
            root_identity=root_identity,
            deadline=deadline_monotonic,
            inventory_provider=observe,
            clock=clock,
            sleep=sleep,
            initial_inventory=last_inventory,
        )
        raise
    finally:
        if gate_write >= 0:
            os.close(gate_write)
        if tracker_thread is not None:
            stop_tracker()
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
    inventory = observe(deadline_monotonic)
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
