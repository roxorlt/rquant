from __future__ import annotations

import ctypes
import errno
import inspect
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from types import SimpleNamespace

import pytest

from rquant import contained_subprocess as contained

_REAL_SIGNAL = signal.signal
_REAL_PTHREAD_SIGMASK = getattr(signal, "pthread_sigmask", None)
_MANAGED_TEST_SIGNALS = (signal.SIGINT, signal.SIGTERM)


def _prepare_unblocked_signal_host() -> tuple[
    dict[int, object],
    set[signal.Signals],
    set[signal.Signals],
]:
    assert _REAL_PTHREAD_SIGMASK is not None
    host_handlers = {signum: signal.getsignal(signum) for signum in _MANAGED_TEST_SIGNALS}
    host_mask = _REAL_PTHREAD_SIGMASK(signal.SIG_BLOCK, set())
    starting_mask = host_mask.difference(_MANAGED_TEST_SIGNALS)
    _REAL_PTHREAD_SIGMASK(signal.SIG_SETMASK, starting_mask)
    return host_handlers, host_mask, starting_mask


def _restore_signal_host(
    host_handlers: dict[int, object],
    host_mask: set[signal.Signals],
) -> None:
    assert _REAL_PTHREAD_SIGMASK is not None
    _REAL_PTHREAD_SIGMASK(signal.SIG_BLOCK, set(_MANAGED_TEST_SIGNALS))
    for signum, previous in host_handlers.items():
        _REAL_SIGNAL(signum, previous)  # type: ignore[arg-type]
    _REAL_PTHREAD_SIGMASK(signal.SIG_SETMASK, host_mask)


@pytest.fixture(autouse=True)
def _restore_host_signal_state() -> Iterator[None]:
    watched = (signal.SIGINT, signal.SIGTERM)
    before_handlers = {signum: signal.getsignal(signum) for signum in watched}
    before_mask = (
        _REAL_PTHREAD_SIGMASK(signal.SIG_BLOCK, set())
        if _REAL_PTHREAD_SIGMASK is not None
        else None
    )
    try:
        yield
    finally:
        if _REAL_PTHREAD_SIGMASK is not None:
            _REAL_PTHREAD_SIGMASK(signal.SIG_BLOCK, set(watched))
        for signum, previous in before_handlers.items():
            _REAL_SIGNAL(signum, previous)
        if _REAL_PTHREAD_SIGMASK is not None and before_mask is not None:
            _REAL_PTHREAD_SIGMASK(signal.SIG_SETMASK, before_mask)


class _FinishedProcess:
    pid = 100
    returncode = -signal.SIGKILL

    def communicate(self, *, timeout: float) -> tuple[str, str]:
        assert timeout > 0
        return "", ""


def _observation(pid: int, parent: int, started: int) -> contained._ProcessObservation:
    return contained._ProcessObservation(
        identity=contained.ProcessIdentity(pid, (started, 0)),
        parent_pid=parent,
    )


def _linux_stat_payload(pid: int, parent: int, started: int | str) -> str:
    fields = ["S", str(parent), *(["0"] * 17), str(started)]
    return f"{pid} (short lived worker) {' '.join(fields)}"


class _FakeLinuxProcStat:
    def __init__(self, payload: str | OSError) -> None:
        self._payload = payload

    def read_text(self, *, encoding: str) -> str:
        assert encoding == "ascii"
        if isinstance(self._payload, OSError):
            raise self._payload
        return self._payload


class _FakeLinuxProcEntry:
    def __init__(self, pid: int, payload: str | OSError) -> None:
        self.name = str(pid)
        self._stat = _FakeLinuxProcStat(payload)

    def __truediv__(self, child: str) -> _FakeLinuxProcStat:
        assert child == "stat"
        return self._stat


class _FakeLinuxProcRoot:
    def __init__(self, entries: tuple[_FakeLinuxProcEntry, ...]) -> None:
        self._entries = entries

    def iterdir(self) -> Iterator[_FakeLinuxProcEntry]:
        return iter(self._entries)


def _install_fake_linux_proc(
    monkeypatch: pytest.MonkeyPatch,
    *entries: _FakeLinuxProcEntry,
) -> None:
    root = _FakeLinuxProcRoot(entries)

    def path(value: str) -> _FakeLinuxProcRoot:
        assert value == "/proc"
        return root

    monkeypatch.setattr(contained, "Path", path)


def _close_test_fd_if_open(descriptor: int) -> None:
    try:
        contained.os.fstat(descriptor)
    except OSError as exc:
        if exc.errno != contained.errno.EBADF:
            raise
    else:
        contained.os.close(descriptor)


class _NextContainedLineFault:
    def __init__(self, error: BaseException) -> None:
        self._error = error
        self._armed = False

    def arm(self) -> None:
        self._armed = True

    def trace(self, frame: object, event: str, _arg: object) -> object:
        if (
            self._armed
            and event == "line"
            and getattr(getattr(frame, "f_code", None), "co_filename", None) == contained.__file__
        ):
            self._armed = False
            raise self._error
        return self.trace


def _clear_execution_hooks() -> None:
    sys.settrace(None)
    sys.setprofile(None)


def _restore_execution_hooks_for_test(trace_hook: object, profile_hook: object) -> None:
    sys.settrace(None)
    sys.setprofile(None)
    sys.settrace(trace_hook)
    sys.setprofile(profile_hook)


class _CountingQueue:
    def __init__(self, descriptor: int, close: Callable[[], None]) -> None:
        self._descriptor = descriptor
        self._close = close
        self.close_count = 0

    def fileno(self) -> int:
        return self._descriptor

    def close(self) -> None:
        self.close_count += 1
        self._close()


def _open_file_descriptors() -> set[int]:
    return {int(entry) for entry in os.listdir("/dev/fd") if entry.isdigit()}


@pytest.mark.parametrize(
    ("use_trace", "use_profile"),
    ((True, False), (False, True), (True, True)),
    ids=("trace", "profile", "both"),
)
def test_active_execution_hooks_fail_before_contained_acquisition(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    use_trace: bool,
    use_profile: bool,
) -> None:
    original_trace = sys.gettrace()
    original_profile = sys.getprofile()
    calls = {"signal_latch": 0, "tracker": 0, "pipe": 0, "popen": 0}
    before_fds = _open_file_descriptors()
    real_install_signal_latch = contained._install_signal_latch

    def trace_hook(_frame: object, _event: str, _arg: object) -> object:
        return trace_hook

    def profile_hook(_frame: object, _event: str, _arg: object) -> None:
        return None

    def install_signal_latch(
        latch: contained._ContainedSignalLatch,
    ) -> tuple[dict[int, object], frozenset[int]]:
        calls["signal_latch"] += 1
        return real_install_signal_latch(latch)

    def create_tracker() -> contained._KernelProcessTracker:
        calls["tracker"] += 1
        raise AssertionError("tracker acquisition must not run")

    def create_pipe() -> tuple[int, int]:
        calls["pipe"] += 1
        raise AssertionError("pipe acquisition must not run")

    def create_process(*_args: object, **_kwargs: object) -> object:
        calls["popen"] += 1
        raise AssertionError("process acquisition must not run")

    monkeypatch.setattr(contained, "_install_signal_latch", install_signal_latch)
    monkeypatch.setattr(contained.os, "pipe", create_pipe)
    monkeypatch.setattr(contained.subprocess, "Popen", create_process)
    try:
        sys.settrace(trace_hook if use_trace else None)
        sys.setprofile(profile_hook if use_profile else None)

        with pytest.raises(
            contained.ContainedProcessError,
            match="contained acquisition does not support active execution hooks",
        ):
            contained.run_contained(
                [sys.executable, "-c", "pass"],
                cwd=tmp_path,
                deadline_monotonic=time.monotonic() + 1,
                kernel_tracker_factory=create_tracker,
                may_spawn_background_descendants=False,
            )

        assert sys.gettrace() is (trace_hook if use_trace else None)
        assert sys.getprofile() is (profile_hook if use_profile else None)
        assert calls == {"signal_latch": 0, "tracker": 0, "pipe": 0, "popen": 0}
        assert _open_file_descriptors() == before_fds
        assert not tuple(tmp_path.glob("*.building"))
    finally:
        _restore_execution_hooks_for_test(original_trace, original_profile)


@pytest.mark.parametrize("hook_kind", ("trace", "profile"))
def test_active_execution_hook_blocks_pidfd_before_open(
    monkeypatch: pytest.MonkeyPatch,
    hook_kind: str,
) -> None:
    original_trace = sys.gettrace()
    original_profile = sys.getprofile()
    tracker = contained._LinuxSubreaperProcessTracker()
    calls = 0
    before_fds = _open_file_descriptors()

    def trace_hook(_frame: object, _event: str, _arg: object) -> object:
        return trace_hook

    def profile_hook(_frame: object, _event: str, _arg: object) -> None:
        return None

    def open_pidfd(_pid: int, _flags: int) -> int:
        nonlocal calls
        calls += 1
        raise AssertionError("pidfd acquisition must not run")

    monkeypatch.setattr(contained.os, "pidfd_open", open_pidfd, raising=False)
    try:
        sys.settrace(trace_hook if hook_kind == "trace" else None)
        sys.setprofile(profile_hook if hook_kind == "profile" else None)

        with pytest.raises(
            contained.ContainedProcessError,
            match="contained acquisition does not support active execution hooks",
        ):
            tracker._bind_pid(contained.ProcessIdentity(101, (1, 0)))

        assert calls == 0
        assert sys.gettrace() is (trace_hook if hook_kind == "trace" else None)
        assert sys.getprofile() is (profile_hook if hook_kind == "profile" else None)
        assert tracker._pidfds == {}
        assert tracker._pending_pidfds == []
        assert _open_file_descriptors() == before_fds
    finally:
        _restore_execution_hooks_for_test(original_trace, original_profile)


@pytest.mark.parametrize(
    ("use_trace", "use_profile"),
    ((True, False), (False, True), (True, True)),
    ids=("trace", "profile", "both"),
)
def test_linux_register_root_rejects_hooks_before_tracker_state_changes(
    monkeypatch: pytest.MonkeyPatch,
    use_trace: bool,
    use_profile: bool,
) -> None:
    original_trace = sys.gettrace()
    original_profile = sys.getprofile()
    tracker = contained._LinuxSubreaperProcessTracker()
    identity = contained.ProcessIdentity(101, (1, 0))
    calls = {"subreaper": 0, "observe": 0, "pidfd": 0}

    def trace_hook(_frame: object, _event: str, _arg: object) -> object:
        return trace_hook

    def profile_hook(_frame: object, _event: str, _arg: object) -> None:
        return None

    def enable_subreaper(_deadline: float) -> None:
        calls["subreaper"] += 1

    def observe(_pid: int) -> contained._ProcessObservation:
        calls["observe"] += 1
        return contained._ProcessObservation(identity=identity, parent_pid=1)

    def open_pidfd(_pid: int, _flags: int) -> int:
        calls["pidfd"] += 1
        raise AssertionError("pidfd acquisition must not run")

    monkeypatch.setattr(tracker, "_enable_subreaper", enable_subreaper)
    monkeypatch.setattr(contained, "_linux_process_observation", observe)
    monkeypatch.setattr(contained.os, "pidfd_open", open_pidfd, raising=False)
    try:
        sys.settrace(trace_hook if use_trace else None)
        sys.setprofile(profile_hook if use_profile else None)

        with pytest.raises(
            contained.ContainedProcessError,
            match="contained acquisition does not support active execution hooks",
        ):
            tracker.register_root(identity.pid, deadline=time.monotonic() + 1)

        assert calls == {"subreaper": 0, "observe": 0, "pidfd": 0}
        assert tracker._root_pid is None
        assert tracker._root_started is None
        assert tracker._known == {}
        assert tracker._pidfds == {}
        assert tracker._pending_pidfds == []
        assert tracker._subreaper_lifecycle == "NEW"
        assert tracker._subreaper_record is None
        assert sys.gettrace() is (trace_hook if use_trace else None)
        assert sys.getprofile() is (profile_hook if use_profile else None)
    finally:
        _restore_execution_hooks_for_test(original_trace, original_profile)


@pytest.mark.parametrize("hook_kind", ("trace", "profile"))
def test_linux_enable_subreaper_rejects_hooks_before_lock_and_prctl(
    monkeypatch: pytest.MonkeyPatch,
    hook_kind: str,
) -> None:
    original_trace = sys.gettrace()
    original_profile = sys.getprofile()
    tracker = contained._LinuxSubreaperProcessTracker()
    calls = {"lock": 0, "cdll": 0, "prctl": 0}

    class Lock:
        def acquire(self, *, timeout: float) -> bool:
            assert timeout > 0
            calls["lock"] += 1
            return True

    class Libc:
        def prctl(self, *_args: object) -> int:
            calls["prctl"] += 1
            return 1

    def load_libc(*_args: object, **_kwargs: object) -> Libc:
        calls["cdll"] += 1
        return Libc()

    def trace_hook(_frame: object, _event: str, _arg: object) -> object:
        return trace_hook

    def profile_hook(_frame: object, _event: str, _arg: object) -> None:
        return None

    monkeypatch.setattr(contained, "_LINUX_SUBREAPER_LOCK", Lock())
    monkeypatch.setattr(contained.ctypes, "CDLL", load_libc)
    try:
        sys.settrace(trace_hook if hook_kind == "trace" else None)
        sys.setprofile(profile_hook if hook_kind == "profile" else None)

        with pytest.raises(
            contained.ContainedProcessError,
            match="contained acquisition does not support active execution hooks",
        ):
            tracker._enable_subreaper(time.monotonic() + 1)

        assert calls == {"lock": 0, "cdll": 0, "prctl": 0}
        assert tracker._subreaper_lifecycle == "NEW"
        assert tracker._subreaper_record is None
        assert sys.gettrace() is (trace_hook if hook_kind == "trace" else None)
        assert sys.getprofile() is (profile_hook if hook_kind == "profile" else None)
    finally:
        _restore_execution_hooks_for_test(original_trace, original_profile)


@pytest.mark.parametrize("entrypoint", ("initialize", "register-process", "register-root"))
@pytest.mark.parametrize("hook_kind", ("trace", "profile", "both"))
def test_darwin_registration_rejects_hooks_before_initialized_queue_side_effects(
    monkeypatch: pytest.MonkeyPatch,
    entrypoint: str,
    hook_kind: str,
) -> None:
    original_trace = sys.gettrace()
    original_profile = sys.getprofile()
    tracker = contained._DarwinKqueueProcessTracker()
    identity = contained.ProcessIdentity(101, (1, 0))
    calls = {"initialize": 0, "observe": 0, "control": 0}

    class Queue:
        def control(self, *_args: object) -> list[object]:
            calls["control"] += 1
            return []

    def initialize() -> None:
        calls["initialize"] += 1
        raise AssertionError("initialized queue must not be consulted")

    def observe(_pid: int) -> contained._ProcessObservation:
        calls["observe"] += 1
        return contained._ProcessObservation(identity=identity, parent_pid=1)

    def trace_hook(_frame: object, _event: str, _arg: object) -> object:
        return trace_hook

    def profile_hook(_frame: object, _event: str, _arg: object) -> None:
        return None

    tracker._queue = Queue()
    tracker._owns_queue = True
    monkeypatch.setattr(contained, "_darwin_process_observation", observe)
    if entrypoint == "register-root":
        monkeypatch.setattr(tracker, "_initialize_queue", initialize)
    try:
        sys.settrace(trace_hook if hook_kind in {"trace", "both"} else None)
        sys.setprofile(profile_hook if hook_kind in {"profile", "both"} else None)

        with pytest.raises(
            contained.ContainedProcessError,
            match="contained acquisition does not support active execution hooks",
        ):
            if entrypoint == "initialize":
                tracker._initialize_queue()
            elif entrypoint == "register-process":
                tracker._register_process(identity, deadline=time.monotonic() + 1)
            else:
                tracker.register_root(identity.pid, deadline=time.monotonic() + 1)

        assert calls == {"initialize": 0, "observe": 0, "control": 0}
        assert tracker._registered == set()
        assert tracker._known == {}
        assert tracker._root_pid is None
        assert tracker._root_started is None
        assert tracker._thread is None
        assert sys.gettrace() is (trace_hook if hook_kind in {"trace", "both"} else None)
        assert sys.getprofile() is (profile_hook if hook_kind in {"profile", "both"} else None)
    finally:
        _restore_execution_hooks_for_test(original_trace, original_profile)


@pytest.mark.parametrize("hook_kind", ("trace", "profile", "both"))
@pytest.mark.parametrize("activation_boundary", ("control", "observation"))
def test_darwin_register_root_rechecks_hooks_after_registration_handoffs(
    monkeypatch: pytest.MonkeyPatch,
    hook_kind: str,
    activation_boundary: str,
) -> None:
    original_trace = sys.gettrace()
    original_profile = sys.getprofile()
    tracker = contained._DarwinKqueueProcessTracker()
    identity = contained.ProcessIdentity(101, (1, 0))
    calls = {"observe": 0, "control": 0, "thread": 0, "start": 0}
    observations_at_control = -1

    class Queue:
        def control(self, changes: object, *_args: object) -> list[object]:
            nonlocal observations_at_control
            assert changes is not None
            calls["control"] += 1
            observations_at_control = calls["observe"]
            if activation_boundary == "control":
                activate_hooks()
            return []

        def close(self) -> None:
            return None

    class Thread:
        def __init__(self, **_kwargs: object) -> None:
            calls["thread"] += 1

        def start(self) -> None:
            calls["start"] += 1

        def join(self, *, timeout: float) -> None:
            assert timeout >= 0

        def is_alive(self) -> bool:
            return False

    def observe(_pid: int) -> contained._ProcessObservation:
        calls["observe"] += 1
        if activation_boundary == "observation" and calls["observe"] == 3:
            activate_hooks()
        return contained._ProcessObservation(identity=identity, parent_pid=1)

    def trace_hook(_frame: object, _event: str, _arg: object) -> object:
        return trace_hook

    def profile_hook(_frame: object, _event: str, _arg: object) -> None:
        return None

    def activate_hooks() -> None:
        sys.settrace(trace_hook if hook_kind in {"trace", "both"} else None)
        sys.setprofile(profile_hook if hook_kind in {"profile", "both"} else None)

    tracker._queue = Queue()
    tracker._owns_queue = True
    monkeypatch.setattr(contained, "_darwin_process_observation", observe)
    monkeypatch.setattr(
        contained.select,
        "kevent",
        lambda *_args, **_kwargs: object(),
        raising=False,
    )
    monkeypatch.setattr(contained.select, "KQ_FILTER_PROC", 1, raising=False)
    monkeypatch.setattr(contained.select, "KQ_EV_ADD", 2, raising=False)
    monkeypatch.setattr(contained.select, "KQ_EV_ENABLE", 4, raising=False)
    monkeypatch.setattr(contained.select, "KQ_EV_CLEAR", 8, raising=False)
    monkeypatch.setattr(contained.select, "KQ_NOTE_FORK", 16, raising=False)
    monkeypatch.setattr(contained.select, "KQ_NOTE_EXIT", 32, raising=False)
    monkeypatch.setattr(contained.threading, "Thread", Thread)
    try:
        _clear_execution_hooks()

        with pytest.raises(
            contained.ContainedProcessError,
            match="contained acquisition does not support active execution hooks",
        ):
            tracker.register_root(identity.pid, deadline=time.monotonic() + 1)

        expected_observations = 2 if activation_boundary == "control" else 3
        assert calls == {
            "observe": expected_observations,
            "control": 1,
            "thread": 0,
            "start": 0,
        }
        assert observations_at_control == 2
        assert tracker._registered == set()
        assert tracker._known == {}
        assert tracker._root_pid is None
        assert tracker._root_started is None
        assert tracker._thread is None
        assert sys.gettrace() is (trace_hook if hook_kind in {"trace", "both"} else None)
        assert sys.getprofile() is (profile_hook if hook_kind in {"profile", "both"} else None)
    finally:
        tracker.close()
        _restore_execution_hooks_for_test(original_trace, original_profile)


def _assert_darwin_tracker_pristine(
    tracker: contained._DarwinKqueueProcessTracker,
) -> None:
    assert tracker._known == {}
    assert tracker._registered == set()
    assert tracker._root_pid is None
    assert tracker._root_started is None
    assert tracker._thread is None
    assert tracker._error is None
    assert tracker._poll_generation == 0
    assert tracker._deadline == 0.0
    assert not tracker._stop.is_set()
    assert tracker._queue is None
    assert not tracker._owns_queue
    assert tracker._construction_error is None
    assert not getattr(tracker, "_queue_tainted", False)
    assert not getattr(tracker, "_cleanup_pending", False)


def test_darwin_register_root_rejects_non_pristine_tracker_without_side_effects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = contained._DarwinKqueueProcessTracker()
    old_identity = contained.ProcessIdentity(77, (7, 0))
    new_identity = contained.ProcessIdentity(101, (1, 0))
    first_error = RuntimeError("first tracker error")
    calls = {"observe": 0, "control": 0, "construct": 0, "join": 0, "close": 0}

    class Queue:
        def control(self, *_args: object) -> list[object]:
            calls["control"] += 1
            return []

        def close(self) -> None:
            calls["close"] += 1

    class OldThread:
        def join(self, *, timeout: float) -> None:
            assert timeout >= 0
            calls["join"] += 1

        def is_alive(self) -> bool:
            return True

    class NewThread:
        def __init__(self, **_kwargs: object) -> None:
            calls["construct"] += 1

        def start(self) -> None:
            return None

        def join(self, *, timeout: float) -> None:
            assert timeout >= 0

        def is_alive(self) -> bool:
            return False

    def observe(_pid: int) -> contained._ProcessObservation:
        calls["observe"] += 1
        return contained._ProcessObservation(identity=new_identity, parent_pid=1)

    queue = Queue()
    old_thread = OldThread()
    tracker._queue = queue
    tracker._owns_queue = True
    tracker._known[old_identity.pid] = old_identity
    tracker._registered.add(old_identity.pid)
    tracker._root_pid = old_identity.pid
    tracker._root_started = old_identity.started
    tracker._thread = old_thread  # type: ignore[assignment]
    tracker._error = first_error
    tracker._poll_generation = 9
    tracker._deadline = 0.25
    monkeypatch.setattr(contained, "_darwin_process_observation", observe)
    monkeypatch.setattr(contained.threading, "Thread", NewThread)

    with pytest.raises(contained.ContainedProcessError, match="not pristine"):
        tracker.register_root(new_identity.pid, deadline=time.monotonic() + 1)

    assert calls == {"observe": 0, "control": 0, "construct": 0, "join": 0, "close": 0}
    assert tracker._queue is queue
    assert tracker._thread is old_thread
    assert tracker._known == {old_identity.pid: old_identity}
    assert tracker._registered == {old_identity.pid}
    assert tracker._root_pid == old_identity.pid
    assert tracker._error is first_error
    assert tracker._poll_generation == 9
    assert not tracker._stop.is_set()


@pytest.mark.skipif(sys.platform != "darwin", reason="Darwin kqueue registration contract")
def test_darwin_register_root_serializes_concurrent_callers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = contained._DarwinKqueueProcessTracker()
    real_thread = threading.Thread
    caller_barrier = threading.Barrier(3)
    first_acquire = threading.Event()
    second_acquire = threading.Event()
    release_acquire = threading.Event()
    result_lock = threading.Lock()
    queues: list[Queue] = []
    results: list[contained.ProcessIdentity] = []
    errors: list[BaseException] = []

    class Queue:
        def __init__(self) -> None:
            self.controls = 0
            self.closes = 0

        def control(self, changes: object, *_args: object) -> list[object]:
            assert changes is not None
            self.controls += 1
            return []

        def close(self) -> None:
            self.closes += 1

    class RegistrationThread:
        def __init__(self, **_kwargs: object) -> None:
            return None

        def start(self) -> None:
            return None

        def join(self, *, timeout: float) -> None:
            assert timeout >= 0

        def is_alive(self) -> bool:
            return False

    def acquire_queue() -> Queue:
        queue = Queue()
        queues.append(queue)
        if len(queues) == 1:
            first_acquire.set()
            assert release_acquire.wait(timeout=1)
        else:
            second_acquire.set()
        return queue

    def observe(pid: int) -> contained._ProcessObservation:
        return contained._ProcessObservation(
            identity=contained.ProcessIdentity(pid, (pid, 0)),
            parent_pid=1,
        )

    def register(pid: int) -> None:
        caller_barrier.wait(timeout=1)
        try:
            result = tracker.register_root(pid, deadline=time.monotonic() + 2)
        except BaseException as exc:
            with result_lock:
                errors.append(exc)
        else:
            with result_lock:
                results.append(result)

    monkeypatch.setattr(contained.select, "kqueue", acquire_queue)
    monkeypatch.setattr(contained.select, "kevent", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(contained, "_darwin_process_observation", observe)
    monkeypatch.setattr(contained.threading, "Thread", RegistrationThread)
    callers = [real_thread(target=register, args=(pid,)) for pid in (101, 102)]
    for caller in callers:
        caller.start()
    caller_barrier.wait(timeout=1)
    assert first_acquire.wait(timeout=1)
    second_acquire.wait(timeout=0.1)
    release_acquire.set()
    for caller in callers:
        caller.join(timeout=2)
        assert not caller.is_alive()

    assert len(results) == 1
    assert len(errors) == 1
    assert isinstance(errors[0], contained.ContainedProcessError)
    assert "not pristine" in str(errors[0])
    assert len(queues) == 1
    assert queues[0].controls == 1
    tracker.close()
    assert queues[0].closes == 1
    _assert_darwin_tracker_pristine(tracker)


@pytest.mark.parametrize("failure_boundary", ("constructor", "start"))
@pytest.mark.skipif(sys.platform != "darwin", reason="Darwin kqueue registration contract")
def test_darwin_register_root_discards_tainted_preinitialized_queue_for_retry(
    monkeypatch: pytest.MonkeyPatch,
    failure_boundary: str,
) -> None:
    tracker = contained._DarwinKqueueProcessTracker()
    identity = contained.ProcessIdentity(101, (1, 0))
    startup_error = RuntimeError(f"{failure_boundary} failed")
    queues: list[Queue] = []
    constructor_calls = 0

    class Queue:
        def __init__(self) -> None:
            self.controls = 0
            self.closes = 0

        def control(self, changes: object, *_args: object) -> list[object]:
            assert changes is not None
            self.controls += 1
            return []

        def close(self) -> None:
            self.closes += 1

    class Thread:
        def __init__(self, **_kwargs: object) -> None:
            nonlocal constructor_calls
            constructor_calls += 1
            self.attempt = constructor_calls
            if failure_boundary == "constructor" and self.attempt == 1:
                raise startup_error

        def start(self) -> None:
            if failure_boundary == "start" and self.attempt == 1:
                raise startup_error

        def join(self, *, timeout: float) -> None:
            assert timeout >= 0

        def is_alive(self) -> bool:
            return False

    def acquire_queue() -> Queue:
        queue = Queue()
        queues.append(queue)
        return queue

    def observe(_pid: int) -> contained._ProcessObservation:
        return contained._ProcessObservation(identity=identity, parent_pid=1)

    monkeypatch.setattr(contained.select, "kqueue", acquire_queue)
    monkeypatch.setattr(contained.select, "kevent", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(contained, "_darwin_process_observation", observe)
    monkeypatch.setattr(contained.threading, "Thread", Thread)
    tracker._initialize_queue()
    assert len(queues) == 1

    with pytest.raises(RuntimeError) as caught:
        tracker.register_root(identity.pid, deadline=time.monotonic() + 1)

    assert caught.value is startup_error
    assert queues[0].controls == 1
    assert queues[0].closes == 1
    _assert_darwin_tracker_pristine(tracker)

    assert tracker.register_root(identity.pid, deadline=time.monotonic() + 1) == identity
    assert len(queues) == 2
    assert queues[1].controls == 1
    tracker.close()
    assert queues[1].closes == 1
    _assert_darwin_tracker_pristine(tracker)


@pytest.mark.skipif(sys.platform != "darwin", reason="Darwin kqueue registration contract")
def test_darwin_register_root_retains_live_failed_start_and_first_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = contained._DarwinKqueueProcessTracker()
    identity = contained.ProcessIdentity(101, (1, 0))
    startup_error = RuntimeError("thread start failed")
    first_error = LookupError("first tracker error")

    class Queue:
        def __init__(self) -> None:
            self.controls = 0
            self.closes = 0

        def control(self, changes: object, *_args: object) -> list[object]:
            assert changes is not None
            self.controls += 1
            return []

        def close(self) -> None:
            self.closes += 1

    class Thread:
        def __init__(self, **_kwargs: object) -> None:
            self.alive = False
            self.allow_stop = False

        def start(self) -> None:
            self.alive = True
            with tracker._condition:
                tracker._error = first_error
                tracker._poll_generation = 12279
                tracker._condition.notify_all()
            raise startup_error

        def join(self, *, timeout: float) -> None:
            assert timeout >= 0
            if self.allow_stop:
                self.alive = False

        def is_alive(self) -> bool:
            return self.alive

    queue = Queue()
    tracker._queue = queue
    tracker._owns_queue = True
    monkeypatch.setattr(contained.select, "kevent", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(
        contained,
        "_darwin_process_observation",
        lambda _pid: contained._ProcessObservation(identity=identity, parent_pid=1),
    )
    monkeypatch.setattr(contained.threading, "Thread", Thread)

    with pytest.raises(RuntimeError) as caught:
        tracker.register_root(identity.pid, deadline=time.monotonic() + 1)

    assert caught.value is startup_error
    cleanup_group = getattr(caught.value, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    thread = tracker._thread
    assert isinstance(thread, Thread)
    assert tracker._error is first_error
    assert tracker._poll_generation == 12279
    assert tracker._queue is queue
    assert tracker._owns_queue
    assert queue.closes == 0
    with pytest.raises(contained.ContainedProcessError, match="not pristine"):
        tracker.register_root(identity.pid, deadline=time.monotonic() + 1)
    assert queue.controls == 1

    thread.allow_stop = True
    tracker.close()
    assert queue.closes == 1
    _assert_darwin_tracker_pristine(tracker)


def test_darwin_close_reports_join_error_after_safe_queue_cleanup() -> None:
    tracker = contained._DarwinKqueueProcessTracker()
    identity = contained.ProcessIdentity(101, (1, 0))
    join_error = RuntimeError("join failed")

    class Queue:
        closes = 0

        def close(self) -> None:
            self.closes += 1

    class Thread:
        def join(self, *, timeout: float) -> None:
            assert timeout >= 0
            raise join_error

        def is_alive(self) -> bool:
            return False

    queue = Queue()
    tracker._queue = queue
    tracker._owns_queue = True
    tracker._thread = Thread()  # type: ignore[assignment]
    tracker._root_pid = identity.pid
    tracker._root_started = identity.started
    tracker._known[identity.pid] = identity
    tracker._registered.add(identity.pid)
    tracker._deadline = time.monotonic() + 1

    with pytest.raises(contained.ContainedProcessError) as caught:
        tracker.close()

    cleanup_group = getattr(caught.value, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert join_error in cleanup_group.exceptions
    assert queue.closes == 1
    _assert_darwin_tracker_pristine(tracker)
    tracker.close()
    _assert_darwin_tracker_pristine(tracker)


@pytest.mark.parametrize("stop_state", ("alive", "unverifiable"))
def test_darwin_close_retains_owner_until_thread_stop_is_verified(
    stop_state: str,
) -> None:
    tracker = contained._DarwinKqueueProcessTracker()
    identity = contained.ProcessIdentity(101, (1, 0))
    first_error = LookupError("first tracker error")
    verification_error = RuntimeError("is_alive failed")

    class Queue:
        closes = 0

        def close(self) -> None:
            self.closes += 1

    class Thread:
        stopped = False

        def join(self, *, timeout: float) -> None:
            assert timeout >= 0

        def is_alive(self) -> bool:
            if self.stopped:
                return False
            if stop_state == "unverifiable":
                raise verification_error
            return True

    queue = Queue()
    thread = Thread()
    tracker._queue = queue
    tracker._owns_queue = True
    tracker._thread = thread  # type: ignore[assignment]
    tracker._root_pid = identity.pid
    tracker._root_started = identity.started
    tracker._known[identity.pid] = identity
    tracker._registered.add(identity.pid)
    tracker._error = first_error
    tracker._poll_generation = 7
    tracker._deadline = time.monotonic() + 1

    with pytest.raises(contained.ContainedProcessError) as caught:
        tracker.close()

    cleanup_group = getattr(caught.value, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    if stop_state == "unverifiable":
        assert verification_error in cleanup_group.exceptions
    assert any("remains open" in str(error) for error in cleanup_group.exceptions)
    assert tracker._thread is thread
    assert tracker._queue is queue
    assert tracker._owns_queue
    assert tracker._error is first_error
    assert tracker._poll_generation == 7
    assert tracker._stop.is_set()
    assert queue.closes == 0

    thread.stopped = True
    tracker.close()
    assert queue.closes == 1
    _assert_darwin_tracker_pristine(tracker)


@pytest.mark.skipif(sys.platform != "darwin", reason="Darwin kqueue registration contract")
def test_darwin_failed_preinitialized_queue_close_blocks_registration_until_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = contained._DarwinKqueueProcessTracker()
    identity = contained.ProcessIdentity(101, (1, 0))
    observe_calls = 0
    queues: list[Queue] = []

    class Queue:
        def __init__(self, *, persistent: bool) -> None:
            self.persistent = persistent
            self.close_calls = 0
            self.control_calls = 0

        def control(self, changes: object, *_args: object) -> list[object]:
            assert changes is not None
            self.control_calls += 1
            return []

        def close(self) -> None:
            self.close_calls += 1
            if self.persistent:
                raise OSError(contained.errno.EIO, "queue close failed")

    class Thread:
        def __init__(self, **_kwargs: object) -> None:
            return None

        def start(self) -> None:
            return None

        def join(self, *, timeout: float) -> None:
            assert timeout >= 0

        def is_alive(self) -> bool:
            return False

    def acquire_queue() -> Queue:
        queue = Queue(persistent=False)
        queues.append(queue)
        return queue

    def observe(_pid: int) -> contained._ProcessObservation:
        nonlocal observe_calls
        observe_calls += 1
        return contained._ProcessObservation(identity=identity, parent_pid=1)

    pending_queue = Queue(persistent=True)
    tracker._queue = pending_queue
    tracker._owns_queue = True
    monkeypatch.setattr(contained.select, "kqueue", acquire_queue)
    monkeypatch.setattr(contained.select, "kevent", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(contained, "_darwin_process_observation", observe)
    monkeypatch.setattr(contained.threading, "Thread", Thread)

    with pytest.raises(contained.ContainedProcessError):
        tracker.close()

    assert pending_queue.close_calls == contained._SIGNAL_STATE_ATTEMPTS
    assert tracker._queue is pending_queue
    assert tracker._owns_queue
    assert tracker._cleanup_pending
    with pytest.raises(contained.ContainedProcessError, match="not pristine"):
        tracker.register_root(identity.pid, deadline=time.monotonic() + 1)
    assert observe_calls == 0
    assert pending_queue.control_calls == 0

    pending_queue.persistent = False
    tracker.close()
    _assert_darwin_tracker_pristine(tracker)
    assert tracker.register_root(identity.pid, deadline=time.monotonic() + 1) == identity
    assert queues[0].control_calls == 1
    tracker.close()
    _assert_darwin_tracker_pristine(tracker)


@pytest.mark.skipif(sys.platform != "darwin", reason="Darwin kqueue registration contract")
def test_darwin_registration_reentrant_close_fails_and_rolls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = contained._DarwinKqueueProcessTracker()
    identity = contained.ProcessIdentity(101, (1, 0))

    class Queue:
        controls = 0
        closes = 0

        def control(self, changes: object, *_args: object) -> list[object]:
            assert changes is not None
            self.controls += 1
            tracker.close()
            raise AssertionError("reentrant close unexpectedly returned")

        def close(self) -> None:
            self.closes += 1

    queue = Queue()
    tracker._queue = queue
    tracker._owns_queue = True
    monkeypatch.setattr(contained.select, "kevent", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(
        contained,
        "_darwin_process_observation",
        lambda _pid: contained._ProcessObservation(identity=identity, parent_pid=1),
    )

    with pytest.raises(contained.ContainedProcessError, match="lifecycle is busy") as caught:
        tracker.register_root(identity.pid, deadline=time.monotonic() + 1)

    assert "cleanup" in str(caught.value)
    assert queue.controls == 1
    assert queue.closes == 1
    _assert_darwin_tracker_pristine(tracker)


@pytest.mark.parametrize("hook_kind", ("trace", "profile", "both"))
@pytest.mark.parametrize("activation_boundary", ("entry", "wait-return"))
def test_darwin_poll_rejects_hooks_at_every_state_handoff(
    monkeypatch: pytest.MonkeyPatch,
    hook_kind: str,
    activation_boundary: str,
) -> None:
    original_trace = sys.gettrace()
    original_profile = sys.getprofile()
    tracker = contained._DarwinKqueueProcessTracker()
    identity = contained.ProcessIdentity(101, (1, 0))
    tracker._known[identity.pid] = identity
    wait_calls = 0

    def trace_hook(_frame: object, _event: str, _arg: object) -> object:
        return trace_hook

    def profile_hook(_frame: object, _event: str, _arg: object) -> None:
        return None

    def activate_hooks() -> None:
        sys.settrace(trace_hook if hook_kind in {"trace", "both"} else None)
        sys.setprofile(profile_hook if hook_kind in {"profile", "both"} else None)

    def wait(*, timeout: float) -> None:
        nonlocal wait_calls
        assert timeout > 0
        wait_calls += 1
        tracker._poll_generation = 1
        if activation_boundary == "wait-return":
            activate_hooks()

    monkeypatch.setattr(tracker._condition, "wait", wait)
    try:
        _clear_execution_hooks()
        if activation_boundary == "entry":
            activate_hooks()
        with pytest.raises(
            contained.ContainedProcessError,
            match="contained acquisition does not support active execution hooks",
        ):
            tracker.poll(deadline=time.monotonic() + 1)

        assert wait_calls == (1 if activation_boundary == "wait-return" else 0)
        assert tracker._known == {identity.pid: identity}
        assert tracker._poll_generation == (1 if activation_boundary == "wait-return" else 0)
        assert tracker._error is None
    finally:
        _restore_execution_hooks_for_test(original_trace, original_profile)


@pytest.mark.parametrize(
    ("boundary", "expected_calls"),
    (
        ("queue", (0, 0, 0, 0, 0)),
        ("root-observation", (1, 0, 0, 0, 0)),
        ("registration-observation", (2, 0, 0, 0, 0)),
        ("kevent", (2, 1, 0, 0, 0)),
        ("control", (2, 1, 1, 0, 0)),
        ("post-observation", (3, 1, 1, 0, 0)),
        ("thread-constructor", (3, 1, 1, 1, 0)),
        ("thread-start", (3, 1, 1, 1, 1)),
    ),
)
@pytest.mark.skipif(sys.platform != "darwin", reason="Darwin kqueue registration contract")
def test_darwin_registration_rechecks_deadline_after_every_handoff(
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
    expected_calls: tuple[int, int, int, int, int],
) -> None:
    tracker = contained._DarwinKqueueProcessTracker()
    identity = contained.ProcessIdentity(101, (1, 0))
    now = [0.0]
    calls = {"observe": 0, "kevent": 0, "control": 0, "construct": 0, "start": 0}

    class Queue:
        closes = 0

        def control(self, changes: object, *_args: object) -> list[object]:
            assert changes is not None
            calls["control"] += 1
            if boundary == "control":
                now[0] = 2.0
            return []

        def close(self) -> None:
            self.closes += 1

    class Thread:
        def __init__(self, **_kwargs: object) -> None:
            calls["construct"] += 1
            if boundary == "thread-constructor":
                now[0] = 2.0

        def start(self) -> None:
            calls["start"] += 1
            if boundary == "thread-start":
                now[0] = 2.0

        def join(self, *, timeout: float) -> None:
            assert timeout >= 0

        def is_alive(self) -> bool:
            return False

    queue = Queue()

    def acquire_queue() -> Queue:
        if boundary == "queue":
            now[0] = 2.0
        return queue

    def observe(_pid: int) -> contained._ProcessObservation:
        calls["observe"] += 1
        observed_boundary = {
            1: "root-observation",
            2: "registration-observation",
            3: "post-observation",
        }[calls["observe"]]
        if boundary == observed_boundary:
            now[0] = 2.0
        return contained._ProcessObservation(identity=identity, parent_pid=1)

    def make_event(*_args: object, **_kwargs: object) -> object:
        calls["kevent"] += 1
        if boundary == "kevent":
            now[0] = 2.0
        return object()

    monkeypatch.setattr(contained.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(contained.select, "kqueue", acquire_queue)
    monkeypatch.setattr(contained.select, "kevent", make_event)
    monkeypatch.setattr(contained, "_darwin_process_observation", observe)
    monkeypatch.setattr(contained.threading, "Thread", Thread)

    with pytest.raises(TimeoutError, match="registration deadline"):
        tracker.register_root(identity.pid, deadline=1.0)

    assert tuple(calls.values()) == expected_calls
    assert queue.closes == 1
    _assert_darwin_tracker_pristine(tracker)


@pytest.mark.parametrize("has_descendant", (False, True), ids=("root-only", "descendant"))
@pytest.mark.parametrize("hook_kind", ("trace", "profile", "both"))
def test_linux_poll_rejects_hooks_before_inventory_and_state_changes(
    monkeypatch: pytest.MonkeyPatch,
    has_descendant: bool,
    hook_kind: str,
) -> None:
    original_trace = sys.gettrace()
    original_profile = sys.getprofile()
    tracker = contained._LinuxSubreaperProcessTracker()
    root = contained.ProcessIdentity(101, (1, 0))
    descendant = contained.ProcessIdentity(102, (2, 0))
    tracker._root_pid = root.pid
    tracker._root_started = root.started
    tracker._known[root.pid] = root
    if has_descendant:
        tracker._known[descendant.pid] = descendant
    tracker._pidfds = {root.pid: 9001}
    tracker._pending_pidfds = [9002]
    expected_state = (
        dict(tracker._known),
        dict(tracker._pidfds),
        list(tracker._pending_pidfds),
        tracker._root_pid,
        tracker._root_started,
        tracker._subreaper_lifecycle,
        tracker._subreaper_record,
    )
    inventory_calls = 0

    def inventory(_deadline: float) -> dict[int, contained._ProcessObservation]:
        nonlocal inventory_calls
        inventory_calls += 1
        raise AssertionError("inventory must not run")

    def trace_hook(_frame: object, _event: str, _arg: object) -> object:
        return trace_hook

    def profile_hook(_frame: object, _event: str, _arg: object) -> None:
        return None

    monkeypatch.setattr(contained, "_linux_process_inventory", inventory)
    try:
        sys.settrace(trace_hook if hook_kind in {"trace", "both"} else None)
        sys.setprofile(profile_hook if hook_kind in {"profile", "both"} else None)

        with pytest.raises(
            contained.ContainedProcessError,
            match="contained acquisition does not support active execution hooks",
        ):
            tracker.poll(deadline=time.monotonic() + 1)

        assert inventory_calls == 0
        assert (
            dict(tracker._known),
            dict(tracker._pidfds),
            list(tracker._pending_pidfds),
            tracker._root_pid,
            tracker._root_started,
            tracker._subreaper_lifecycle,
            tracker._subreaper_record,
        ) == expected_state
        assert sys.gettrace() is (trace_hook if hook_kind in {"trace", "both"} else None)
        assert sys.getprofile() is (profile_hook if hook_kind in {"profile", "both"} else None)
    finally:
        _restore_execution_hooks_for_test(original_trace, original_profile)


def test_linux_poll_without_hooks_still_inventories_and_binds_descendants(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_trace = sys.gettrace()
    original_profile = sys.getprofile()
    tracker = contained._LinuxSubreaperProcessTracker()
    root = contained.ProcessIdentity(101, (1, 0))
    descendant = contained.ProcessIdentity(102, (2, 0))
    tracker._root_pid = root.pid
    tracker._root_started = root.started
    tracker._known[root.pid] = root
    inventory_calls = 0
    bound: list[contained.ProcessIdentity] = []

    def inventory(_deadline: float) -> dict[int, contained._ProcessObservation]:
        nonlocal inventory_calls
        inventory_calls += 1
        return {
            root.pid: contained._ProcessObservation(identity=root, parent_pid=1),
            descendant.pid: contained._ProcessObservation(
                identity=descendant,
                parent_pid=root.pid,
            ),
        }

    def bind(identity: contained.ProcessIdentity) -> None:
        bound.append(identity)
        tracker._known[identity.pid] = identity

    monkeypatch.setattr(contained, "_linux_process_inventory", inventory)
    monkeypatch.setattr(tracker, "_bind_pid", bind)
    try:
        _clear_execution_hooks()
        result = tracker.poll(deadline=time.monotonic() + 1)

        assert inventory_calls == 1
        assert bound == [descendant]
        assert result == {root.pid: root, descendant.pid: descendant}
    finally:
        _restore_execution_hooks_for_test(original_trace, original_profile)


@pytest.mark.parametrize("hook_kind", ("trace", "profile", "both"))
def test_darwin_track_direct_call_rejects_hooks_before_state_changes(
    hook_kind: str,
) -> None:
    original_trace = sys.gettrace()
    original_profile = sys.getprofile()
    tracker = contained._DarwinKqueueProcessTracker()
    identity = contained.ProcessIdentity(101, (1, 0))
    control_calls = 0

    class Queue:
        def control(self, *_args: object) -> list[object]:
            nonlocal control_calls
            control_calls += 1
            raise AssertionError("kernel control must not run")

    def trace_hook(_frame: object, _event: str, _arg: object) -> object:
        return trace_hook

    def profile_hook(_frame: object, _event: str, _arg: object) -> None:
        return None

    tracker._queue = Queue()
    tracker._owns_queue = True
    tracker._root_pid = identity.pid
    tracker._root_started = identity.started
    tracker._known[identity.pid] = identity
    tracker._registered.add(identity.pid)
    tracker._deadline = time.monotonic() + 1
    try:
        sys.settrace(trace_hook if hook_kind in {"trace", "both"} else None)
        sys.setprofile(profile_hook if hook_kind in {"profile", "both"} else None)

        with pytest.raises(
            contained.ContainedProcessError,
            match="contained acquisition does not support active execution hooks",
        ):
            tracker._track()

        assert control_calls == 0
        assert tracker._poll_generation == 0
        assert tracker._error is None
        assert tracker._known == {identity.pid: identity}
        assert tracker._registered == {identity.pid}
        assert not tracker._stop.is_set()
        assert sys.gettrace() is (trace_hook if hook_kind in {"trace", "both"} else None)
        assert sys.getprofile() is (profile_hook if hook_kind in {"profile", "both"} else None)
    finally:
        _restore_execution_hooks_for_test(original_trace, original_profile)


@pytest.mark.parametrize("hook_kind", ("trace", "profile", "both"))
def test_darwin_register_root_reports_startup_thread_hooks(
    monkeypatch: pytest.MonkeyPatch,
    hook_kind: str,
) -> None:
    original_thread_trace = threading.gettrace()
    original_thread_profile = threading.getprofile()
    original_excepthook = threading.excepthook
    tracker = contained._DarwinKqueueProcessTracker()
    identity = contained.ProcessIdentity(101, (1, 0))
    registration_controls = 0
    poll_controls = 0
    close_calls = 0
    uncaught: list[BaseException] = []

    class Queue:
        def control(
            self,
            changes: object,
            _max_events: int,
            _timeout: float,
        ) -> list[object]:
            nonlocal registration_controls, poll_controls
            if changes is None:
                poll_controls += 1
            else:
                registration_controls += 1
            return []

        def close(self) -> None:
            nonlocal close_calls
            close_calls += 1

    def observe(_pid: int) -> contained._ProcessObservation:
        return contained._ProcessObservation(identity=identity, parent_pid=1)

    def trace_hook(_frame: object, _event: str, _arg: object) -> object:
        return trace_hook

    def profile_hook(_frame: object, _event: str, _arg: object) -> None:
        return None

    def capture_uncaught(args: threading.ExceptHookArgs) -> None:
        uncaught.append(args.exc_value)

    tracker._queue = Queue()
    tracker._owns_queue = True
    monkeypatch.setattr(contained, "_darwin_process_observation", observe)
    monkeypatch.setattr(
        contained.select,
        "kevent",
        lambda *_args, **_kwargs: object(),
        raising=False,
    )
    monkeypatch.setattr(contained.select, "KQ_FILTER_PROC", 1, raising=False)
    monkeypatch.setattr(contained.select, "KQ_EV_ADD", 2, raising=False)
    monkeypatch.setattr(contained.select, "KQ_EV_ENABLE", 4, raising=False)
    monkeypatch.setattr(contained.select, "KQ_EV_CLEAR", 8, raising=False)
    monkeypatch.setattr(contained.select, "KQ_NOTE_FORK", 16, raising=False)
    monkeypatch.setattr(contained.select, "KQ_NOTE_EXIT", 32, raising=False)
    try:
        threading.excepthook = capture_uncaught
        threading.settrace(trace_hook if hook_kind in {"trace", "both"} else None)
        threading.setprofile(profile_hook if hook_kind in {"profile", "both"} else None)

        assert tracker.register_root(identity.pid, deadline=time.monotonic() + 1) == identity
        thread = tracker._thread
        assert thread is not None
        thread.join(timeout=1)
        assert not thread.is_alive()

        assert registration_controls == 1
        assert poll_controls == 0
        assert tracker._poll_generation == 0
        startup_error = tracker._error
        assert isinstance(startup_error, contained.ContainedProcessError)
        assert str(startup_error) == (
            "contained acquisition does not support active execution hooks"
        )
        assert uncaught == []

        started = time.monotonic()
        with pytest.raises(
            contained.ContainedProcessError,
            match="kernel process tracking failed",
        ) as caught:
            tracker.poll(deadline=time.monotonic() + 0.5)
        assert time.monotonic() - started < 0.1
        assert caught.value.__cause__ is startup_error
    finally:
        tracker.close()
        threading.settrace(original_thread_trace)
        threading.setprofile(original_thread_profile)
        threading.excepthook = original_excepthook

    assert close_calls == 1


def test_darwin_track_stops_before_next_control_when_hook_activates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = contained._DarwinKqueueProcessTracker()
    activation_error = contained.ContainedProcessError(
        "contained acquisition does not support active execution hooks"
    )
    gate_calls = 0
    control_calls = 0

    class Queue:
        def control(self, *_args: object) -> list[object]:
            nonlocal control_calls
            control_calls += 1
            if control_calls > 1:
                raise AssertionError("hook gate was not rechecked")
            return []

    def gate() -> None:
        nonlocal gate_calls
        gate_calls += 1
        if gate_calls == 4:
            raise activation_error

    tracker._queue = Queue()
    tracker._owns_queue = True
    tracker._deadline = time.monotonic() + 1
    monkeypatch.setattr(contained, "_require_no_execution_hooks", gate)

    tracker._track()

    assert gate_calls == 4
    assert control_calls == 1
    assert tracker._poll_generation == 1
    assert tracker._error is activation_error
    with pytest.raises(
        contained.ContainedProcessError, match="kernel process tracking failed"
    ) as caught:
        tracker.poll(deadline=time.monotonic() + 1)
    assert caught.value.__cause__ is activation_error


@pytest.mark.parametrize("error_timing", ("before", "during"))
def test_darwin_track_preserves_first_error_across_later_control_failure(
    error_timing: str,
) -> None:
    tracker = contained._DarwinKqueueProcessTracker()
    first_error = RuntimeError("first tracker error")
    later_error = OSError("later queue failure")
    control_calls = 0

    class Queue:
        def control(self, *_args: object) -> list[object]:
            nonlocal control_calls
            control_calls += 1
            if error_timing == "during":
                with tracker._condition:
                    tracker._error = first_error
                    tracker._condition.notify_all()
            raise later_error

    tracker._queue = Queue()
    tracker._owns_queue = True
    tracker._deadline = time.monotonic() + 1
    if error_timing == "before":
        tracker._error = first_error

    tracker._track()

    assert control_calls == (0 if error_timing == "before" else 1)
    assert tracker._error is first_error
    with pytest.raises(
        contained.ContainedProcessError,
        match="kernel process tracking failed",
    ) as caught:
        tracker.poll(deadline=time.monotonic() + 1)
    assert caught.value.__cause__ is first_error


@pytest.mark.parametrize("hook_kind", ("none", "trace", "profile", "both"))
@pytest.mark.parametrize("activation_boundary", ("control", "inventory"))
def test_darwin_track_rechecks_hooks_after_kernel_handoffs(
    monkeypatch: pytest.MonkeyPatch,
    hook_kind: str,
    activation_boundary: str,
) -> None:
    original_trace = sys.gettrace()
    original_profile = sys.getprofile()
    tracker = contained._DarwinKqueueProcessTracker()
    root = contained.ProcessIdentity(101, (1, 0))
    control_calls = 0
    inventory_calls = 0
    discover_calls = 0

    class Event:
        fflags = 16

    class Queue:
        def control(self, *_args: object) -> list[object]:
            nonlocal control_calls
            control_calls += 1
            if activation_boundary == "control":
                activate_hooks()
            tracker._stop.set()
            return [Event()]

    def trace_hook(_frame: object, _event: str, _arg: object) -> object:
        return trace_hook

    def profile_hook(_frame: object, _event: str, _arg: object) -> None:
        return None

    def activate_hooks() -> None:
        sys.settrace(trace_hook if hook_kind in {"trace", "both"} else None)
        sys.setprofile(profile_hook if hook_kind in {"profile", "both"} else None)

    def inventory(
        _deadline: float,
        *,
        started_at_or_after: tuple[int, int],
    ) -> dict[int, contained._ProcessObservation]:
        nonlocal inventory_calls
        inventory_calls += 1
        assert started_at_or_after == root.started
        if activation_boundary == "inventory":
            activate_hooks()
        return {
            root.pid: contained._ProcessObservation(identity=root, parent_pid=1),
        }

    def discover(
        _root_pid: int,
        _inventory: dict[int, contained._ProcessObservation],
        _known: dict[int, contained.ProcessIdentity],
    ) -> dict[int, contained.ProcessIdentity]:
        nonlocal discover_calls
        discover_calls += 1
        return {}

    tracker._queue = Queue()
    tracker._owns_queue = True
    tracker._root_pid = root.pid
    tracker._root_started = root.started
    tracker._known[root.pid] = root
    tracker._registered.add(root.pid)
    tracker._deadline = time.monotonic() + 1
    monkeypatch.setattr(contained.select, "KQ_NOTE_FORK", Event.fflags, raising=False)
    monkeypatch.setattr(contained.select, "KQ_NOTE_TRACKERR", 32, raising=False)
    monkeypatch.setattr(contained, "_darwin_process_inventory", inventory)
    monkeypatch.setattr(contained, "_discover_descendants", discover)
    expected_known = dict(tracker._known)
    expected_registered = set(tracker._registered)
    try:
        _clear_execution_hooks()

        tracker._track()

        assert control_calls == 1
        assert tracker._known == expected_known
        assert tracker._registered == expected_registered
        if hook_kind == "none":
            assert inventory_calls == 1
            assert discover_calls == 1
            assert tracker._poll_generation == 1
            assert tracker._error is None
        else:
            expected_inventory_calls = 0 if activation_boundary == "control" else 1
            assert inventory_calls == expected_inventory_calls
            assert discover_calls == 0
            assert tracker._poll_generation == 0
            hook_error = tracker._error
            assert isinstance(hook_error, contained.ContainedProcessError)
            assert str(hook_error) == (
                "contained acquisition does not support active execution hooks"
            )
            with pytest.raises(
                contained.ContainedProcessError,
                match="kernel process tracking failed",
            ) as caught:
                tracker.poll(deadline=time.monotonic() + 1)
            assert caught.value.__cause__ is hook_error
    finally:
        _restore_execution_hooks_for_test(original_trace, original_profile)


@pytest.mark.skipif(sys.platform != "darwin", reason="Darwin kqueue hook contract")
def test_kqueue_acquisition_accepts_no_execution_hooks_and_rejects_both(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_trace = sys.gettrace()
    original_profile = sys.getprofile()
    acquired: list[object] = []

    class Queue:
        def close(self) -> None:
            return None

    def acquire_queue() -> object:
        queue = Queue()
        acquired.append(queue)
        return queue

    def trace_hook(_frame: object, _event: str, _arg: object) -> object:
        return trace_hook

    def profile_hook(_frame: object, _event: str, _arg: object) -> None:
        return None

    monkeypatch.setattr(contained.select, "kqueue", acquire_queue)
    first = contained._DarwinKqueueProcessTracker()
    blocked = contained._DarwinKqueueProcessTracker()
    try:
        _clear_execution_hooks()
        first._initialize_queue()
        assert acquired == [first._queue]

        sys.settrace(trace_hook)
        sys.setprofile(profile_hook)
        with pytest.raises(
            contained.ContainedProcessError,
            match="contained acquisition does not support active execution hooks",
        ):
            blocked._initialize_queue()

        assert acquired == [first._queue]
        assert sys.gettrace() is trace_hook
        assert sys.getprofile() is profile_hook
        assert blocked._queue is None
        assert not blocked._owns_queue
    finally:
        _clear_execution_hooks()
        with contained.suppress(BaseException):
            first.close()
        with contained.suppress(BaseException):
            blocked.close()
        _restore_execution_hooks_for_test(original_trace, original_profile)


def test_descendant_discovery_uses_immutable_birth_parent_identity_after_reparent() -> None:
    root = contained.ProcessIdentity(100, (1, 0), kernel_unique_id=1000)
    reparented_child = contained.ProcessIdentity(101, (2, 0), kernel_unique_id=1001)
    inventory = {
        101: contained._ProcessObservation(
            identity=reparented_child,
            parent_pid=1,
            parent_kernel_unique_id=root.kernel_unique_id,
        )
    }

    descendants = contained._discover_descendants(100, inventory, {100: root})

    assert descendants == {101: reparented_child}


@pytest.mark.parametrize(
    "gone_error",
    (
        FileNotFoundError(errno.ENOENT, "process exited"),
        ProcessLookupError(errno.ESRCH, "process exited"),
    ),
    ids=("enoent", "esrch"),
)
def test_linux_inventory_skips_only_entries_gone_after_enumeration(
    monkeypatch: pytest.MonkeyPatch,
    gone_error: OSError,
) -> None:
    _install_fake_linux_proc(
        monkeypatch,
        _FakeLinuxProcEntry(101, _linux_stat_payload(101, 1, 11)),
        _FakeLinuxProcEntry(102, gone_error),
    )

    inventory = contained._linux_process_inventory(time.monotonic() + 1)

    assert inventory == {101: _observation(101, 1, 11)}


def test_linux_inventory_keeps_permission_denial_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    denied = PermissionError(errno.EACCES, "proc stat denied")
    _install_fake_linux_proc(monkeypatch, _FakeLinuxProcEntry(101, denied))

    with pytest.raises(contained.ContainedProcessError, match="process inventory failed") as caught:
        contained._linux_process_inventory(time.monotonic() + 1)

    assert caught.value.__cause__ is denied


@pytest.mark.parametrize(
    "payload",
    ("malformed stat", _linux_stat_payload(101, 1, "not-a-start-time")),
    ids=("structure", "starttime"),
)
def test_linux_inventory_keeps_malformed_stat_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
    payload: str,
) -> None:
    _install_fake_linux_proc(monkeypatch, _FakeLinuxProcEntry(101, payload))

    with pytest.raises(contained.ContainedProcessError, match="process inventory failed") as caught:
        contained._linux_process_inventory(time.monotonic() + 1)

    assert isinstance(caught.value.__cause__, ValueError)


def test_signal_identity_never_signals_reused_pid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old_identity = contained.ProcessIdentity(101, (11, 0))
    replacement = _observation(101, 1, 99)
    signalled: list[tuple[int, int]] = []
    monkeypatch.setattr(
        contained.os,
        "kill",
        lambda pid, signum: signalled.append((pid, signum)),
    )

    contained._signal_identity(old_identity, signal.SIGKILL, {101: replacement})

    assert signalled == []


@pytest.mark.skipif(
    not sys.platform.startswith("linux"),
    reason="Linux /proc process-churn regression",
)
def test_linux_inventory_survives_short_lived_process_churn() -> None:
    failures: list[BaseException] = []

    def churn() -> None:
        try:
            for _ in range(64):
                subprocess.run(
                    [sys.executable, "-c", "pass"],
                    check=True,
                    timeout=5,
                )
        except BaseException as exc:
            failures.append(exc)

    worker = threading.Thread(target=churn)
    worker.start()
    inventories = 0
    deadline = time.monotonic() + 20
    try:
        while worker.is_alive():
            contained._linux_process_inventory(deadline)
            inventories += 1
    finally:
        worker.join(timeout=20)

    assert not worker.is_alive()
    assert failures == []
    assert inventories > 0


class _FakeKernelTracker:
    def __init__(
        self,
        *,
        identity: contained.ProcessIdentity | None = None,
        poll_error: BaseException | None = None,
    ) -> None:
        self.identity = identity
        self.poll_error = poll_error
        self.closed = False
        self.registered_identity: contained.ProcessIdentity | None = None

    def register_root(self, pid: int, *, deadline: float) -> contained.ProcessIdentity:
        del deadline
        if self.identity is None:
            raise contained.ContainedProcessError("kernel root registration failed")
        self.registered_identity = contained.ProcessIdentity(pid, self.identity.started)
        return self.registered_identity

    def poll(self, *, deadline: float) -> dict[int, contained.ProcessIdentity]:
        del deadline
        if self.poll_error is not None:
            raise self.poll_error
        assert self.registered_identity is not None
        return {self.registered_identity.pid: self.registered_identity}

    def close(self) -> None:
        self.closed = True


class _SequenceKernelTracker:
    def __init__(self, snapshots: tuple[dict[int, contained.ProcessIdentity], ...]) -> None:
        self._snapshots = iter(snapshots)
        self._last: dict[int, contained.ProcessIdentity] = {}

    def register_root(self, pid: int, *, deadline: float) -> contained.ProcessIdentity:
        del deadline
        return contained.ProcessIdentity(pid, (1, 0))

    def poll(self, *, deadline: float) -> dict[int, contained.ProcessIdentity]:
        del deadline
        self._last = next(self._snapshots, self._last)
        return dict(self._last)

    def close(self) -> None:
        return None


class _CloseFailingKernelTracker:
    def __init__(self) -> None:
        self.identity: contained.ProcessIdentity | None = None

    def register_root(self, pid: int, *, deadline: float) -> contained.ProcessIdentity:
        del deadline
        observed = contained._process_observation(pid)
        assert observed is not None
        self.identity = observed.identity
        return observed.identity

    def poll(self, *, deadline: float) -> dict[int, contained.ProcessIdentity]:
        del deadline
        assert self.identity is not None
        return {self.identity.pid: self.identity}

    def close(self) -> None:
        raise contained.ContainedProcessError("close boom")


def _tracker_factory(
    tracker: _FakeKernelTracker,
) -> Callable[[], _FakeKernelTracker]:
    return lambda: tracker


def test_cleanup_error_group_merges_nested_and_sequential_evidence() -> None:
    primary = RuntimeError("primary")
    nested_first = OSError("nested first")
    duplicate = ValueError("duplicate")
    outer = LookupError("outer")
    later = InterruptedError("later")
    primary.cleanup_error_group = BaseExceptionGroup(  # type: ignore[attr-defined]
        "nested cleanup",
        [nested_first, duplicate],
    )
    primary.add_note("nested cleanup note")

    contained._attach_cleanup_error_group(
        primary,
        [duplicate, outer],
        error_label="outer cleanup",
        note="outer cleanup note",
    )
    contained._attach_cleanup_error_group(
        primary,
        [outer, later, nested_first],
        error_label="later cleanup",
        note="later cleanup note",
    )

    cleanup_group = getattr(primary, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert cleanup_group.exceptions == (nested_first, duplicate, outer, later)
    assert tuple(getattr(primary, "__notes__", ())) == (
        "nested cleanup note",
        "outer cleanup note",
        "later cleanup note",
    )


def test_cleanup_error_group_preserves_plain_exception_evidence() -> None:
    primary = RuntimeError("primary")
    prior = OSError("plain prior cleanup")
    later = ValueError("later cleanup")
    primary.cleanup_error_group = prior  # type: ignore[attr-defined]

    contained._attach_cleanup_error_group(
        primary,
        [later, later],
        error_label="merged cleanup",
        note="merged cleanup note",
    )

    cleanup_group = getattr(primary, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert cleanup_group.exceptions == (prior, later)


def test_cleanup_error_group_recursively_deduplicates_nested_identity() -> None:
    primary = RuntimeError("primary")
    duplicate = OSError("duplicate cleanup")
    nested = ValueError("nested cleanup")
    later = LookupError("later cleanup")
    primary.cleanup_error_group = BaseExceptionGroup(  # type: ignore[attr-defined]
        "existing cleanup",
        [
            duplicate,
            BaseExceptionGroup("nested cleanup", [nested, duplicate]),
        ],
    )

    contained._attach_cleanup_error_group(
        primary,
        [duplicate, later],
        error_label="merged cleanup",
        note="merged cleanup note",
    )

    cleanup_group = getattr(primary, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert cleanup_group.exceptions == (duplicate, nested, later)


def test_cleanup_error_group_preserves_hostile_group_and_continues_merge() -> None:
    class HostileCleanupGroup(BaseExceptionGroup):
        @property
        def exceptions(self) -> tuple[BaseException, ...]:
            raise RuntimeError("hostile subgroup inspection")

    primary = RuntimeError("primary")
    opaque = HostileCleanupGroup("opaque cleanup", [OSError("hidden cleanup")])
    later = ValueError("later cleanup")
    primary.cleanup_error_group = opaque  # type: ignore[attr-defined]

    contained._attach_cleanup_error_group(
        primary,
        [later],
        error_label="merged cleanup",
        note="merged cleanup note",
    )

    cleanup_group = getattr(primary, "cleanup_error_group", None)
    assert type(cleanup_group) is BaseExceptionGroup
    assert cleanup_group.exceptions == (opaque, later)


def test_cleanup_error_group_iteratively_flattens_beyond_recursion_limit() -> None:
    primary = RuntimeError("primary")
    leaf = OSError("deep cleanup")
    later = ValueError("later cleanup")
    nested: BaseException = leaf
    for depth in range(sys.getrecursionlimit() + 50):
        nested = BaseExceptionGroup(f"nested cleanup {depth}", [nested])
    primary.cleanup_error_group = nested  # type: ignore[attr-defined]

    contained._attach_cleanup_error_group(
        primary,
        [later],
        error_label="merged cleanup",
        note="merged cleanup note",
    )

    cleanup_group = getattr(primary, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert cleanup_group.exceptions == (leaf, later)


def test_cleanup_error_group_bounds_fresh_subgroup_expansion() -> None:
    source_root = Path(__file__).parents[2] / "src"
    environment = dict(os.environ)
    environment["PYTHONPATH"] = os.pathsep.join(
        filter(None, (str(source_root), environment.get("PYTHONPATH", "")))
    )
    probe = """
import os
import signal
from rquant import contained_subprocess as contained

class ExpandingCleanupGroup(BaseExceptionGroup):
    @property
    def exceptions(self):
        return (ExpandingCleanupGroup('fresh cleanup', [OSError('hidden')]),)

signal.signal(signal.SIGALRM, lambda _signum, _frame: os._exit(91))
signal.setitimer(signal.ITIMER_REAL, 0.5)
primary = RuntimeError('primary')
before = OSError('before cleanup')
expanding = ExpandingCleanupGroup('expanding cleanup', [OSError('hidden')])
after = LookupError('after cleanup')
later = ValueError('later cleanup')
primary.cleanup_error_group = BaseExceptionGroup(
    'outer cleanup',
    [before, expanding, after],
)
contained._attach_cleanup_error_group(
    primary,
    [later],
    error_label='bounded cleanup',
    note='bounded cleanup note',
)
signal.setitimer(signal.ITIMER_REAL, 0)
group = primary.cleanup_error_group
assert type(group) is BaseExceptionGroup
assert group.exceptions == (before, expanding, after, later)
"""

    completed = subprocess.run(
        [sys.executable, "-c", probe],
        capture_output=True,
        text=True,
        timeout=2,
        env=environment,
        check=False,
    )

    assert completed.returncode == 0, (completed.stdout, completed.stderr)


def test_cleanup_budget_resumes_with_legal_sibling_groups(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ExpandingCleanupGroup(BaseExceptionGroup):
        @property
        def exceptions(self) -> tuple[BaseException, ...]:
            return (ExpandingCleanupGroup("fresh cleanup", [OSError("hidden")]),)

    monkeypatch.setattr(contained, "_CLEANUP_GROUP_NODE_BUDGET", 20)
    monkeypatch.setattr(contained, "_CLEANUP_GROUP_FRAME_BUDGET", 20)
    monkeypatch.setattr(contained, "_CLEANUP_GROUP_WORK_BUDGET", 80)
    primary = RuntimeError("primary")
    before = OSError("before cleanup")
    expanding = ExpandingCleanupGroup("expanding cleanup", [OSError("hidden")])
    after = LookupError("after cleanup")
    legal_leaf = ValueError("legal nested cleanup")
    legal_group = BaseExceptionGroup("legal cleanup", [legal_leaf])
    tail = InterruptedError("tail cleanup")
    independent = ArithmeticError("independent cleanup")
    primary.cleanup_error_group = BaseExceptionGroup(  # type: ignore[attr-defined]
        "outer cleanup",
        [before, expanding, after, legal_group, tail],
    )

    contained._attach_cleanup_error_group(
        primary,
        [independent],
        error_label="bounded cleanup",
        note="bounded cleanup note",
    )

    cleanup_group = getattr(primary, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert cleanup_group.exceptions == (
        before,
        expanding,
        after,
        legal_leaf,
        tail,
        independent,
    )


def test_cleanup_budget_rolls_back_explicit_branch_inside_single_wrapper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(contained, "_CLEANUP_GROUP_NODE_BUDGET", 100)
    monkeypatch.setattr(contained, "_CLEANUP_GROUP_FRAME_BUDGET", 100)
    monkeypatch.setattr(contained, "_CLEANUP_GROUP_WORK_BUDGET", 8)
    primary = RuntimeError("primary")
    before = OSError("branch before")
    middle = LookupError("branch middle")
    after = ValueError("branch after")
    branch = BaseExceptionGroup("branch cleanup", [before, middle, after])
    wrapper = BaseExceptionGroup("single wrapper", [branch])
    independent = InterruptedError("independent cleanup")
    primary.cleanup_error_group = wrapper  # type: ignore[attr-defined]

    contained._attach_cleanup_error_group(
        primary,
        [independent],
        error_label="bounded cleanup",
        note="bounded cleanup note",
    )

    cleanup_group = getattr(primary, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert cleanup_group.exceptions == (branch, independent)


def test_cleanup_group_default_budget_flattens_three_recursion_limits() -> None:
    primary = RuntimeError("primary")
    leaf = OSError("deep legal cleanup")
    later = ValueError("later cleanup")
    nested: BaseException = leaf
    for depth in range(3 * sys.getrecursionlimit()):
        nested = BaseExceptionGroup(f"deep legal cleanup {depth}", [nested])
    primary.cleanup_error_group = nested  # type: ignore[attr-defined]

    contained._attach_cleanup_error_group(
        primary,
        [later],
        error_label="deep legal cleanup",
        note="deep legal cleanup note",
    )

    cleanup_group = getattr(primary, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert cleanup_group.exceptions == (leaf, later)


def test_cleanup_default_budget_preserves_each_exhausted_branch_and_later_evidence() -> None:
    class ExpandingCleanupGroup(BaseExceptionGroup):
        @property
        def exceptions(self) -> tuple[BaseException, ...]:
            return (ExpandingCleanupGroup("fresh cleanup", [OSError("hidden")]),)

    primary = RuntimeError("primary")
    expanding = tuple(
        ExpandingCleanupGroup(f"expanding cleanup {index}", [OSError("hidden")])
        for index in range(4)
    )
    legal_leaf = LookupError("legal nested cleanup")
    legal_group = BaseExceptionGroup("legal cleanup", [legal_leaf])
    after = ValueError("after cleanup")
    later = InterruptedError("later cleanup")
    primary.cleanup_error_group = BaseExceptionGroup(  # type: ignore[attr-defined]
        "outer cleanup",
        [*expanding, legal_group, after],
    )

    contained._attach_cleanup_error_group(
        primary,
        [later],
        error_label="default bounded cleanup",
        note="default bounded cleanup note",
    )

    cleanup_group = getattr(primary, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert cleanup_group.exceptions == (*expanding, legal_leaf, after, later)


def test_cleanup_budget_reserves_each_fresh_marker_root_sibling() -> None:
    class FreshMarkerCleanupGroup(BaseExceptionGroup):
        marker = 0

        @property
        def exceptions(self) -> tuple[BaseException, ...]:
            type(self).marker += 1
            return (
                FreshMarkerCleanupGroup(
                    f"fresh marker {type(self).marker}",
                    [OSError("hidden")],
                ),
            )

    primary = RuntimeError("primary")
    expanding = tuple(
        FreshMarkerCleanupGroup(f"root {index}", [OSError("hidden")]) for index in range(4)
    )
    legal_leaf = LookupError("legal nested cleanup")
    legal_group = BaseExceptionGroup("legal cleanup", [legal_leaf])
    after = ValueError("after cleanup")
    later = InterruptedError("later cleanup")
    primary.cleanup_error_group = BaseExceptionGroup(  # type: ignore[attr-defined]
        "outer cleanup",
        [*expanding, legal_group, after],
    )

    contained._attach_cleanup_error_group(
        primary,
        [later],
        error_label="fresh marker cleanup",
        note="fresh marker cleanup note",
    )

    cleanup_group = getattr(primary, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert cleanup_group.exceptions == (*expanding, legal_leaf, after, later)


@pytest.mark.parametrize("source", ("existing", "new"))
def test_cleanup_budget_isolates_fresh_marker_root_siblings_for_all_sources(
    source: str,
) -> None:
    class FreshMarkerCleanupGroup(BaseExceptionGroup):
        marker = 0

        @property
        def exceptions(self) -> tuple[BaseException, ...]:
            type(self).marker += 1
            return (
                FreshMarkerCleanupGroup(
                    f"fresh marker {type(self).marker}",
                    [OSError("hidden")],
                ),
            )

    primary = RuntimeError("primary")
    expanding = tuple(
        FreshMarkerCleanupGroup(f"root {index}", [OSError("hidden")]) for index in range(4)
    )
    legal_leaf = LookupError("legal nested cleanup")
    legal_group = BaseExceptionGroup("legal cleanup", [legal_leaf])
    after = ValueError("after cleanup")
    later = InterruptedError("later cleanup")
    root = BaseExceptionGroup(
        "root cleanup",
        [*expanding, legal_group, after],
    )
    errors: list[BaseException] = [root, later]
    if source == "existing":
        primary.cleanup_error_group = root  # type: ignore[attr-defined]
        errors = [later]

    contained._attach_cleanup_error_group(
        primary,
        errors,
        error_label="root sibling cleanup",
        note="root sibling cleanup note",
    )

    cleanup_group = getattr(primary, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert cleanup_group.exceptions == (*expanding, legal_leaf, after, later)


@pytest.mark.parametrize("source", ("existing", "new"))
def test_cleanup_budget_isolates_nested_fresh_marker_siblings_for_all_sources(
    source: str,
) -> None:
    class FreshMarkerCleanupGroup(BaseExceptionGroup):
        marker = 0

        @property
        def exceptions(self) -> tuple[BaseException, ...]:
            type(self).marker += 1
            return (
                FreshMarkerCleanupGroup(
                    f"fresh marker {type(self).marker}",
                    [OSError("hidden")],
                ),
            )

    primary = RuntimeError("primary")
    expanding = tuple(
        FreshMarkerCleanupGroup(f"nested root {index}", [OSError("hidden")]) for index in range(4)
    )
    legal_leaf = LookupError("legal nested cleanup")
    legal_group = BaseExceptionGroup("legal cleanup", [legal_leaf])
    after = ValueError("after cleanup")
    later = InterruptedError("later cleanup")
    branch = BaseExceptionGroup(
        "branch cleanup",
        [*expanding, legal_group, after],
    )
    wrapper = BaseExceptionGroup("wrapper cleanup", [branch])
    errors: list[BaseException] = [wrapper, later]
    if source == "existing":
        primary.cleanup_error_group = wrapper  # type: ignore[attr-defined]
        errors = [later]

    contained._attach_cleanup_error_group(
        primary,
        errors,
        error_label="nested sibling cleanup",
        note="nested sibling cleanup note",
    )

    cleanup_group = getattr(primary, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert cleanup_group.exceptions == (*expanding, legal_leaf, after, later)


@pytest.mark.parametrize(
    "width_offset",
    (-1, 0, 1),
    ids=("budget-minus-one", "budget", "budget-plus-one"),
)
def test_cleanup_root_width_boundary_is_hard(
    monkeypatch: pytest.MonkeyPatch,
    width_offset: int,
) -> None:
    node_budget = 8
    monkeypatch.setattr(contained, "_CLEANUP_GROUP_NODE_BUDGET", node_budget)
    monkeypatch.setattr(contained, "_CLEANUP_GROUP_FRAME_BUDGET", 20)
    monkeypatch.setattr(contained, "_CLEANUP_GROUP_WORK_BUDGET", 1000)
    primary = RuntimeError("primary")
    leaves = tuple(
        OSError(f"wide root leaf {index}") for index in range(node_budget + width_offset)
    )
    later = ValueError("wide root later cleanup")
    root = BaseExceptionGroup(
        "wide root cleanup",
        list(leaves),
    )
    primary.cleanup_error_group = root  # type: ignore[attr-defined]

    contained._attach_cleanup_error_group(
        primary,
        [later],
        error_label="wide root cleanup",
        note="wide root cleanup note",
    )

    cleanup_group = getattr(primary, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    expected = (*leaves, later) if width_offset == -1 else (root, later)
    assert cleanup_group.exceptions == expected


@pytest.mark.parametrize("width", (100, 10000))
def test_cleanup_extremely_wide_root_is_opaque_and_bounded(width: int) -> None:
    source_root = Path(__file__).parents[2] / "src"
    environment = dict(os.environ)
    environment["PYTHONPATH"] = os.pathsep.join(
        filter(None, (str(source_root), environment.get("PYTHONPATH", "")))
    )
    probe = """
import signal
from rquant import contained_subprocess as contained

contained._CLEANUP_GROUP_NODE_BUDGET = 8
contained._CLEANUP_GROUP_FRAME_BUDGET = 20
contained._CLEANUP_GROUP_WORK_BUDGET = 20
primary = RuntimeError('primary')
root = BaseExceptionGroup(
    'extremely wide cleanup',
    [OSError(f'leaf {index}') for index in range(ROOT_WIDTH)],
)
later = ValueError('later cleanup')
primary.cleanup_error_group = root
signal.setitimer(signal.ITIMER_REAL, 0.5)
contained._attach_cleanup_error_group(
    primary,
    [later],
    error_label='extremely wide cleanup',
    note='extremely wide cleanup note',
)
signal.setitimer(signal.ITIMER_REAL, 0)
group = primary.cleanup_error_group
assert isinstance(group, BaseExceptionGroup)
assert group.exceptions == (root, later)
""".replace("ROOT_WIDTH", str(width))

    completed = subprocess.run(
        [sys.executable, "-c", probe],
        capture_output=True,
        text=True,
        timeout=3,
        env=environment,
        check=False,
    )

    assert completed.returncode == 0, (completed.stdout, completed.stderr)


@pytest.mark.parametrize(
    ("cycle_count", "alarm_seconds"),
    ((8000, 0.75), (16000, 1.5)),
    ids=("8000", "16000"),
)
def test_cleanup_many_cyclic_siblings_is_linear_and_preserves_exact_evidence(
    cycle_count: int,
    alarm_seconds: float,
) -> None:
    source_root = Path(__file__).parents[2] / "src"
    environment = dict(os.environ)
    environment["PYTHONPATH"] = os.pathsep.join(
        filter(None, (str(source_root), environment.get("PYTHONPATH", "")))
    )
    probe = """
import signal
from rquant import contained_subprocess as contained

class CyclicCleanupGroup(BaseExceptionGroup):
    @property
    def exceptions(self):
        return (self,)

cycles = tuple(
    CyclicCleanupGroup(f'cycle {index}', [OSError('hidden')])
    for index in range(CYCLE_COUNT)
)
later = ValueError('later cleanup')
primary = RuntimeError('primary')
primary.cleanup_error_group = BaseExceptionGroup('cycles', list(cycles))
signal.setitimer(signal.ITIMER_REAL, ALARM_SECONDS)
contained._attach_cleanup_error_group(
    primary,
    [later],
    error_label='linear cleanup',
    note='linear cleanup note',
)
signal.setitimer(signal.ITIMER_REAL, 0)
group = primary.cleanup_error_group
assert type(group) is BaseExceptionGroup
assert group.exceptions == (*cycles, later)
""".replace("CYCLE_COUNT", str(cycle_count)).replace("ALARM_SECONDS", str(alarm_seconds))

    completed = subprocess.run(
        [sys.executable, "-c", probe],
        capture_output=True,
        text=True,
        timeout=3,
        env=environment,
        check=False,
    )

    assert completed.returncode == 0, (completed.stdout, completed.stderr)


@pytest.mark.parametrize("budget_kind", ("node", "frame", "work"))
def test_cleanup_error_group_budgets_have_exact_boundaries(
    monkeypatch: pytest.MonkeyPatch,
    budget_kind: str,
) -> None:
    def nested_group(depth: int, leaf: BaseException) -> BaseException:
        nested = leaf
        for index in range(depth):
            nested = BaseExceptionGroup(f"nested cleanup {index}", [nested])
        return nested

    monkeypatch.setattr(contained, "_CLEANUP_GROUP_NODE_BUDGET", 100)
    monkeypatch.setattr(contained, "_CLEANUP_GROUP_FRAME_BUDGET", 100)
    monkeypatch.setattr(contained, "_CLEANUP_GROUP_WORK_BUDGET", 100)
    boundary_primary = RuntimeError("boundary primary")
    boundary_leaves = (OSError("boundary leaf"),)
    over_leaves = (OSError("hidden over-budget leaf"),)
    if budget_kind == "node":
        monkeypatch.setattr(contained, "_CLEANUP_GROUP_NODE_BUDGET", 4)
        boundary_leaves = tuple(OSError(f"boundary leaf {index}") for index in range(3))
        over_leaves = tuple(OSError(f"over-budget leaf {index}") for index in range(4))
        boundary_group = BaseExceptionGroup("boundary cleanup", list(boundary_leaves))
        over_group = BaseExceptionGroup("over-budget cleanup", list(over_leaves))
    elif budget_kind == "frame":
        monkeypatch.setattr(contained, "_CLEANUP_GROUP_FRAME_BUDGET", 3)
        boundary_group = nested_group(3, boundary_leaves[0])
        over_group = nested_group(4, over_leaves[0])
    else:
        monkeypatch.setattr(contained, "_CLEANUP_GROUP_WORK_BUDGET", 4)
        boundary_group = BaseExceptionGroup("boundary cleanup", list(boundary_leaves))
        over_leaves = (over_leaves[0], LookupError("second over-budget leaf"))
        over_group = BaseExceptionGroup("over-budget cleanup", list(over_leaves))
    boundary_later = ValueError("boundary later")

    contained._attach_cleanup_error_group(
        boundary_primary,
        [boundary_group, boundary_later],
        error_label="boundary cleanup",
        note="boundary cleanup note",
    )

    boundary_cleanup = getattr(boundary_primary, "cleanup_error_group", None)
    assert isinstance(boundary_cleanup, BaseExceptionGroup)
    assert boundary_cleanup.exceptions == (*boundary_leaves, boundary_later)

    over_primary = RuntimeError("over-budget primary")
    before = InterruptedError("before over-budget cleanup")
    after = LookupError("after over-budget cleanup")
    over_primary.cleanup_error_group = before  # type: ignore[attr-defined]

    contained._attach_cleanup_error_group(
        over_primary,
        [over_group, after],
        error_label="over-budget cleanup",
        note="over-budget cleanup note",
    )

    over_cleanup = getattr(over_primary, "cleanup_error_group", None)
    assert isinstance(over_cleanup, BaseExceptionGroup)
    assert over_cleanup.exceptions == (before, over_group, after)


def test_cleanup_error_group_preserves_malformed_subgroup_as_opaque() -> None:
    class MalformedCleanupGroup(BaseExceptionGroup):
        @property
        def exceptions(self) -> tuple[object, ...]:
            return (object(),)

    primary = RuntimeError("primary")
    malformed = MalformedCleanupGroup("malformed cleanup", [OSError("hidden cleanup")])
    later = ValueError("later cleanup")
    primary.cleanup_error_group = malformed  # type: ignore[attr-defined]

    contained._attach_cleanup_error_group(
        primary,
        [later],
        error_label="merged cleanup",
        note="merged cleanup note",
    )

    cleanup_group = getattr(primary, "cleanup_error_group", None)
    assert type(cleanup_group) is BaseExceptionGroup
    assert cleanup_group.exceptions == (malformed, later)


def test_cleanup_error_group_preserves_cycle_as_opaque() -> None:
    class CyclicCleanupGroup(BaseExceptionGroup):
        @property
        def exceptions(self) -> tuple[BaseException, ...]:
            return (self,)

    primary = RuntimeError("primary")
    cycle = CyclicCleanupGroup("cyclic cleanup", [OSError("hidden cleanup")])
    later = ValueError("later cleanup")
    primary.cleanup_error_group = cycle  # type: ignore[attr-defined]

    contained._attach_cleanup_error_group(
        primary,
        [later],
        error_label="merged cleanup",
        note="merged cleanup note",
    )

    cleanup_group = getattr(primary, "cleanup_error_group", None)
    assert type(cleanup_group) is BaseExceptionGroup
    assert cleanup_group.exceptions == (cycle, later)


def test_cleanup_error_group_rolls_back_leaf_before_self_cycle() -> None:
    class MixedCyclicCleanupGroup(BaseExceptionGroup):
        leaf: BaseException

        @property
        def exceptions(self) -> tuple[BaseException, ...]:
            return (self.leaf, self)

    primary = RuntimeError("primary")
    leaf = OSError("rolled back leaf")
    cycle = MixedCyclicCleanupGroup("mixed cyclic cleanup", [leaf])
    cycle.leaf = leaf
    later = ValueError("later cleanup")
    primary.cleanup_error_group = cycle  # type: ignore[attr-defined]

    contained._attach_cleanup_error_group(
        primary,
        [later],
        error_label="merged cleanup",
        note="merged cleanup note",
    )

    cleanup_group = getattr(primary, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert cleanup_group.exceptions == (cycle, later)


def test_cleanup_error_group_rolls_back_indirect_cycle() -> None:
    class LinkedCleanupGroup(BaseExceptionGroup):
        linked: tuple[BaseException, ...]

        @property
        def exceptions(self) -> tuple[BaseException, ...]:
            return self.linked

    primary = RuntimeError("primary")
    first_leaf = OSError("rolled back first leaf")
    second_leaf = LookupError("rolled back second leaf")
    first = LinkedCleanupGroup("first cyclic cleanup", [first_leaf])
    second = LinkedCleanupGroup("second cyclic cleanup", [second_leaf])
    first.linked = (first_leaf, second)
    second.linked = (second_leaf, first)
    later = ValueError("later cleanup")
    primary.cleanup_error_group = first  # type: ignore[attr-defined]

    contained._attach_cleanup_error_group(
        primary,
        [later],
        error_label="merged cleanup",
        note="merged cleanup note",
    )

    cleanup_group = getattr(primary, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert cleanup_group.exceptions == (first, later)


def test_cleanup_error_group_flattens_distinct_nested_groups() -> None:
    primary = RuntimeError("primary")
    first = OSError("first cleanup")
    second = LookupError("second cleanup")
    later = ValueError("later cleanup")
    nested = BaseExceptionGroup("nested cleanup", [second])
    outer = BaseExceptionGroup("outer cleanup", [first, nested])
    primary.cleanup_error_group = outer  # type: ignore[attr-defined]

    contained._attach_cleanup_error_group(
        primary,
        [later],
        error_label="merged cleanup",
        note="merged cleanup note",
    )

    cleanup_group = getattr(primary, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert cleanup_group.exceptions == (first, second, later)


def test_hostile_cleanup_formatting_cannot_displace_primary() -> None:
    class HostileCleanupError(Exception):
        def __str__(self) -> str:
            raise RuntimeError("hostile cleanup string")

        def __format__(self, _format_spec: str) -> str:
            raise UnicodeError("hostile cleanup format")

    primary = RuntimeError("primary")
    cleanup = HostileCleanupError()

    with pytest.raises(RuntimeError) as caught:
        try:
            raise primary
        finally:
            contained._finish_signal_restoration(
                {},
                frozenset(),
                contained._ContainedSignalLatch(),
                [cleanup],
                primary_exception=primary,
                error_label="contained subprocess cleanup failures",
            )

    assert caught.value is primary
    cleanup_group = getattr(primary, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert cleanup_group.exceptions == (cleanup,)


def test_hostile_cleanup_type_name_formatting_cannot_displace_primary() -> None:
    class HostileErrorType(type):
        def __getattribute__(cls, name: str) -> object:
            if name == "__name__":
                raise UnicodeError("hostile cleanup type name")
            return super().__getattribute__(name)

    class HostileCleanupError(Exception, metaclass=HostileErrorType):
        def __str__(self) -> str:
            raise RuntimeError("hostile cleanup string")

    primary = RuntimeError("primary")
    cleanup = HostileCleanupError()

    with pytest.raises(RuntimeError) as caught:
        try:
            raise primary
        finally:
            contained._finish_signal_restoration(
                {},
                frozenset(),
                contained._ContainedSignalLatch(),
                [cleanup],
                primary_exception=primary,
                error_label="contained subprocess cleanup failures",
            )

    assert caught.value is primary
    cleanup_group = getattr(primary, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert cleanup_group.exceptions == (cleanup,)


def test_cleanup_attachment_cannot_displace_read_only_primary() -> None:
    prior = OSError("read-only prior cleanup")

    class ReadOnlyCleanupError(RuntimeError):
        @property
        def cleanup_error_group(self) -> BaseException:
            return prior

    primary = ReadOnlyCleanupError("primary")
    later = ValueError("later cleanup")

    with pytest.raises(ReadOnlyCleanupError) as caught:
        try:
            raise primary
        finally:
            contained._attach_cleanup_error_group(
                primary,
                [later],
                error_label="read-only cleanup",
                note="cleanup evidence could not be assigned",
            )

    assert caught.value is primary
    assert "cleanup evidence could not be assigned" in getattr(primary, "__notes__", ())


@pytest.mark.parametrize("malformed_value", (object(), "not an exception"))
def test_cleanup_attachment_ignores_malformed_existing_attribute(
    malformed_value: object,
) -> None:
    primary = RuntimeError("primary")
    later = OSError("later cleanup")
    primary.cleanup_error_group = malformed_value  # type: ignore[attr-defined]

    contained._attach_cleanup_error_group(
        primary,
        [later],
        error_label="replacement cleanup",
        note="replacement cleanup note",
    )

    cleanup_group = getattr(primary, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert cleanup_group.exceptions == (later,)


def test_hostile_cleanup_attributes_and_notes_cannot_displace_primary() -> None:
    class HostileCleanupError(RuntimeError):
        @property
        def cleanup_error_group(self) -> object:
            raise LookupError("hostile cleanup getter")

        @cleanup_error_group.setter
        def cleanup_error_group(self, _value: object) -> None:
            raise OSError("hostile cleanup setter")

        @property
        def __notes__(self) -> object:
            raise RuntimeError("hostile notes getter")

        def add_note(self, _note: str) -> None:
            raise UnicodeError("hostile add_note")

    primary = HostileCleanupError("primary")

    with pytest.raises(HostileCleanupError) as caught:
        try:
            raise primary
        finally:
            contained._attach_cleanup_error_group(
                primary,
                [OSError("cleanup")],
                error_label="hostile cleanup",
                note="hostile cleanup note",
            )

    assert caught.value is primary


@pytest.mark.parametrize("replay_ready", (True, False), ids=("released", "blocked"))
def test_finish_signal_restoration_merges_existing_cleanup_evidence(
    replay_ready: bool,
) -> None:
    primary = RuntimeError("primary")
    original_primary = primary
    nested = OSError("nested cleanup")
    duplicate = ValueError("duplicate cleanup")
    later = LookupError("later cleanup")
    contained._attach_cleanup_error_group(
        primary,
        [nested, duplicate],
        error_label="nested cleanup",
        note="nested cleanup note",
    )

    contained._finish_signal_restoration(
        {},
        frozenset(),
        contained._ContainedSignalLatch(),
        [duplicate, later],
        primary_exception=primary,
        error_label="outer cleanup",
        replay_ready=replay_ready,
    )

    cleanup_group = getattr(primary, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert cleanup_group.exceptions == (nested, duplicate, later)
    assert "nested cleanup note" in getattr(primary, "__notes__", ())
    assert primary is original_primary


def test_cleanup_repeatedly_discovers_fork_during_containment(
    monkeypatch,
) -> None:
    inventories = iter(
        (
            {100: _observation(100, 1, 1), 101: _observation(101, 100, 2)},
            {
                100: _observation(100, 1, 1),
                101: _observation(101, 100, 2),
                102: _observation(102, 101, 3),
            },
            {
                100: _observation(100, 1, 1),
                101: _observation(101, 100, 2),
                102: _observation(102, 101, 3),
            },
            {
                100: _observation(100, 1, 1),
                101: _observation(101, 100, 2),
                102: _observation(102, 101, 3),
            },
            {
                100: _observation(100, 1, 1),
                101: _observation(101, 100, 2),
                102: _observation(102, 101, 3),
            },
            {},
            {},
        )
    )
    signalled: list[tuple[int, int]] = []
    monkeypatch.setattr(contained.os, "killpg", lambda pid, sig: signalled.append((pid, sig)))
    monkeypatch.setattr(contained.os, "kill", lambda pid, sig: signalled.append((pid, sig)))
    monkeypatch.setattr(
        contained,
        "_signal_bound_identity",
        lambda identity, signum: signalled.append((identity.pid, signum)),
    )

    contained._cleanup_process_tree(
        _FinishedProcess(),  # type: ignore[arg-type]
        {},
        root_identity=contained.ProcessIdentity(100, (1, 0)),
        deadline=10,
        inventory_provider=lambda _deadline: next(inventories),
        clock=lambda: 1,
        sleep=lambda _seconds: None,
    )

    assert (101, signal.SIGKILL) in signalled
    assert (102, signal.SIGKILL) in signalled


def test_cleanup_never_signals_reused_pid_identity(monkeypatch) -> None:
    known = {101: contained.ProcessIdentity(101, (2, 0))}
    inventories = iter(
        (
            {100: _observation(100, 1, 1), 101: _observation(101, 100, 9)},
            {100: _observation(100, 1, 1), 101: _observation(101, 100, 9)},
            {100: _observation(100, 1, 1), 101: _observation(101, 100, 9)},
            {100: _observation(100, 1, 1), 101: _observation(101, 100, 9)},
            {},
        )
    )
    signalled: list[tuple[int, int]] = []
    monkeypatch.setattr(contained.os, "killpg", lambda pid, sig: signalled.append((pid, sig)))
    monkeypatch.setattr(contained.os, "kill", lambda pid, sig: signalled.append((pid, sig)))

    contained._cleanup_process_tree(
        _FinishedProcess(),  # type: ignore[arg-type]
        known,
        root_identity=contained.ProcessIdentity(100, (1, 0)),
        deadline=10,
        inventory_provider=lambda _deadline: next(inventories),
        clock=lambda: 1,
        sleep=lambda _seconds: None,
    )

    assert not any(pid == 101 for pid, _signal in signalled)


def test_cleanup_repeatedly_consumes_kernel_fork_tracking(monkeypatch) -> None:
    first = contained.ProcessIdentity(101, (2, 0))
    second = contained.ProcessIdentity(102, (3, 0))
    tracker = _SequenceKernelTracker(
        (
            {100: contained.ProcessIdentity(100, (1, 0)), 101: first},
            {100: contained.ProcessIdentity(100, (1, 0)), 101: first, 102: second},
            {100: contained.ProcessIdentity(100, (1, 0)), 101: first, 102: second},
            {100: contained.ProcessIdentity(100, (1, 0)), 101: first, 102: second},
            {100: contained.ProcessIdentity(100, (1, 0)), 101: first, 102: second},
            {},
        )
    )
    signalled: list[tuple[int, int]] = []
    monkeypatch.setattr(contained.os, "killpg", lambda pid, sig: signalled.append((pid, sig)))
    monkeypatch.setattr(contained.os, "kill", lambda pid, sig: signalled.append((pid, sig)))
    monkeypatch.setattr(
        contained,
        "_signal_bound_identity",
        lambda identity, signum: signalled.append((identity.pid, signum)),
    )

    contained._cleanup_process_tree(
        _FinishedProcess(),  # type: ignore[arg-type]
        {},
        root_identity=contained.ProcessIdentity(100, (1, 0)),
        deadline=10,
        inventory_provider=lambda _deadline: {},
        kernel_tracker=tracker,
        clock=lambda: 1,
        sleep=lambda _seconds: None,
    )

    assert (101, signal.SIGKILL) in signalled
    assert (102, signal.SIGKILL) in signalled


def test_kernel_tracker_rejects_pid_reuse_before_signal(monkeypatch) -> None:
    known = {101: contained.ProcessIdentity(101, (2, 0))}
    tracker = _SequenceKernelTracker(({101: contained.ProcessIdentity(101, (9, 0))},))
    signalled: list[tuple[int, int]] = []
    monkeypatch.setattr(contained.os, "kill", lambda pid, sig: signalled.append((pid, sig)))

    with pytest.raises(contained.ContainedProcessError, match="PID identity reuse"):
        contained._merge_kernel_identities(
            known,
            tracker,
            root_pid=100,
            deadline=10,
        )

    assert signalled == []


def test_linux_subreaper_reaps_only_known_adopted_descendants(monkeypatch) -> None:
    tracker = contained._LinuxSubreaperProcessTracker()
    tracker._root_pid = 100
    tracker._root_started = (1, 0)
    tracker._known[100] = contained.ProcessIdentity(100, (1, 0))
    adopted = _observation(101, contained.os.getpid(), 2)
    monkeypatch.setattr(
        contained,
        "_linux_process_inventory",
        lambda _deadline: {
            100: _observation(100, 1, 1),
            101: adopted,
            102: _observation(102, contained.os.getpid(), 3),
        },
    )
    monkeypatch.setattr(
        tracker, "_bind_pid", lambda identity: tracker._known.setdefault(identity.pid, identity)
    )
    reaped: list[int] = []

    def waitpid(pid: int, options: int) -> tuple[int, int]:
        assert options == contained.os.WNOHANG
        reaped.append(pid)
        return pid, 0

    monkeypatch.setattr(contained.os, "waitpid", waitpid)

    tracker.poll(deadline=10)

    assert reaped == [101, 102]


def test_linux_subreaper_reentrant_trackers_restore_once_after_nested_exceptions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[int, int]] = []

    class _FakeLibc:
        subreaper = 0

        @classmethod
        def prctl(cls, operation: int, value: object, *_args: object) -> int:
            if operation == contained._LinuxSubreaperProcessTracker._PR_GET_CHILD_SUBREAPER:
                ctypes.cast(value, ctypes.POINTER(ctypes.c_int)).contents.value = cls.subreaper
            else:
                assert isinstance(value, int)
                cls.subreaper = value
            calls.append((operation, cls.subreaper))
            return 0

    monkeypatch.setattr(contained.ctypes, "CDLL", lambda *_args, **_kwargs: _FakeLibc())
    monkeypatch.setattr(contained, "_LINUX_SUBREAPER_LOCK", threading.Lock())
    outer = contained._LinuxSubreaperProcessTracker()
    middle = contained._LinuxSubreaperProcessTracker()
    inner = contained._LinuxSubreaperProcessTracker()

    outer._enable_subreaper(time.monotonic() + 1)
    try:
        middle._enable_subreaper(time.monotonic() + 1)
        try:
            inner._enable_subreaper(time.monotonic() + 1)
            try:
                raise RuntimeError("inner failure")
            finally:
                inner.close()
        finally:
            middle.close()
        raise RuntimeError("outer failure")
    except RuntimeError:
        pass
    finally:
        outer.close()

    assert calls == [
        (contained._LinuxSubreaperProcessTracker._PR_GET_CHILD_SUBREAPER, 0),
        (contained._LinuxSubreaperProcessTracker._PR_SET_CHILD_SUBREAPER, 1),
        (contained._LinuxSubreaperProcessTracker._PR_SET_CHILD_SUBREAPER, 0),
    ]


def test_linux_subreaper_remains_serialized_across_threads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[int, int]] = []
    first_enabled = threading.Event()
    release_first = threading.Event()
    second_enabled = threading.Event()
    failures: list[BaseException] = []

    class _FakeLibc:
        subreaper = 0

        @classmethod
        def prctl(cls, operation: int, value: object, *_args: object) -> int:
            if operation == contained._LinuxSubreaperProcessTracker._PR_GET_CHILD_SUBREAPER:
                ctypes.cast(value, ctypes.POINTER(ctypes.c_int)).contents.value = cls.subreaper
            else:
                assert isinstance(value, int)
                cls.subreaper = value
            calls.append((operation, cls.subreaper))
            return 0

    monkeypatch.setattr(contained.ctypes, "CDLL", lambda *_args, **_kwargs: _FakeLibc())
    monkeypatch.setattr(contained, "_LINUX_SUBREAPER_LOCK", threading.Lock())

    def first() -> None:
        tracker = contained._LinuxSubreaperProcessTracker()
        try:
            tracker._enable_subreaper(time.monotonic() + 2)
            first_enabled.set()
            assert release_first.wait(timeout=1)
        except BaseException as exc:
            failures.append(exc)
        finally:
            tracker.close()

    def second() -> None:
        tracker = contained._LinuxSubreaperProcessTracker()
        try:
            assert first_enabled.wait(timeout=1)
            tracker._enable_subreaper(time.monotonic() + 2)
            second_enabled.set()
        except BaseException as exc:
            failures.append(exc)
        finally:
            tracker.close()

    first_thread = threading.Thread(target=first)
    second_thread = threading.Thread(target=second)
    first_thread.start()
    second_thread.start()
    assert first_enabled.wait(timeout=1)
    assert not second_enabled.wait(timeout=0.05)
    release_first.set()
    first_thread.join(timeout=2)
    second_thread.join(timeout=2)

    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    assert failures == []
    assert calls == [
        (contained._LinuxSubreaperProcessTracker._PR_GET_CHILD_SUBREAPER, 0),
        (contained._LinuxSubreaperProcessTracker._PR_SET_CHILD_SUBREAPER, 1),
        (contained._LinuxSubreaperProcessTracker._PR_SET_CHILD_SUBREAPER, 0),
        (contained._LinuxSubreaperProcessTracker._PR_GET_CHILD_SUBREAPER, 0),
        (contained._LinuxSubreaperProcessTracker._PR_SET_CHILD_SUBREAPER, 1),
        (contained._LinuxSubreaperProcessTracker._PR_SET_CHILD_SUBREAPER, 0),
    ]


def test_linux_subreaper_non_owner_thread_cannot_reenter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_enabled = threading.Event()
    release_first = threading.Event()
    second_failure: list[BaseException] = []

    class _FakeLibc:
        @staticmethod
        def prctl(operation: int, value: object, *_args: object) -> int:
            if operation == contained._LinuxSubreaperProcessTracker._PR_GET_CHILD_SUBREAPER:
                ctypes.cast(value, ctypes.POINTER(ctypes.c_int)).contents.value = 0
            return 0

    monkeypatch.setattr(contained.ctypes, "CDLL", lambda *_args, **_kwargs: _FakeLibc())
    monkeypatch.setattr(contained, "_LINUX_SUBREAPER_LOCK", threading.Lock())

    def first() -> None:
        tracker = contained._LinuxSubreaperProcessTracker()
        try:
            tracker._enable_subreaper(time.monotonic() + 1)
            first_enabled.set()
            assert release_first.wait(timeout=1)
        finally:
            tracker.close()

    def second() -> None:
        tracker = contained._LinuxSubreaperProcessTracker()
        try:
            assert first_enabled.wait(timeout=1)
            tracker._enable_subreaper(time.monotonic() + 0.05)
        except BaseException as exc:
            second_failure.append(exc)
        finally:
            tracker.close()

    first_thread = threading.Thread(target=first)
    second_thread = threading.Thread(target=second)
    first_thread.start()
    second_thread.start()
    second_thread.join(timeout=1)
    release_first.set()
    first_thread.join(timeout=1)

    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    assert len(second_failure) == 1
    assert isinstance(second_failure[0], TimeoutError)


class _FakeLinuxSubreaperLibc:
    subreaper = 0
    fail_get = False
    fail_enable = False
    restore_failures = 0
    calls: list[tuple[int, int]] = []

    @classmethod
    def reset(cls) -> None:
        cls.subreaper = 0
        cls.fail_get = False
        cls.fail_enable = False
        cls.restore_failures = 0
        cls.calls = []

    @classmethod
    def prctl(cls, operation: int, value: object, *_args: object) -> int:
        if operation == contained._LinuxSubreaperProcessTracker._PR_GET_CHILD_SUBREAPER:
            cls.calls.append((operation, cls.subreaper))
            if cls.fail_get:
                return -1
            ctypes.cast(value, ctypes.POINTER(ctypes.c_int)).contents.value = cls.subreaper
            return 0
        assert operation == contained._LinuxSubreaperProcessTracker._PR_SET_CHILD_SUBREAPER
        assert isinstance(value, int)
        cls.calls.append((operation, value))
        if value == 1 and cls.fail_enable:
            return -1
        if value == 0 and cls.restore_failures:
            cls.restore_failures -= 1
            return -1
        cls.subreaper = value
        return 0


def _install_fake_linux_subreaper(
    monkeypatch: pytest.MonkeyPatch,
) -> type[_FakeLinuxSubreaperLibc]:
    _FakeLinuxSubreaperLibc.reset()
    monkeypatch.setattr(
        contained.ctypes, "CDLL", lambda *_args, **_kwargs: _FakeLinuxSubreaperLibc()
    )
    monkeypatch.setattr(contained, "_LINUX_SUBREAPER_LOCK", threading.Lock())
    monkeypatch.setattr(
        contained, "_LINUX_SUBREAPER_METADATA_LOCK", threading.Lock(), raising=False
    )
    monkeypatch.setattr(contained, "_LINUX_SUBREAPER_RECORD", None, raising=False)
    monkeypatch.setattr(contained, "_LINUX_SUBREAPER_MEMBERSHIP", threading.local(), raising=False)
    return _FakeLinuxSubreaperLibc


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux subreaper ownership")
def test_linux_subreaper_foreign_outer_close_fails_closed_then_owner_recovers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[int, int]] = []
    close_started = threading.Event()
    close_finished = threading.Event()
    foreign_errors: list[BaseException] = []

    class _FakeLibc:
        subreaper = 0

        @classmethod
        def prctl(cls, operation: int, value: object, *_args: object) -> int:
            if operation == contained._LinuxSubreaperProcessTracker._PR_GET_CHILD_SUBREAPER:
                ctypes.cast(value, ctypes.POINTER(ctypes.c_int)).contents.value = cls.subreaper
            else:
                assert isinstance(value, int)
                cls.subreaper = value
            calls.append((operation, cls.subreaper))
            return 0

    monkeypatch.setattr(contained.ctypes, "CDLL", lambda *_args, **_kwargs: _FakeLibc())
    monkeypatch.setattr(contained, "_LINUX_SUBREAPER_LOCK", threading.Lock())
    tracker = contained._LinuxSubreaperProcessTracker()
    read_fd, write_fd = contained.os.pipe()
    tracker._pidfds[101] = read_fd
    tracker._enable_subreaper(time.monotonic() + 1)
    calls_before_foreign_close = list(calls)

    def close_from_foreign_thread() -> None:
        close_started.set()
        try:
            tracker.close()
        except BaseException as exc:
            foreign_errors.append(exc)
        finally:
            close_finished.set()

    worker = threading.Thread(target=close_from_foreign_thread)
    worker.start()
    assert close_started.wait(timeout=1)
    assert close_finished.wait(timeout=1)
    worker.join(timeout=1)
    try:
        assert not worker.is_alive()
        assert len(foreign_errors) == 1
        assert isinstance(foreign_errors[0], contained.ContainedProcessError)
        assert calls == calls_before_foreign_close
        assert tracker._pidfds == {101: read_fd}
        contained.os.fstat(read_fd)
    finally:
        tracker.close()
        _close_test_fd_if_open(write_fd)

    assert calls == [
        (contained._LinuxSubreaperProcessTracker._PR_GET_CHILD_SUBREAPER, 0),
        (contained._LinuxSubreaperProcessTracker._PR_SET_CHILD_SUBREAPER, 1),
        (contained._LinuxSubreaperProcessTracker._PR_SET_CHILD_SUBREAPER, 0),
    ]


def test_linux_subreaper_owner_recovery_after_foreign_close_releases_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    libc = _install_fake_linux_subreaper(monkeypatch)
    tracker = contained._LinuxSubreaperProcessTracker()
    tracker._enable_subreaper(time.monotonic() + 1)
    foreign_errors: list[BaseException] = []

    def foreign_close() -> None:
        try:
            tracker.close()
        except BaseException as exc:
            foreign_errors.append(exc)

    worker = threading.Thread(target=foreign_close)
    worker.start()
    worker.join(timeout=1)
    assert not worker.is_alive()
    assert len(foreign_errors) == 1
    assert isinstance(foreign_errors[0], contained.ContainedProcessError)

    tracker.close()
    successor = contained._LinuxSubreaperProcessTracker()
    successor._enable_subreaper(time.monotonic() + 1)
    successor.close()

    assert libc.calls == [
        (tracker._PR_GET_CHILD_SUBREAPER, 0),
        (tracker._PR_SET_CHILD_SUBREAPER, 1),
        (tracker._PR_SET_CHILD_SUBREAPER, 0),
        (tracker._PR_GET_CHILD_SUBREAPER, 0),
        (tracker._PR_SET_CHILD_SUBREAPER, 1),
        (tracker._PR_SET_CHILD_SUBREAPER, 0),
    ]


def test_linux_subreaper_foreign_nested_close_preserves_owner_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    libc = _install_fake_linux_subreaper(monkeypatch)
    outer = contained._LinuxSubreaperProcessTracker()
    inner = contained._LinuxSubreaperProcessTracker()
    outer._enable_subreaper(time.monotonic() + 1)
    inner._enable_subreaper(time.monotonic() + 1)
    before = list(libc.calls)
    errors: list[BaseException] = []

    def foreign_close() -> None:
        try:
            inner.close()
        except BaseException as exc:
            errors.append(exc)

    worker = threading.Thread(target=foreign_close)
    worker.start()
    worker.join(timeout=1)
    assert not worker.is_alive()
    assert len(errors) == 1
    assert isinstance(errors[0], contained.ContainedProcessError)
    assert libc.calls == before

    inner.close()
    outer.close()
    assert libc.calls[-1] == (outer._PR_SET_CHILD_SUBREAPER, 0)


def test_linux_subreaper_out_of_order_outer_close_is_non_mutating(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    libc = _install_fake_linux_subreaper(monkeypatch)
    outer = contained._LinuxSubreaperProcessTracker()
    inner = contained._LinuxSubreaperProcessTracker()
    outer._enable_subreaper(time.monotonic() + 1)
    inner._enable_subreaper(time.monotonic() + 1)
    before = list(libc.calls)

    with pytest.raises(contained.ContainedProcessError, match="nested subreaper tracker"):
        outer.close()

    assert libc.calls == before
    inner.close()
    outer.close()
    assert libc.calls[-1] == (outer._PR_SET_CHILD_SUBREAPER, 0)


def test_linux_subreaper_repeated_terminal_close_does_not_restore_twice(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    libc = _install_fake_linux_subreaper(monkeypatch)
    tracker = contained._LinuxSubreaperProcessTracker()
    tracker._enable_subreaper(time.monotonic() + 1)
    tracker.close()
    tracker.close()

    assert libc.calls == [
        (tracker._PR_GET_CHILD_SUBREAPER, 0),
        (tracker._PR_SET_CHILD_SUBREAPER, 1),
        (tracker._PR_SET_CHILD_SUBREAPER, 0),
    ]


def test_linux_subreaper_second_top_level_waits_for_owner_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    libc = _install_fake_linux_subreaper(monkeypatch)
    first_enabled = threading.Event()
    release_first = threading.Event()
    second_enabled = threading.Event()
    failures: list[BaseException] = []

    def first() -> None:
        tracker = contained._LinuxSubreaperProcessTracker()
        try:
            tracker._enable_subreaper(time.monotonic() + 2)
            first_enabled.set()
            assert release_first.wait(timeout=1)
        except BaseException as exc:
            failures.append(exc)
        finally:
            tracker.close()

    def second() -> None:
        tracker = contained._LinuxSubreaperProcessTracker()
        try:
            assert first_enabled.wait(timeout=1)
            tracker._enable_subreaper(time.monotonic() + 2)
            second_enabled.set()
        except BaseException as exc:
            failures.append(exc)
        finally:
            tracker.close()

    first_thread = threading.Thread(target=first)
    second_thread = threading.Thread(target=second)
    first_thread.start()
    second_thread.start()
    assert first_enabled.wait(timeout=1)
    assert not second_enabled.wait(timeout=0.05)
    release_first.set()
    first_thread.join(timeout=2)
    second_thread.join(timeout=2)

    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    assert failures == []
    assert (
        libc.calls.count((contained._LinuxSubreaperProcessTracker._PR_SET_CHILD_SUBREAPER, 0)) == 2
    )


@pytest.mark.parametrize("failure", ("get", "set"), ids=("get", "set"))
def test_linux_subreaper_enable_failure_unwinds_for_later_registration(
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    libc = _install_fake_linux_subreaper(monkeypatch)
    if failure == "get":
        libc.fail_get = True
    else:
        libc.fail_enable = True
    failed = contained._LinuxSubreaperProcessTracker()

    with pytest.raises(contained.ContainedProcessError):
        failed._enable_subreaper(time.monotonic() + 1)

    libc.fail_get = False
    libc.fail_enable = False
    successor = contained._LinuxSubreaperProcessTracker()
    successor._enable_subreaper(time.monotonic() + 1)
    successor.close()
    assert libc.subreaper == 0


def test_linux_subreaper_restore_failure_keeps_owner_for_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    libc = _install_fake_linux_subreaper(monkeypatch)
    tracker = contained._LinuxSubreaperProcessTracker()
    tracker._enable_subreaper(time.monotonic() + 1)
    libc.restore_failures = 1

    with pytest.raises(contained.ContainedProcessError, match="restore child subreaper"):
        tracker.close()

    blocked: list[BaseException] = []

    def second_top_level() -> None:
        contender = contained._LinuxSubreaperProcessTracker()
        try:
            contender._enable_subreaper(time.monotonic() + 0.05)
        except BaseException as exc:
            blocked.append(exc)
        finally:
            contender.close()

    worker = threading.Thread(target=second_top_level)
    worker.start()
    worker.join(timeout=1)
    assert not worker.is_alive()
    assert len(blocked) == 1
    assert isinstance(blocked[0], TimeoutError)

    tracker.close()
    assert libc.subreaper == 0
    assert libc.calls.count((tracker._PR_SET_CHILD_SUBREAPER, 0)) == 2


@pytest.mark.parametrize("persistent", (False, True), ids=("retry", "persistent"))
def test_linux_tracker_verified_pidfd_close_retains_unresolved_inventory(
    monkeypatch: pytest.MonkeyPatch,
    persistent: bool,
) -> None:
    tracker = contained._LinuxSubreaperProcessTracker()
    read_fd, write_fd = contained.os.pipe()
    tracker._pidfds[101] = read_fd
    real_close = contained.os.close
    failures: list[OSError] = []

    def fail_owned_descriptor(descriptor: int) -> None:
        if descriptor == read_fd and (persistent or not failures):
            failure = OSError(contained.errno.EIO, f"pidfd close failure {len(failures) + 1}")
            failures.append(failure)
            raise failure
        real_close(descriptor)

    monkeypatch.setattr(contained.os, "close", fail_owned_descriptor)
    try:
        with pytest.raises(contained.ContainedProcessError) as caught:
            tracker.close()
        cleanup_group = getattr(caught.value, "cleanup_error_group", None)
        assert isinstance(cleanup_group, BaseExceptionGroup)
        assert cleanup_group.exceptions[: len(failures)] == tuple(failures)
        if persistent:
            assert len(failures) == contained._SIGNAL_STATE_ATTEMPTS
            assert tracker._pidfds == {101: read_fd}
            contained.os.fstat(read_fd)
            assert "remain open" in str(cleanup_group.exceptions[-1])
        else:
            assert len(failures) == 1
            assert tracker._pidfds == {}
            with pytest.raises(OSError) as closed:
                contained.os.fstat(read_fd)
            assert closed.value.errno == contained.errno.EBADF
    finally:
        monkeypatch.setattr(contained.os, "close", real_close)
        if tracker._pidfds:
            tracker.close()
        _close_test_fd_if_open(read_fd)
        real_close(write_fd)


def test_linux_pidfd_insertion_failure_closes_unbound_descriptor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = contained._LinuxSubreaperProcessTracker()
    read_fd, write_fd = contained.os.pipe()
    insertion_failure = RuntimeError("pidfd insertion failed")

    class RejectingPidfds(dict[int, int]):
        def __setitem__(self, _pid: int, _descriptor: int) -> None:
            raise insertion_failure

    tracker._pidfds = RejectingPidfds()
    monkeypatch.setattr(contained.os, "pidfd_open", lambda _pid, _flags: read_fd, raising=False)
    try:
        with pytest.raises(RuntimeError) as caught:
            tracker._bind_pid(contained.ProcessIdentity(101, (1, 0)))

        assert caught.value is insertion_failure
        with pytest.raises(OSError) as closed:
            contained.os.fstat(read_fd)
        assert closed.value.errno == contained.errno.EBADF
        assert tracker._pidfds == {}
    finally:
        _close_test_fd_if_open(read_fd)
        contained.os.close(write_fd)


def test_linux_pidfd_partial_insertion_retains_failed_close_for_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = contained._LinuxSubreaperProcessTracker()
    read_fd, write_fd = contained.os.pipe()
    real_close = contained.os.close
    insertion_failure = RuntimeError("pidfd insertion failed after mutation")
    close_failure = OSError(contained.errno.EIO, "pidfd rollback close failed")
    close_attempts = 0
    close_fails = True

    class InsertThenRejectPidfds(dict[int, int]):
        def __setitem__(self, pid: int, descriptor: int) -> None:
            super().__setitem__(pid, descriptor)
            raise insertion_failure

    def fail_owned_close(descriptor: int) -> None:
        nonlocal close_attempts
        if descriptor == read_fd and close_fails:
            close_attempts += 1
            raise close_failure
        real_close(descriptor)

    tracker._pidfds = InsertThenRejectPidfds()
    monkeypatch.setattr(contained.os, "pidfd_open", lambda _pid, _flags: read_fd, raising=False)
    monkeypatch.setattr(contained.os, "close", fail_owned_close)
    try:
        with pytest.raises(RuntimeError) as caught:
            tracker._bind_pid(contained.ProcessIdentity(101, (1, 0)))

        assert caught.value is insertion_failure
        assert close_attempts == contained._SIGNAL_STATE_ATTEMPTS
        assert tracker._pidfds == {101: read_fd}
        assert tracker._pending_pidfds == [read_fd]
        contained.os.fstat(read_fd)
        cleanup_group = getattr(caught.value, "cleanup_error_group", None)
        assert isinstance(cleanup_group, BaseExceptionGroup)
        assert cleanup_group.exceptions == (close_failure,)

        close_fails = False
        tracker.close()
        assert tracker._pidfds == {}
        assert tracker._pending_pidfds == []
        with pytest.raises(OSError) as closed:
            contained.os.fstat(read_fd)
        assert closed.value.errno == contained.errno.EBADF
    finally:
        monkeypatch.setattr(contained.os, "close", real_close)
        _close_test_fd_if_open(read_fd)
        real_close(write_fd)


def test_signal_latch_records_first_signal_without_raising_from_handler() -> None:
    latch = contained._ContainedSignalLatch()

    latch.handle(signal.SIGTERM, None)
    latch.handle(signal.SIGINT, None)

    assert latch.first_signum == signal.SIGTERM


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="atomic signal-mask arbitration requires pthread_sigmask",
)
def test_post_install_handoff_failure_restores_signal_ownership(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    host_handlers, host_mask, starting_mask = _prepare_unblocked_signal_host()
    fault_observed_latch = False
    failure = OSError("post-install ownership handoff failed")

    def fail_after_install(_length: int) -> str:
        nonlocal fault_observed_latch
        fault_observed_latch = all(
            isinstance(
                getattr(signal.getsignal(signum), "__self__", None),
                contained._ContainedSignalLatch,
            )
            for signum in _MANAGED_TEST_SIGNALS
        )
        raise failure

    monkeypatch.setattr(contained.secrets, "token_hex", fail_after_install)
    try:
        with pytest.raises(OSError) as caught:
            contained.run_contained(
                [sys.executable, "-c", "pass"],
                cwd=tmp_path,
                deadline_monotonic=time.monotonic() + 2,
                may_spawn_background_descendants=False,
            )
        observed_handlers = {signum: signal.getsignal(signum) for signum in _MANAGED_TEST_SIGNALS}
        observed_mask = _REAL_PTHREAD_SIGMASK(signal.SIG_BLOCK, set())
    finally:
        _restore_signal_host(host_handlers, host_mask)

    assert caught.value is failure
    assert fault_observed_latch
    assert observed_handlers == host_handlers
    assert observed_mask == starting_mask


@pytest.mark.parametrize(
    "hostile_setup",
    (
        """
class HostileError(BaseException):
    def __str__(self):
        raise RuntimeError("hostile string conversion")
message = "unsafe signal state"
errors = [HostileError()]
""",
        """
message = "unsafe signal state \\ud800"
errors = []
""",
    ),
)
def test_unsafe_signal_state_exit_cannot_be_bypassed_by_diagnostics(
    hostile_setup: str,
) -> None:
    program = f"""
import os
from rquant.contained_subprocess import _terminate_unsafe_signal_state
{hostile_setup}
try:
    _terminate_unsafe_signal_state(message, errors)
except BaseException:
    os._exit(99)
"""

    completed = subprocess.run(
        [sys.executable, "-c", program],
        check=False,
        timeout=5,
    )

    assert completed.returncode == contained._UNSAFE_SIGNAL_STATE_EXIT_CODE


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="atomic signal-mask arbitration requires pthread_sigmask",
)
def test_signal_latch_release_failure_rolls_back_handlers_while_blocked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_signal = contained.signal.signal
    real_sigmask = contained.signal.pthread_sigmask
    watched = (signal.SIGINT, signal.SIGTERM)
    before_handlers = {signum: signal.getsignal(signum) for signum in watched}
    before_mask = real_sigmask(signal.SIG_BLOCK, set())
    active = {
        signum for signum, previous in before_handlers.items() if previous is not signal.SIG_IGN
    }
    primary = OSError("release verification boom")
    awaiting_release_verification = False
    verification_failures = 0
    rollback_masks: list[set[signal.Signals]] = []

    def fail_release_verification(how: int, mask: object) -> set[signal.Signals]:
        nonlocal awaiting_release_verification, verification_failures
        if how == signal.SIG_SETMASK and set(mask) == before_mask:  # type: ignore[arg-type]
            result = real_sigmask(how, mask)  # type: ignore[arg-type]
            if verification_failures < contained._SIGNAL_STATE_ATTEMPTS:
                awaiting_release_verification = True
            return result
        if how == signal.SIG_BLOCK and not mask and awaiting_release_verification:
            awaiting_release_verification = False
            verification_failures += 1
            raise primary
        return real_sigmask(how, mask)  # type: ignore[arg-type]

    def verify_rollback_is_blocked(signum: int, handler: object) -> object:
        installing_latch = isinstance(
            getattr(handler, "__self__", None), contained._ContainedSignalLatch
        )
        current_handler = signal.getsignal(signum)
        removing_tracker = isinstance(
            getattr(current_handler, "__self__", None),
            contained._SignalHandlerInvocationTracker,
        )
        if (
            not installing_latch
            and not removing_tracker
            and verification_failures == contained._SIGNAL_STATE_ATTEMPTS
        ):
            observed_mask = real_sigmask(signal.SIG_BLOCK, set())
            rollback_masks.append(observed_mask)
            assert active <= observed_mask
        return real_signal(signum, handler)  # type: ignore[arg-type]

    monkeypatch.setattr(contained.signal, "pthread_sigmask", fail_release_verification)
    monkeypatch.setattr(contained.signal, "signal", verify_rollback_is_blocked)
    try:
        with pytest.raises(OSError) as caught:
            contained._install_signal_latch(contained._ContainedSignalLatch())
        observed_mask = real_sigmask(signal.SIG_BLOCK, set())
        observed_handlers = {signum: signal.getsignal(signum) for signum in watched}
    finally:
        for signum, previous in before_handlers.items():
            real_signal(signum, previous)
        real_sigmask(signal.SIG_SETMASK, before_mask)

    assert caught.value is primary
    assert verification_failures == contained._SIGNAL_STATE_ATTEMPTS
    assert len(rollback_masks) >= len(active)
    assert observed_mask == before_mask
    assert observed_handlers == before_handlers


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="atomic signal-mask arbitration requires pthread_sigmask",
)
def test_signal_latch_install_failure_retries_rollback_while_blocked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_signal = contained.signal.signal
    real_sigmask = contained.signal.pthread_sigmask
    watched = (signal.SIGINT, signal.SIGTERM)
    before_handlers = {signum: signal.getsignal(signum) for signum in watched}
    before_mask = real_sigmask(signal.SIG_BLOCK, set())
    active = {
        signum for signum, previous in before_handlers.items() if previous is not signal.SIG_IGN
    }
    primary = OSError("second handler install boom")
    rollback_failure = OSError("first rollback boom")
    installs = 0
    rollback_attempts = 0

    def fail_install_then_rollback(signum: int, handler: object) -> object:
        nonlocal installs, rollback_attempts
        installing_latch = isinstance(
            getattr(handler, "__self__", None), contained._ContainedSignalLatch
        )
        if installing_latch:
            installs += 1
            if installs == 2:
                raise primary
        elif installs == 2 and signum == signal.SIGINT and handler is before_handlers[signum]:
            rollback_attempts += 1
            assert active <= real_sigmask(signal.SIG_BLOCK, set())
            if rollback_attempts == 1:
                raise rollback_failure
        return real_signal(signum, handler)  # type: ignore[arg-type]

    monkeypatch.setattr(contained.signal, "signal", fail_install_then_rollback)
    try:
        with pytest.raises(OSError) as caught:
            contained._install_signal_latch(contained._ContainedSignalLatch())
        observed_mask = real_sigmask(signal.SIG_BLOCK, set())
        observed_handlers = {signum: signal.getsignal(signum) for signum in watched}
    finally:
        for signum, previous in before_handlers.items():
            real_signal(signum, previous)
        real_sigmask(signal.SIG_SETMASK, before_mask)

    assert caught.value is primary
    assert rollback_attempts == 2
    assert observed_mask == before_mask
    assert observed_handlers == before_handlers
    cleanup_group = getattr(caught.value, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert rollback_failure in cleanup_group.exceptions


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="atomic signal-mask arbitration requires pthread_sigmask",
)
def test_persistent_signal_latch_install_rollback_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_signal = contained.signal.signal
    real_sigmask = contained.signal.pthread_sigmask
    watched = (signal.SIGINT, signal.SIGTERM)
    before_handlers = {signum: signal.getsignal(signum) for signum in watched}
    before_mask = real_sigmask(signal.SIG_BLOCK, set())
    active = {
        signum for signum, previous in before_handlers.items() if previous is not signal.SIG_IGN
    }
    primary = OSError("second handler install boom")
    rollback_failures: list[OSError] = []
    installs = 0

    def fail_install_and_all_rollbacks(signum: int, handler: object) -> object:
        nonlocal installs
        installing_latch = isinstance(
            getattr(handler, "__self__", None), contained._ContainedSignalLatch
        )
        if installing_latch:
            installs += 1
            if installs == 2:
                raise primary
        elif installs == 2 and signum == signal.SIGINT and handler is before_handlers[signum]:
            failure = OSError(f"persistent rollback boom {len(rollback_failures) + 1}")
            rollback_failures.append(failure)
            raise failure
        return real_signal(signum, handler)  # type: ignore[arg-type]

    monkeypatch.setattr(contained.signal, "signal", fail_install_and_all_rollbacks)
    try:
        with pytest.raises(OSError) as caught:
            contained._install_signal_latch(contained._ContainedSignalLatch())
        observed_mask = real_sigmask(signal.SIG_BLOCK, set())
        observed_sigint_handler = signal.getsignal(signal.SIGINT)
    finally:
        for signum, previous in before_handlers.items():
            real_signal(signum, previous)
        real_sigmask(signal.SIG_SETMASK, before_mask)

    assert caught.value is primary
    assert len(rollback_failures) == contained._SIGNAL_STATE_ATTEMPTS
    assert active <= observed_mask
    assert isinstance(
        getattr(observed_sigint_handler, "__self__", None),
        contained._ContainedSignalLatch,
    )
    cleanup_group = getattr(caught.value, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert cleanup_group.exceptions == tuple(rollback_failures)


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="atomic signal-mask arbitration requires pthread_sigmask",
)
@pytest.mark.parametrize(
    ("failure", "fail_on_install"),
    (
        (OSError("first handler install boom"), 1),
        (ValueError("non-main-thread handler install boom"), 1),
        (ValueError("second handler install boom"), 2),
    ),
)
def test_signal_arbiter_install_fails_before_tracker_pipe_or_popen(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: BaseException,
    fail_on_install: int,
) -> None:
    real_signal = contained.signal.signal
    watched = (signal.SIGINT, signal.SIGTERM)
    before_handlers = {signum: signal.getsignal(signum) for signum in watched}
    before_mask = signal.pthread_sigmask(signal.SIG_BLOCK, set())
    installs = 0
    resource_calls: list[str] = []

    def failing_install(signum: int, handler: object) -> object:
        nonlocal installs
        installing_latch = isinstance(
            getattr(handler, "__self__", None), contained._ContainedSignalLatch
        )
        if installing_latch:
            installs += 1
            if installs == fail_on_install:
                raise failure
        return real_signal(signum, handler)  # type: ignore[arg-type]

    def forbidden_tracker_factory() -> _FakeKernelTracker:
        resource_calls.append("tracker")
        raise AssertionError("tracker created before signal authority")

    def forbidden_pipe() -> tuple[int, int]:
        resource_calls.append("pipe")
        raise AssertionError("pipe created before signal authority")

    def forbidden_popen(*_args: object, **_kwargs: object) -> subprocess.Popen[str]:
        resource_calls.append("popen")
        raise AssertionError("Popen called before signal authority")

    monkeypatch.setattr(contained.signal, "signal", failing_install)
    monkeypatch.setattr(contained.os, "pipe", forbidden_pipe)
    monkeypatch.setattr(contained.subprocess, "Popen", forbidden_popen)
    try:
        with pytest.raises(type(failure)) as caught:
            contained.run_contained(
                [sys.executable, "-c", "pass"],
                cwd=tmp_path,
                deadline_monotonic=time.monotonic() + 2,
                kernel_tracker_factory=forbidden_tracker_factory,
                may_spawn_background_descendants=False,
            )
        observed_mask = signal.pthread_sigmask(signal.SIG_BLOCK, set())
        observed_handlers = {signum: signal.getsignal(signum) for signum in watched}
    finally:
        for signum, previous in before_handlers.items():
            real_signal(signum, previous)
        signal.pthread_sigmask(signal.SIG_SETMASK, before_mask)

    assert caught.value is failure
    assert resource_calls == []
    assert observed_mask == before_mask
    assert observed_handlers == before_handlers


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="atomic signal-mask arbitration requires pthread_sigmask",
)
def test_pre_spawn_preparation_failure_restores_signal_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    watched = (signal.SIGINT, signal.SIGTERM)
    before_handlers = {signum: signal.getsignal(signum) for signum in watched}
    before_mask = signal.pthread_sigmask(signal.SIG_BLOCK, set())
    primary = OSError("token generation boom")
    resource_calls: list[str] = []

    def fail_token_generation(_size: int) -> str:
        raise primary

    def forbidden_tracker_factory() -> _FakeKernelTracker:
        resource_calls.append("tracker")
        raise AssertionError("tracker created after preparation failure")

    monkeypatch.setattr(contained.secrets, "token_hex", fail_token_generation)
    try:
        with pytest.raises(OSError) as caught:
            contained.run_contained(
                [sys.executable, "-c", "pass"],
                cwd=tmp_path,
                deadline_monotonic=time.monotonic() + 2,
                kernel_tracker_factory=forbidden_tracker_factory,
                may_spawn_background_descendants=False,
            )
        observed_mask = signal.pthread_sigmask(signal.SIG_BLOCK, set())
        observed_handlers = {signum: signal.getsignal(signum) for signum in watched}
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, before_mask)
        for signum, previous in before_handlers.items():
            signal.signal(signum, previous)

    assert caught.value is primary
    assert resource_calls == []
    assert observed_mask == before_mask
    assert observed_handlers == before_handlers


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="atomic signal-mask arbitration requires pthread_sigmask",
)
@pytest.mark.parametrize("boundary", ("after_final_sigpending", "sig_setmask"))
def test_unlatched_restore_boundary_signal_propagates_original_handler(
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    real_signal = contained.signal.signal
    real_sigpending = contained.signal.sigpending
    real_sigmask = contained.signal.pthread_sigmask
    watched = (signal.SIGINT, signal.SIGTERM)
    before_handlers = {signum: signal.getsignal(signum) for signum in watched}
    before_mask = signal.pthread_sigmask(signal.SIG_BLOCK, set())
    latch = contained._ContainedSignalLatch()
    injected = False
    queued_for_unmask: list[int] = []
    primary = InterruptedError(f"{boundary} original handler")

    def previous_handler(_signum: int, _frame: object) -> None:
        raise primary

    for signum in watched:
        real_signal(signum, previous_handler)
    previous_handlers, active_signals = contained._install_signal_latch(latch)

    def sigpending_with_boundary_delivery() -> set[signal.Signals]:
        nonlocal injected
        pending = real_sigpending()
        if boundary == "after_final_sigpending" and not injected and signal.SIGTERM not in pending:
            injected = True
            queued_for_unmask.append(signal.SIGTERM)
        return pending

    def sigmask_with_boundary_delivery(how: int, mask: object) -> set[signal.Signals]:
        nonlocal injected
        if how == signal.SIG_SETMASK and (
            queued_for_unmask or (boundary == "sig_setmask" and not injected)
        ):
            if boundary == "sig_setmask":
                injected = True
            queued_for_unmask.clear()
            handler = signal.getsignal(signal.SIGTERM)
            assert callable(handler)
            handler(signal.SIGTERM, None)
        return real_sigmask(how, mask)  # type: ignore[arg-type]

    monkeypatch.setattr(contained.signal, "sigpending", sigpending_with_boundary_delivery)
    monkeypatch.setattr(contained.signal, "pthread_sigmask", sigmask_with_boundary_delivery)
    try:
        restoration = contained._restore_signal_handlers_atomically(
            previous_handlers,
            active_signals,
            latch,
        )
        with pytest.raises(InterruptedError) as caught:
            restoration.release_and_replay(
                latch,
                previous_handlers,
                [],
                error_label="contained subprocess cleanup failures",
            )
    finally:
        monkeypatch.setattr(contained.signal, "pthread_sigmask", real_sigmask)
        signal.pthread_sigmask(signal.SIG_SETMASK, before_mask)
        for signum, previous in before_handlers.items():
            real_signal(signum, previous)

    assert caught.value is primary
    assert injected
    assert latch.first_signum is None


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="atomic signal-mask arbitration requires pthread_sigmask",
)
def test_restore_boundary_signal_after_transient_handler_failure_is_replayed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_signal = contained.signal.signal
    real_sigmask = contained.signal.pthread_sigmask
    watched = (signal.SIGINT, signal.SIGTERM)
    before_handlers = {signum: signal.getsignal(signum) for signum in watched}
    before_mask = signal.pthread_sigmask(signal.SIG_BLOCK, set())
    latch = contained._ContainedSignalLatch()
    restore_failure = OSError("handler restore boom")
    replayed = InterruptedError("boundary signal replayed by original handler")
    injected = False
    restore_attempts = 0

    def previous_handler(_signum: int, _frame: object) -> None:
        raise replayed

    for signum in watched:
        real_signal(signum, previous_handler)
    previous_handlers, active_signals = contained._install_signal_latch(latch)

    def fail_sigterm_restore(signum: int, handler: object) -> object:
        nonlocal restore_attempts
        removing_tracker = isinstance(
            getattr(signal.getsignal(signum), "__self__", None),
            contained._SignalHandlerInvocationTracker,
        )
        if signum == signal.SIGTERM and handler is previous_handler and not removing_tracker:
            restore_attempts += 1
            if restore_attempts == 1:
                raise restore_failure
        return real_signal(signum, handler)  # type: ignore[arg-type]

    def sigmask_with_boundary_delivery(how: int, mask: object) -> set[signal.Signals]:
        nonlocal injected
        if how == signal.SIG_SETMASK and not injected:
            injected = True
            handler = signal.getsignal(signal.SIGTERM)
            assert callable(handler)
            handler(signal.SIGTERM, None)
        return real_sigmask(how, mask)  # type: ignore[arg-type]

    monkeypatch.setattr(contained.signal, "signal", fail_sigterm_restore)
    monkeypatch.setattr(contained.signal, "pthread_sigmask", sigmask_with_boundary_delivery)
    try:
        with pytest.raises(InterruptedError) as caught:
            contained._finish_signal_restoration(
                previous_handlers,
                active_signals,
                latch,
                [],
                primary_exception=None,
                error_label="contained subprocess cleanup failures",
            )
    finally:
        monkeypatch.setattr(contained.signal, "pthread_sigmask", real_sigmask)
        signal.pthread_sigmask(signal.SIG_SETMASK, before_mask)
        for signum, previous in before_handlers.items():
            real_signal(signum, previous)

    assert caught.value is replayed
    assert injected
    assert restore_attempts == 2
    cleanup_group = getattr(caught.value, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert restore_failure in cleanup_group.exceptions


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="atomic signal-mask arbitration requires pthread_sigmask",
)
@pytest.mark.parametrize("has_primary", (True, False), ids=("with-primary", "without-primary"))
def test_latched_signal_survives_persistent_tracker_install_failure(
    monkeypatch: pytest.MonkeyPatch,
    has_primary: bool,
) -> None:
    real_signal = contained.signal.signal
    real_sigmask = contained.signal.pthread_sigmask
    watched = (signal.SIGINT, signal.SIGTERM)
    before_handlers = {signum: signal.getsignal(signum) for signum in watched}
    before_mask = real_sigmask(signal.SIG_BLOCK, set())
    first = KeyboardInterrupt("latched signal replay")
    primary = RuntimeError("existing primary") if has_primary else None
    install_failures: list[OSError] = []
    restore_failure = OSError("transient original handler restoration failure")
    restore_attempts = 0

    def previous_handler(signum: int, _frame: object) -> None:
        if signum == signal.SIGTERM:
            raise first

    for signum in watched:
        real_signal(signum, previous_handler)
    latch = contained._ContainedSignalLatch()
    previous_handlers, active_signals = contained._install_signal_latch(latch)
    latch.handle(signal.SIGTERM, None)

    def reject_tracker_then_retry_original(signum: int, handler: object) -> object:
        nonlocal restore_attempts
        tracker = getattr(handler, "__self__", None)
        if signum == signal.SIGINT and isinstance(
            tracker,
            contained._SignalHandlerInvocationTracker,
        ):
            failure = OSError(f"tracker install failure {len(install_failures) + 1}")
            install_failures.append(failure)
            raise failure
        current_tracker = getattr(signal.getsignal(signum), "__self__", None)
        if (
            signum == signal.SIGTERM
            and handler is previous_handler
            and isinstance(current_tracker, contained._SignalHandlerInvocationTracker)
        ):
            restore_attempts += 1
            if restore_attempts == 1:
                raise restore_failure
        return real_signal(signum, handler)  # type: ignore[arg-type]

    monkeypatch.setattr(
        contained.signal,
        "signal",
        reject_tracker_then_retry_original,
    )
    try:
        with pytest.raises(BaseException) as caught:
            if primary is None:
                contained._finish_signal_restoration(
                    previous_handlers,
                    active_signals,
                    latch,
                    [],
                    primary_exception=None,
                    error_label="contained subprocess cleanup failures",
                )
            else:
                try:
                    raise primary
                finally:
                    contained._finish_signal_restoration(
                        previous_handlers,
                        active_signals,
                        latch,
                        [],
                        primary_exception=sys.exception(),
                        error_label="contained subprocess cleanup failures",
                    )
        observed_mask = real_sigmask(signal.SIG_BLOCK, set())
        observed_handlers = {signum: signal.getsignal(signum) for signum in watched}
    finally:
        monkeypatch.setattr(contained.signal, "signal", real_signal)
        real_sigmask(signal.SIG_BLOCK, set(watched))
        for signum, handler in before_handlers.items():
            real_signal(signum, handler)
        real_sigmask(signal.SIG_SETMASK, before_mask)

    assert caught.value is first
    assert len(install_failures) == contained._SIGNAL_STATE_ATTEMPTS
    assert restore_attempts == 2
    assert set(active_signals) <= observed_mask
    assert observed_handlers == previous_handlers
    cleanup_group = getattr(caught.value, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert cleanup_group.exceptions == (*install_failures, restore_failure)


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="atomic signal-mask arbitration requires pthread_sigmask",
)
def test_signal_restoration_preserves_mask_when_initial_block_raises_after_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_signal = contained.signal.signal
    real_sigmask = contained.signal.pthread_sigmask
    watched = (signal.SIGINT, signal.SIGTERM)
    before_handlers = {signum: signal.getsignal(signum) for signum in watched}
    before_mask = real_sigmask(signal.SIG_BLOCK, set())
    latch = contained._ContainedSignalLatch()
    first = KeyboardInterrupt()
    block_failure = OSError("first restoration block boom")
    block_attempts = 0

    def previous_handler(_signum: int, _frame: object) -> None:
        raise first

    for signum in watched:
        real_signal(signum, previous_handler)
    previous_handlers, active_signals = contained._install_signal_latch(latch)
    latch.handle(signal.SIGTERM, None)

    def fail_first_block_after_mutation(how: int, mask: object) -> set[signal.Signals]:
        nonlocal block_attempts
        if how == signal.SIG_BLOCK and mask:
            block_attempts += 1
            if block_attempts == 1:
                real_sigmask(how, mask)  # type: ignore[arg-type]
                raise block_failure
        return real_sigmask(how, mask)  # type: ignore[arg-type]

    monkeypatch.setattr(
        contained.signal,
        "pthread_sigmask",
        fail_first_block_after_mutation,
    )
    try:
        with pytest.raises(KeyboardInterrupt) as caught:
            contained._finish_signal_restoration(
                previous_handlers,
                active_signals,
                latch,
                [],
                primary_exception=None,
                error_label="contained subprocess cleanup failures",
            )
        observed_mask = real_sigmask(signal.SIG_BLOCK, set())
        observed_handlers = {signum: signal.getsignal(signum) for signum in watched}
    finally:
        for signum, previous in before_handlers.items():
            real_signal(signum, previous)
        real_sigmask(signal.SIG_SETMASK, before_mask)

    assert caught.value is first
    assert block_attempts == 1
    assert observed_mask == before_mask
    assert observed_handlers == previous_handlers
    cleanup_group = getattr(caught.value, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert block_failure in cleanup_group.exceptions


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="atomic signal-mask arbitration requires pthread_sigmask",
)
def test_signal_restoration_falls_back_to_exact_mask_after_persistent_block_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_signal = contained.signal.signal
    real_sigmask = contained.signal.pthread_sigmask
    watched = (signal.SIGINT, signal.SIGTERM)
    before_handlers = {signum: signal.getsignal(signum) for signum in watched}
    before_mask = real_sigmask(signal.SIG_BLOCK, set())
    latch = contained._ContainedSignalLatch()
    first = KeyboardInterrupt()
    block_failures: list[OSError] = []
    restore_calls = 0

    def previous_handler(_signum: int, _frame: object) -> None:
        raise first

    for signum in watched:
        real_signal(signum, previous_handler)
    previous_handlers, active_signals = contained._install_signal_latch(latch)
    latch.handle(signal.SIGTERM, None)

    def fail_nonempty_block(how: int, mask: object) -> set[signal.Signals]:
        if how == signal.SIG_BLOCK and mask:
            failure = OSError(f"persistent restoration block boom {len(block_failures) + 1}")
            block_failures.append(failure)
            raise failure
        return real_sigmask(how, mask)  # type: ignore[arg-type]

    def verify_restore_is_blocked(signum: int, handler: object) -> object:
        nonlocal restore_calls
        if handler is previous_handlers[signum]:
            restore_calls += 1
            assert set(active_signals) <= real_sigmask(signal.SIG_BLOCK, set())
        return real_signal(signum, handler)  # type: ignore[arg-type]

    monkeypatch.setattr(contained.signal, "pthread_sigmask", fail_nonempty_block)
    monkeypatch.setattr(contained.signal, "signal", verify_restore_is_blocked)
    try:
        with pytest.raises(KeyboardInterrupt) as caught:
            contained._finish_signal_restoration(
                previous_handlers,
                active_signals,
                latch,
                [],
                primary_exception=None,
                error_label="contained subprocess cleanup failures",
            )
        observed_mask = real_sigmask(signal.SIG_BLOCK, set())
        observed_handlers = {signum: signal.getsignal(signum) for signum in watched}
    finally:
        for signum, previous in before_handlers.items():
            real_signal(signum, previous)
        real_sigmask(signal.SIG_SETMASK, before_mask)

    assert caught.value is first
    assert len(block_failures) == contained._SIGNAL_STATE_ATTEMPTS
    assert restore_calls >= len(previous_handlers)
    assert observed_mask == before_mask
    assert observed_handlers == previous_handlers
    cleanup_group = getattr(caught.value, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert cleanup_group.exceptions[: len(block_failures)] == tuple(block_failures)


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="atomic signal-mask arbitration requires pthread_sigmask",
)
def test_signal_restoration_retries_partial_handler_failure_while_blocked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_signal = contained.signal.signal
    real_sigmask = contained.signal.pthread_sigmask
    watched = (signal.SIGINT, signal.SIGTERM)
    before_handlers = {signum: signal.getsignal(signum) for signum in watched}
    before_mask = real_sigmask(signal.SIG_BLOCK, set())
    latch = contained._ContainedSignalLatch()
    first = KeyboardInterrupt()
    restore_failure = OSError("first partial restore boom")
    restore_attempts = 0

    def previous_handler(_signum: int, _frame: object) -> None:
        raise first

    for signum in watched:
        real_signal(signum, previous_handler)
    previous_handlers, active_signals = contained._install_signal_latch(latch)
    latch.handle(signal.SIGTERM, None)

    def fail_first_sigint_restore(signum: int, handler: object) -> object:
        nonlocal restore_attempts
        removing_tracker = isinstance(
            getattr(signal.getsignal(signum), "__self__", None),
            contained._SignalHandlerInvocationTracker,
        )
        if (
            signum == signal.SIGINT
            and handler is previous_handlers[signum]
            and not removing_tracker
        ):
            restore_attempts += 1
            assert set(active_signals) <= real_sigmask(signal.SIG_BLOCK, set())
            if restore_attempts == 1:
                raise restore_failure
        return real_signal(signum, handler)  # type: ignore[arg-type]

    monkeypatch.setattr(contained.signal, "signal", fail_first_sigint_restore)
    try:
        with pytest.raises(KeyboardInterrupt) as caught:
            contained._finish_signal_restoration(
                previous_handlers,
                active_signals,
                latch,
                [],
                primary_exception=None,
                error_label="contained subprocess cleanup failures",
            )
        observed_mask = real_sigmask(signal.SIG_BLOCK, set())
        observed_handlers = {signum: signal.getsignal(signum) for signum in watched}
    finally:
        for signum, previous in before_handlers.items():
            real_signal(signum, previous)
        real_sigmask(signal.SIG_SETMASK, before_mask)

    assert caught.value is first
    assert restore_attempts == 2
    assert observed_mask == before_mask
    assert observed_handlers == previous_handlers
    cleanup_group = getattr(caught.value, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert restore_failure in cleanup_group.exceptions


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="atomic signal-mask arbitration requires pthread_sigmask",
)
def test_persistent_partial_signal_restoration_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_signal = contained.signal.signal
    real_sigmask = contained.signal.pthread_sigmask
    watched = (signal.SIGINT, signal.SIGTERM)
    before_handlers = {signum: signal.getsignal(signum) for signum in watched}
    before_mask = real_sigmask(signal.SIG_BLOCK, set())
    latch = contained._ContainedSignalLatch()
    first = KeyboardInterrupt()
    restore_failures: list[OSError] = []

    def previous_handler(_signum: int, _frame: object) -> None:
        raise first

    for signum in watched:
        real_signal(signum, previous_handler)
    previous_handlers, active_signals = contained._install_signal_latch(latch)
    latch.handle(signal.SIGTERM, None)

    def fail_sigint_restore(signum: int, handler: object) -> object:
        if signum == signal.SIGINT and handler is previous_handlers[signum]:
            assert set(active_signals) <= real_sigmask(signal.SIG_BLOCK, set())
            failure = OSError(f"persistent partial restore boom {len(restore_failures) + 1}")
            restore_failures.append(failure)
            raise failure
        return real_signal(signum, handler)  # type: ignore[arg-type]

    monkeypatch.setattr(contained.signal, "signal", fail_sigint_restore)
    try:
        with pytest.raises(KeyboardInterrupt) as caught:
            contained._finish_signal_restoration(
                previous_handlers,
                active_signals,
                latch,
                [],
                primary_exception=None,
                error_label="contained subprocess cleanup failures",
            )
        observed_mask = real_sigmask(signal.SIG_BLOCK, set())
        observed_sigint_handler = signal.getsignal(signal.SIGINT)
    finally:
        for signum, previous in before_handlers.items():
            real_signal(signum, previous)
        real_sigmask(signal.SIG_SETMASK, before_mask)

    assert caught.value is first
    assert len(restore_failures) == contained._SIGNAL_STATE_ATTEMPTS
    assert set(active_signals) <= observed_mask
    assert isinstance(
        getattr(observed_sigint_handler, "__self__", None),
        contained._ContainedSignalLatch,
    )
    cleanup_group = getattr(caught.value, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert cleanup_group.exceptions[: len(restore_failures)] == tuple(restore_failures)


def test_latched_first_signal_survives_helper_return_boundary_signal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_restore = contained._restore_signal_handlers_atomically
    real_signal = contained.signal.signal
    watched = (signal.SIGINT, signal.SIGTERM)
    before_handlers = {signum: signal.getsignal(signum) for signum in watched}
    restored = False
    later = InterruptedError("helper return boundary signal")

    def first_handler(_signum: int, _frame: object) -> None:
        raise SystemExit(128 + signal.SIGTERM)

    def later_handler(_signum: int, _frame: object) -> None:
        raise later

    real_signal(signal.SIGTERM, first_handler)
    real_signal(signal.SIGINT, later_handler)

    def restore_then_interrupt(
        previous_handlers: dict[int, object],
        active_signals: frozenset[int],
        latch: contained._ContainedSignalLatch,
    ) -> object:
        nonlocal restored
        latch.handle(signal.SIGTERM, None)
        restoration = real_restore(previous_handlers, active_signals, latch)
        restored = True
        release = restoration.release

        def release_with_queued_interrupt() -> None:
            release()
            later_handler(signal.SIGINT, None)

        restoration.release = release_with_queued_interrupt  # type: ignore[method-assign]
        return restoration

    monkeypatch.setattr(contained, "_restore_signal_handlers_atomically", restore_then_interrupt)
    try:
        with pytest.raises(SystemExit) as caught:
            contained.run_contained(
                [sys.executable, "-c", "pass"],
                cwd=tmp_path,
                deadline_monotonic=time.monotonic() + 2,
                may_spawn_background_descendants=False,
            )
    finally:
        for signum, previous in before_handlers.items():
            real_signal(signum, previous)

    assert restored
    assert caught.value.code == 128 + signal.SIGTERM
    cleanup_group = getattr(caught.value, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert later in cleanup_group.exceptions


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="atomic signal-mask arbitration requires pthread_sigmask",
)
def test_first_callable_signal_survives_second_signal_after_release_returns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_signal = contained.signal.signal
    watched = (signal.SIGINT, signal.SIGTERM)
    before_handlers = {signum: signal.getsignal(signum) for signum in watched}
    before_mask = signal.pthread_sigmask(signal.SIG_BLOCK, set())
    latch = contained._ContainedSignalLatch()
    first = KeyboardInterrupt()
    second = InterruptedError("second signal at post-release boundary")
    armed = False
    injected = False

    def first_handler(_signum: int, _frame: object) -> None:
        raise first

    def second_handler(_signum: int, _frame: object) -> None:
        raise second

    real_signal(signal.SIGTERM, first_handler)
    real_signal(signal.SIGINT, second_handler)
    previous_handlers, active_signals = contained._install_signal_latch(latch)
    latch.handle(signal.SIGTERM, None)
    restoration = contained._restore_signal_handlers_atomically(
        previous_handlers,
        active_signals,
        latch,
    )
    monkeypatch.setattr(
        contained,
        "_restore_signal_handlers_atomically",
        lambda *_args: restoration,
    )
    release_code = restoration.release.__func__.__code__
    release_and_replay_code = restoration.release_and_replay.__func__.__code__

    def trace_release_return(frame: object, event: str, _arg: object) -> object:
        nonlocal armed, injected
        code = getattr(frame, "f_code", None)
        if code is release_code and event == "return":
            armed = True
        elif armed and code is release_and_replay_code and event == "line" and not injected:
            injected = True
            sys.settrace(None)
            second_handler(signal.SIGINT, None)
        return trace_release_return

    try:
        sys.settrace(trace_release_return)
        with pytest.raises(KeyboardInterrupt) as caught:
            contained._finish_signal_restoration(
                previous_handlers,
                active_signals,
                latch,
                [],
                primary_exception=None,
                error_label="contained subprocess cleanup failures",
            )
    finally:
        sys.settrace(None)
        signal.pthread_sigmask(signal.SIG_SETMASK, before_mask)
        for signum, previous in before_handlers.items():
            real_signal(signum, previous)

    assert caught.value is first
    assert injected
    cleanup_group = getattr(caught.value, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert second in cleanup_group.exceptions


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="atomic signal-mask arbitration requires pthread_sigmask",
)
@pytest.mark.parametrize(
    ("injection_kind", "boundary_source", "occurrence", "injection_limit"),
    (
        ("line", "if protected_replay_error is None:", 0, 1),
        ("attach", "cleanup attachment", 0, 1),
        ("line", "raise protected_replay_error", 0, 1),
        (
            "attach",
            "persistent cleanup attachment",
            0,
            contained._SIGNAL_STATE_ATTEMPTS + 1,
        ),
    ),
)
def test_first_signal_survives_every_post_unmask_replay_boundary(
    monkeypatch: pytest.MonkeyPatch,
    injection_kind: str,
    boundary_source: str,
    occurrence: int,
    injection_limit: int,
) -> None:
    real_signal = contained.signal.signal
    watched = (signal.SIGINT, signal.SIGTERM)
    before_handlers = {signum: signal.getsignal(signum) for signum in watched}
    before_mask = signal.pthread_sigmask(signal.SIG_BLOCK, set())
    latch = contained._ContainedSignalLatch()
    first = KeyboardInterrupt()
    second = InterruptedError(f"second signal before {boundary_source}")
    injections = 0

    def first_handler(_signum: int, _frame: object) -> None:
        raise first

    def second_handler(_signum: int, _frame: object) -> None:
        raise second

    real_signal(signal.SIGTERM, first_handler)
    real_signal(signal.SIGINT, second_handler)
    previous_handlers, active_signals = contained._install_signal_latch(latch)
    latch.handle(signal.SIGTERM, None)
    restoration = contained._restore_signal_handlers_atomically(
        previous_handlers,
        active_signals,
        latch,
    )
    target_line = -1
    if injection_kind == "line":
        source_lines, first_line = inspect.getsourcelines(restoration.release_and_replay.__func__)
        matching_lines = [
            first_line + offset
            for offset, source_line in enumerate(source_lines)
            if source_line.strip() == boundary_source
        ]
        target_line = matching_lines[occurrence]
    release_and_replay_code = restoration.release_and_replay.__func__.__code__
    real_attach_cleanup = contained._attach_cleanup_error_group

    def inject_second_signal(frame: object, event: str, _arg: object) -> object:
        nonlocal injections
        if (
            injections < injection_limit
            and getattr(frame, "f_code", None) is release_and_replay_code
            and event == "line"
            and getattr(frame, "f_lineno", None) == target_line
        ):
            injections += 1
            second_handler(signal.SIGINT, None)
        return inject_second_signal

    def fail_cleanup_attachment(*args: object, **kwargs: object) -> None:
        nonlocal injections
        real_attach_cleanup(*args, **kwargs)  # type: ignore[arg-type]
        if injections < injection_limit:
            injections += 1
            second_handler(signal.SIGINT, None)

    cleanup_evidence = [OSError("existing cleanup evidence")]
    try:
        if injection_kind == "line":
            sys.settrace(inject_second_signal)
        else:
            monkeypatch.setattr(
                contained,
                "_attach_cleanup_error_group",
                fail_cleanup_attachment,
            )
        with pytest.raises(KeyboardInterrupt) as caught:
            restoration.release_and_replay(
                latch,
                previous_handlers,
                cleanup_evidence,
                error_label="contained subprocess cleanup failures",
            )
    finally:
        sys.settrace(None)
        signal.pthread_sigmask(signal.SIG_SETMASK, before_mask)
        for signum, previous in before_handlers.items():
            real_signal(signum, previous)

    assert caught.value is first
    assert injections == injection_limit
    cleanup_group = getattr(caught.value, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert second in cleanup_group.exceptions
    assert sum(error is second for error in cleanup_group.exceptions) == 1


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="atomic signal-mask arbitration requires pthread_sigmask",
)
def test_failed_signal_mask_release_remains_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_sigmask = contained.signal.pthread_sigmask
    before_mask = real_sigmask(signal.SIG_BLOCK, set())
    managed = {signal.SIGTERM}
    real_sigmask(signal.SIG_BLOCK, managed)
    restoration = contained._SignalRestoration(
        (),
        previous_mask=before_mask,
        blocked_mask={*before_mask, *managed},
    )
    failure = OSError("first unmask boom")
    first = KeyboardInterrupt()
    latch = contained._ContainedSignalLatch()
    latch.handle(signal.SIGTERM, None)
    attempts = 0

    def previous_handler(_signum: int, _frame: object) -> None:
        raise first

    def fail_first_unmask(how: int, mask: object) -> set[signal.Signals]:
        nonlocal attempts
        if how == signal.SIG_SETMASK:
            attempts += 1
            if attempts == 1:
                raise failure
        return real_sigmask(how, mask)  # type: ignore[arg-type]

    monkeypatch.setattr(contained.signal, "pthread_sigmask", fail_first_unmask)
    try:
        with pytest.raises(OSError) as caught:
            restoration.release()
        assert caught.value is failure
        assert not restoration._released
        assert signal.SIGTERM in real_sigmask(signal.SIG_BLOCK, set())

        with pytest.raises(KeyboardInterrupt) as replayed:
            restoration.release_and_replay(
                latch,
                {signal.SIGTERM: previous_handler},
                [failure],
                error_label="contained subprocess cleanup failures",
            )

        assert replayed.value is first
        assert restoration._released
        assert real_sigmask(signal.SIG_BLOCK, set()) == before_mask
        assert attempts == 2
        cleanup_group = getattr(replayed.value, "cleanup_error_group", None)
        assert isinstance(cleanup_group, BaseExceptionGroup)
        assert cleanup_group.exceptions == (failure,)
    finally:
        real_sigmask(signal.SIG_SETMASK, before_mask)


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="atomic signal-mask arbitration requires pthread_sigmask",
)
def test_signal_mask_release_commits_only_after_verification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_sigmask = contained.signal.pthread_sigmask
    before_mask = real_sigmask(signal.SIG_BLOCK, set())
    managed = {signal.SIGTERM}
    real_sigmask(signal.SIG_BLOCK, managed)
    restoration = contained._SignalRestoration(
        (),
        previous_mask=before_mask,
        blocked_mask={*before_mask, *managed},
    )
    setmask_calls = 0

    def ignore_setmask(how: int, mask: object) -> set[signal.Signals]:
        nonlocal setmask_calls
        if how == signal.SIG_SETMASK:
            setmask_calls += 1
            return real_sigmask(signal.SIG_BLOCK, set())
        return real_sigmask(how, mask)  # type: ignore[arg-type]

    monkeypatch.setattr(contained.signal, "pthread_sigmask", ignore_setmask)
    try:
        with pytest.raises(contained.ContainedProcessError, match="could not be verified"):
            restoration.release()

        assert not restoration._released
        assert signal.SIGTERM in real_sigmask(signal.SIG_BLOCK, set())
        assert setmask_calls == 1
    finally:
        real_sigmask(signal.SIG_SETMASK, before_mask)


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="atomic signal-mask arbitration requires pthread_sigmask",
)
def test_first_signal_survives_persistent_unmask_failure_without_committing_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_sigmask = contained.signal.pthread_sigmask
    before_mask = real_sigmask(signal.SIG_BLOCK, set())
    managed = {signal.SIGTERM}
    real_sigmask(signal.SIG_BLOCK, managed)
    latch = contained._ContainedSignalLatch()
    latch.handle(signal.SIGTERM, None)
    first = KeyboardInterrupt()
    failures: list[OSError] = []
    restoration = contained._SignalRestoration(
        (),
        previous_mask=before_mask,
        blocked_mask={*before_mask, *managed},
    )

    def previous_handler(_signum: int, _frame: object) -> None:
        raise first

    def fail_unmask(how: int, mask: object) -> set[signal.Signals]:
        if how == signal.SIG_SETMASK and set(mask) == before_mask:  # type: ignore[arg-type]
            failure = OSError(f"persistent unmask boom {len(failures) + 1}")
            failures.append(failure)
            raise failure
        return real_sigmask(how, mask)  # type: ignore[arg-type]

    monkeypatch.setattr(contained.signal, "pthread_sigmask", fail_unmask)
    try:
        with pytest.raises(KeyboardInterrupt) as caught:
            restoration.release_and_replay(
                latch,
                {signal.SIGTERM: previous_handler},
                [],
                error_label="contained subprocess cleanup failures",
            )

        assert caught.value is first
        assert not restoration._released
        assert signal.SIGTERM in real_sigmask(signal.SIG_BLOCK, set())
        cleanup_group = getattr(caught.value, "cleanup_error_group", None)
        assert isinstance(cleanup_group, BaseExceptionGroup)
        assert cleanup_group.exceptions == tuple(failures)
        assert len(failures) == contained._SIGNAL_STATE_ATTEMPTS
    finally:
        real_sigmask(signal.SIG_SETMASK, before_mask)


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="atomic signal-mask arbitration requires pthread_sigmask",
)
def test_first_signal_survives_terminal_replay_boundary_after_guard_exhaustion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_signal = contained.signal.signal
    real_sigmask = contained.signal.pthread_sigmask
    host_handlers, host_mask, _starting_mask = _prepare_unblocked_signal_host()
    latch = contained._ContainedSignalLatch()
    first = KeyboardInterrupt()
    second = InterruptedError("second signal at terminal replay boundary")
    attachment_injections = 0
    terminal_occurrences = 0
    terminal_masks: list[set[signal.Signals]] = []

    def first_handler(_signum: int, _frame: object) -> None:
        raise first

    def second_handler(_signum: int, _frame: object) -> None:
        raise second

    real_signal(signal.SIGTERM, first_handler)
    real_signal(signal.SIGINT, second_handler)
    previous_handlers, active_signals = contained._install_signal_latch(latch)
    latch.handle(signal.SIGTERM, None)
    restoration = contained._restore_signal_handlers_atomically(
        previous_handlers,
        active_signals,
        latch,
    )
    source_lines, first_line = inspect.getsourcelines(restoration.release_and_replay.__func__)
    replay_raise_lines = [
        first_line + offset
        for offset, source_line in enumerate(source_lines)
        if source_line.strip() == "raise protected_replay_error"
    ]
    terminal_line = replay_raise_lines[-1]
    release_and_replay_code = restoration.release_and_replay.__func__.__code__
    real_attach_cleanup = contained._attach_cleanup_error_group

    def fail_guarded_attachments(*args: object, **kwargs: object) -> None:
        nonlocal attachment_injections
        real_attach_cleanup(*args, **kwargs)  # type: ignore[arg-type]
        if attachment_injections < contained._SIGNAL_STATE_ATTEMPTS:
            attachment_injections += 1
            second_handler(signal.SIGINT, None)

    def inject_at_terminal_raise(frame: object, event: str, _arg: object) -> object:
        nonlocal terminal_occurrences
        if (
            getattr(frame, "f_code", None) is release_and_replay_code
            and event == "line"
            and getattr(frame, "f_lineno", None) == terminal_line
        ):
            terminal_occurrences += 1
            observed = real_sigmask(signal.SIG_BLOCK, set())
            terminal_masks.append(observed)
            if not set(active_signals) <= observed:
                second_handler(signal.SIGINT, None)
        return inject_at_terminal_raise

    monkeypatch.setattr(
        contained,
        "_attach_cleanup_error_group",
        fail_guarded_attachments,
    )
    try:
        sys.settrace(inject_at_terminal_raise)
        with pytest.raises(BaseException) as caught:
            restoration.release_and_replay(
                latch,
                previous_handlers,
                [OSError("existing cleanup evidence")],
                error_label="contained subprocess cleanup failures",
            )
    finally:
        sys.settrace(None)
        _restore_signal_host(host_handlers, host_mask)

    assert caught.value is first
    assert attachment_injections == contained._SIGNAL_STATE_ATTEMPTS
    assert terminal_occurrences == 1
    assert set(active_signals) <= terminal_masks[0]
    cleanup_group = getattr(caught.value, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert sum(error is second for error in cleanup_group.exceptions) == 1


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="atomic signal-mask arbitration requires pthread_sigmask",
)
def test_unmask_exception_after_mutation_restores_blocked_state_before_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_sigmask = contained.signal.pthread_sigmask
    host_handlers, host_mask, starting_mask = _prepare_unblocked_signal_host()
    latch = contained._ContainedSignalLatch()
    attempt_masks: list[set[signal.Signals]] = []
    failures: list[OSError] = []

    previous_handlers, active_signals = contained._install_signal_latch(latch)
    restoration = contained._restore_signal_handlers_atomically(
        previous_handlers,
        active_signals,
        latch,
    )

    def mutate_then_fail_unmask(how: int, mask: object) -> set[signal.Signals]:
        target = set(mask)  # type: ignore[arg-type]
        if how == signal.SIG_SETMASK and target == starting_mask:
            attempt_masks.append(real_sigmask(signal.SIG_BLOCK, set()))
            real_sigmask(how, target)
            failure = OSError(f"unmask mutation boom {len(failures) + 1}")
            failures.append(failure)
            raise failure
        return real_sigmask(how, target)

    monkeypatch.setattr(contained.signal, "pthread_sigmask", mutate_then_fail_unmask)
    try:
        with pytest.raises(OSError) as caught:
            restoration.release_and_replay(
                latch,
                previous_handlers,
                [],
                error_label="contained subprocess cleanup failures",
            )
        observed_mask = real_sigmask(signal.SIG_BLOCK, set())
    finally:
        _restore_signal_host(host_handlers, host_mask)

    assert caught.value is failures[0]
    assert len(attempt_masks) == contained._SIGNAL_STATE_ATTEMPTS
    assert all(set(active_signals) <= mask for mask in attempt_masks)
    assert set(active_signals) <= observed_mask
    assert not restoration._released


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="atomic signal-mask arbitration requires pthread_sigmask",
)
def test_installation_terminates_when_latch_is_unblocked_and_cannot_be_recovered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_sigmask = contained.signal.pthread_sigmask
    host_handlers, host_mask, starting_mask = _prepare_unblocked_signal_host()
    active = set(_MANAGED_TEST_SIGNALS)
    release_verification_failures = 0
    awaiting_release_verification = False
    reblock_failures: list[OSError] = []
    termination_states: list[tuple[set[signal.Signals], dict[int, object]]] = []

    class UnsafeSignalState(BaseException):
        pass

    def fail_release_and_reblock(how: int, mask: object) -> set[signal.Signals]:
        nonlocal awaiting_release_verification, release_verification_failures
        target = set(mask)  # type: ignore[arg-type]
        if how == signal.SIG_SETMASK and target == starting_mask:
            result = real_sigmask(how, target)
            awaiting_release_verification = True
            return result
        if how == signal.SIG_BLOCK and not target and awaiting_release_verification:
            awaiting_release_verification = False
            release_verification_failures += 1
            raise OSError("release verification boom")
        if release_verification_failures and (
            (how == signal.SIG_BLOCK and active <= target)
            or (how == signal.SIG_SETMASK and active <= target)
        ):
            failure = OSError(f"reblock boom {len(reblock_failures) + 1}")
            reblock_failures.append(failure)
            raise failure
        return real_sigmask(how, target)

    def terminate_unsafe_state(*_args: object, **_kwargs: object) -> None:
        termination_states.append(
            (
                real_sigmask(signal.SIG_BLOCK, set()),
                {signum: signal.getsignal(signum) for signum in _MANAGED_TEST_SIGNALS},
            )
        )
        raise UnsafeSignalState()

    monkeypatch.setattr(contained.signal, "pthread_sigmask", fail_release_and_reblock)
    monkeypatch.setattr(
        contained,
        "_terminate_unsafe_signal_state",
        terminate_unsafe_state,
        raising=False,
    )
    try:
        with pytest.raises(UnsafeSignalState):
            contained._install_signal_latch(contained._ContainedSignalLatch())
    finally:
        _restore_signal_host(host_handlers, host_mask)

    assert release_verification_failures >= 1
    assert len(reblock_failures) == contained._SIGNAL_STATE_ATTEMPTS
    assert len(termination_states) == 1
    unsafe_mask, unsafe_handlers = termination_states[0]
    assert not active <= unsafe_mask
    assert all(
        isinstance(getattr(handler, "__self__", None), contained._ContainedSignalLatch)
        for handler in unsafe_handlers.values()
    )


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="atomic signal-mask arbitration requires pthread_sigmask",
)
def test_restoration_fallback_preserves_pretransition_mask_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_sigmask = contained.signal.pthread_sigmask
    host_handlers, host_mask, starting_mask = _prepare_unblocked_signal_host()
    latch = contained._ContainedSignalLatch()
    verification_failures = 0
    awaiting_verification = False

    previous_handlers, active_signals = contained._install_signal_latch(latch)

    def exhaust_block_verification(how: int, mask: object) -> set[signal.Signals]:
        nonlocal awaiting_verification, verification_failures
        target = set(mask)  # type: ignore[arg-type]
        if (
            how == signal.SIG_BLOCK
            and target
            and verification_failures < contained._SIGNAL_STATE_ATTEMPTS
        ):
            result = real_sigmask(how, target)
            awaiting_verification = True
            return result
        if how == signal.SIG_BLOCK and not target and awaiting_verification:
            awaiting_verification = False
            verification_failures += 1
            raise OSError(f"block verification boom {verification_failures}")
        return real_sigmask(how, target)

    monkeypatch.setattr(contained.signal, "pthread_sigmask", exhaust_block_verification)
    try:
        restoration = contained._restore_signal_handlers_atomically(
            previous_handlers,
            active_signals,
            latch,
        )
    finally:
        _restore_signal_host(host_handlers, host_mask)

    assert verification_failures == contained._SIGNAL_STATE_ATTEMPTS
    assert restoration._previous_mask == starting_mask


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="atomic signal-mask arbitration requires pthread_sigmask",
)
def test_transient_release_failure_is_cleanup_evidence_for_existing_primary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_sigmask = contained.signal.pthread_sigmask
    host_handlers, host_mask, starting_mask = _prepare_unblocked_signal_host()
    latch = contained._ContainedSignalLatch()
    primary = RuntimeError("existing primary")
    release_failure = OSError("transient release boom")
    release_attempts = 0

    previous_handlers, active_signals = contained._install_signal_latch(latch)
    restoration = contained._restore_signal_handlers_atomically(
        previous_handlers,
        active_signals,
        latch,
    )

    def fail_first_release(how: int, mask: object) -> set[signal.Signals]:
        nonlocal release_attempts
        target = set(mask)  # type: ignore[arg-type]
        if how == signal.SIG_SETMASK and target == starting_mask:
            release_attempts += 1
            if release_attempts == 1:
                real_sigmask(how, target)
                raise release_failure
        return real_sigmask(how, target)

    monkeypatch.setattr(
        contained,
        "_restore_signal_handlers_atomically",
        lambda *_args: restoration,
    )
    monkeypatch.setattr(contained.signal, "pthread_sigmask", fail_first_release)
    try:
        with pytest.raises(RuntimeError) as caught:
            try:
                raise primary
            finally:
                contained._finish_signal_restoration(
                    previous_handlers,
                    active_signals,
                    latch,
                    [],
                    primary_exception=sys.exception(),
                    error_label="contained subprocess cleanup failures",
                )
        observed_mask = real_sigmask(signal.SIG_BLOCK, set())
    finally:
        _restore_signal_host(host_handlers, host_mask)

    assert caught.value is primary
    assert release_attempts == 2
    assert restoration._released
    assert observed_mask == starting_mask
    cleanup_group = getattr(caught.value, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert cleanup_group.exceptions == (release_failure,)


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="atomic signal-mask arbitration requires pthread_sigmask",
)
def test_unlatched_signal_during_unmask_displaces_existing_primary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_signal = contained.signal.signal
    real_sigmask = contained.signal.pthread_sigmask
    host_handlers, host_mask, starting_mask = _prepare_unblocked_signal_host()
    latch = contained._ContainedSignalLatch()
    primary = RuntimeError("existing primary")
    first_signal = KeyboardInterrupt()
    release_attempts = 0
    transition_masks: list[set[signal.Signals]] = []

    def previous_handler(_signum: int, _frame: object) -> None:
        raise first_signal

    for signum in _MANAGED_TEST_SIGNALS:
        real_signal(signum, previous_handler)
    previous_handlers, active_signals = contained._install_signal_latch(latch)
    restoration = contained._restore_signal_handlers_atomically(
        previous_handlers,
        active_signals,
        latch,
    )

    def deliver_signal_after_unmask(how: int, mask: object) -> set[signal.Signals]:
        nonlocal release_attempts
        target = set(mask)  # type: ignore[arg-type]
        if how == signal.SIG_SETMASK and target == starting_mask:
            release_attempts += 1
            result = real_sigmask(how, target)
            if release_attempts == 1:
                transition_masks.append(real_sigmask(signal.SIG_BLOCK, set()))
                handler = signal.getsignal(signal.SIGTERM)
                assert handler is not previous_handler
                assert callable(handler)
                handler(signal.SIGTERM, None)
            return result
        return real_sigmask(how, target)

    monkeypatch.setattr(
        contained,
        "_restore_signal_handlers_atomically",
        lambda *_args: restoration,
    )
    monkeypatch.setattr(contained.signal, "pthread_sigmask", deliver_signal_after_unmask)
    try:
        with pytest.raises(KeyboardInterrupt) as caught:
            try:
                raise primary
            finally:
                contained._finish_signal_restoration(
                    previous_handlers,
                    active_signals,
                    latch,
                    [],
                    primary_exception=sys.exception(),
                    error_label="contained subprocess cleanup failures",
                )
        observed_mask = real_sigmask(signal.SIG_BLOCK, set())
    finally:
        _restore_signal_host(host_handlers, host_mask)

    assert caught.value is first_signal
    assert latch.first_signum is None
    assert transition_masks == [starting_mask]
    assert release_attempts == 2
    assert restoration._released
    assert observed_mask == starting_mask
    assert not hasattr(caught.value, "cleanup_error_group")


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="atomic signal-mask arbitration requires pthread_sigmask",
)
def test_builtin_handler_exception_during_unmask_displaces_existing_primary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_signal = contained.signal.signal
    real_sigmask = contained.signal.pthread_sigmask
    host_handlers, host_mask, starting_mask = _prepare_unblocked_signal_host()
    latch = contained._ContainedSignalLatch()
    primary = RuntimeError("existing primary")
    boundary_errors: list[BaseException] = []
    release_attempts = 0

    for signum in _MANAGED_TEST_SIGNALS:
        real_signal(signum, pow)
    previous_handlers, active_signals = contained._install_signal_latch(latch)
    restoration = contained._restore_signal_handlers_atomically(
        previous_handlers,
        active_signals,
        latch,
    )

    def deliver_builtin_after_unmask(how: int, mask: object) -> set[signal.Signals]:
        nonlocal release_attempts
        target = set(mask)  # type: ignore[arg-type]
        if how == signal.SIG_SETMASK and target == starting_mask:
            release_attempts += 1
            if release_attempts == 1:
                os.kill(os.getpid(), signal.SIGTERM)
                try:
                    previous_mask = real_sigmask(how, target)
                    time.sleep(0.01)
                    return previous_mask
                except BaseException as exc:
                    boundary_errors.append(exc)
                    raise
            return real_sigmask(how, target)
        return real_sigmask(how, target)

    monkeypatch.setattr(
        contained,
        "_restore_signal_handlers_atomically",
        lambda *_args: restoration,
    )
    monkeypatch.setattr(contained.signal, "pthread_sigmask", deliver_builtin_after_unmask)
    try:
        with pytest.raises(TypeError) as caught:
            try:
                raise primary
            finally:
                contained._finish_signal_restoration(
                    previous_handlers,
                    active_signals,
                    latch,
                    [],
                    primary_exception=sys.exception(),
                    error_label="contained subprocess cleanup failures",
                )
        observed_mask = real_sigmask(signal.SIG_BLOCK, set())
        observed_handlers = {signum: signal.getsignal(signum) for signum in _MANAGED_TEST_SIGNALS}
    finally:
        _restore_signal_host(host_handlers, host_mask)

    assert caught.value is boundary_errors[0]
    assert release_attempts == 2
    assert restoration._released
    assert observed_mask == starting_mask
    assert observed_handlers == previous_handlers


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="atomic signal-mask arbitration requires pthread_sigmask",
)
def test_same_code_mask_failure_remains_cleanup_for_existing_primary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_signal = contained.signal.signal
    real_sigmask = contained.signal.pthread_sigmask
    host_handlers, host_mask, starting_mask = _prepare_unblocked_signal_host()
    latch = contained._ContainedSignalLatch()
    primary = RuntimeError("existing primary")
    mask_failure = OSError("genuine mask operation failure")
    release_attempts = 0

    def make_raiser(error: BaseException) -> Callable[[int, object], None]:
        def raise_error(_signum: int, _frame: object) -> None:
            raise error

        return raise_error

    previous_handler = make_raiser(KeyboardInterrupt())
    unrelated_raiser = make_raiser(mask_failure)
    assert previous_handler.__code__ is unrelated_raiser.__code__
    for signum in _MANAGED_TEST_SIGNALS:
        real_signal(signum, previous_handler)
    previous_handlers, active_signals = contained._install_signal_latch(latch)
    restoration = contained._restore_signal_handlers_atomically(
        previous_handlers,
        active_signals,
        latch,
    )

    def fail_after_mutation(how: int, mask: object) -> set[signal.Signals]:
        nonlocal release_attempts
        target = set(mask)  # type: ignore[arg-type]
        if how == signal.SIG_SETMASK and target == starting_mask:
            release_attempts += 1
            result = real_sigmask(how, target)
            if release_attempts == 1:
                unrelated_raiser(signal.SIGTERM, None)
            return result
        return real_sigmask(how, target)

    monkeypatch.setattr(
        contained,
        "_restore_signal_handlers_atomically",
        lambda *_args: restoration,
    )
    monkeypatch.setattr(contained.signal, "pthread_sigmask", fail_after_mutation)
    try:
        with pytest.raises(RuntimeError) as caught:
            try:
                raise primary
            finally:
                contained._finish_signal_restoration(
                    previous_handlers,
                    active_signals,
                    latch,
                    [],
                    primary_exception=sys.exception(),
                    error_label="contained subprocess cleanup failures",
                )
        observed_mask = real_sigmask(signal.SIG_BLOCK, set())
        observed_handlers = {signum: signal.getsignal(signum) for signum in _MANAGED_TEST_SIGNALS}
    finally:
        _restore_signal_host(host_handlers, host_mask)

    assert caught.value is primary
    assert release_attempts == 2
    assert restoration._released
    assert observed_mask == starting_mask
    assert observed_handlers == previous_handlers
    cleanup_group = getattr(caught.value, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert cleanup_group.exceptions == (mask_failure,)


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="atomic signal-mask arbitration requires pthread_sigmask",
)
def test_first_real_signal_survives_second_signal_during_final_handoff() -> None:
    program = """
import os
import signal
import sys

from rquant import contained_subprocess as contained

managed = (signal.SIGINT, signal.SIGTERM)
real_signal = contained.signal.signal
real_sigmask = contained.signal.pthread_sigmask
host_handlers = {signum: signal.getsignal(signum) for signum in managed}
host_mask = real_sigmask(signal.SIG_BLOCK, set())
starting_mask = host_mask.difference(managed)
real_sigmask(signal.SIG_SETMASK, starting_mask)

primary = RuntimeError("existing primary")
first = KeyboardInterrupt("first queued signal")
second = InterruptedError("second queued signal at exact handoff")
first_queued = False
second_queued = False

def previous_handler(signum, _frame):
    if signum == signal.SIGTERM:
        raise first
    raise second

for signum in managed:
    real_signal(signum, previous_handler)
latch = contained._ContainedSignalLatch()
previous_handlers, active_signals = contained._install_signal_latch(latch)
restoration = contained._restore_signal_handlers_atomically(
    previous_handlers,
    active_signals,
    latch,
)

def queue_signals_at_handoffs(how, mask):
    global first_queued
    target = set(mask)
    if how == signal.SIG_SETMASK and target == starting_mask:
        current = signal.getsignal(signal.SIGTERM)
        if not first_queued and current is not previous_handler:
            first_queued = True
            os.kill(os.getpid(), signal.SIGTERM)
    return real_sigmask(how, target)

def queue_second_during_original_restore(signum, handler):
    global second_queued
    if first_queued and not second_queued and handler is previous_handler:
        second_queued = True
        os.kill(os.getpid(), signal.SIGINT)
    return real_signal(signum, handler)

contained.signal.pthread_sigmask = queue_signals_at_handoffs
contained.signal.signal = queue_second_during_original_restore
result = 90
try:
    cleanup_errors = list(restoration)
    try:
        try:
            raise primary
        finally:
            restoration.release_and_replay(
                latch,
                previous_handlers,
                cleanup_errors,
                primary_exception=sys.exception(),
                error_label="contained subprocess cleanup failures",
            )
    except BaseException as exc:
        cleanup_group = getattr(exc, "cleanup_error_group", None)
        result = 0 if (
            exc is first
            and first_queued
            and second_queued
            and isinstance(cleanup_group, BaseExceptionGroup)
            and second in cleanup_group.exceptions
            and real_sigmask(signal.SIG_BLOCK, set()) == starting_mask
            and all(signal.getsignal(signum) is previous_handler for signum in managed)
        ) else 91
finally:
    contained.signal.pthread_sigmask = real_sigmask
    contained.signal.signal = real_signal
    real_sigmask(signal.SIG_BLOCK, set(managed))
    for signum, handler in host_handlers.items():
        real_signal(signum, handler)
    real_sigmask(signal.SIG_SETMASK, host_mask)

os._exit(result)
"""

    completed = subprocess.run(
        [sys.executable, "-c", program],
        check=False,
        capture_output=True,
        timeout=5,
    )

    assert completed.returncode == 0, completed.stderr.decode(errors="replace")


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="atomic signal-mask arbitration requires pthread_sigmask",
)
@pytest.mark.parametrize(
    ("has_primary", "signal_count"),
    (
        pytest.param(True, 2, id="with-primary"),
        pytest.param(False, 2, id="without-primary"),
        pytest.param(True, 1, id="first-only-with-primary"),
        pytest.param(False, 1, id="first-only-without-primary"),
        pytest.param(False, 3, id="three-signals-without-primary"),
    ),
)
def test_returning_first_real_signal_remains_authoritative(
    has_primary: bool,
    signal_count: int,
) -> None:
    program = """
import functools
import os
import signal
import sys

from rquant import contained_subprocess as contained

has_primary = sys.argv[1] == "1"
signal_count = int(sys.argv[2])
managed = (signal.SIGINT, signal.SIGTERM)
real_signal = contained.signal.signal
real_sigmask = contained.signal.pthread_sigmask
host_handlers = {signum: signal.getsignal(signum) for signum in managed}
host_mask = real_sigmask(signal.SIG_BLOCK, set())
starting_mask = host_mask.difference(managed)
real_sigmask(signal.SIG_SETMASK, starting_mask)

class ReturningHandler:
    def __init__(self):
        self.calls = []

    def handle(self, signum, _frame):
        self.calls.append(signum)

returning = ReturningHandler()
later_calls = []
later_errors = [
    InterruptedError(f"later queued signal {index}")
    for index in range(2, signal_count + 1)
]

def raise_later(errors, calls, signum, _frame):
    error = errors[len(calls)]
    calls.append(signum)
    raise error

later_handler = functools.partial(raise_later, later_errors, later_calls)
real_signal(signal.SIGTERM, returning.handle)
real_signal(signal.SIGINT, later_handler)
latch = contained._ContainedSignalLatch()
previous_handlers, active_signals = contained._install_signal_latch(latch)
restoration = contained._restore_signal_handlers_atomically(
    previous_handlers,
    active_signals,
    latch,
)

first_queued = False
third_queued = False
real_release = restoration.release

def queue_first_at_unmask(how, mask):
    global first_queued
    target = set(mask)
    if how == signal.SIG_SETMASK and target == starting_mask and not first_queued:
        first_queued = True
        os.kill(os.getpid(), signal.SIGTERM)
    return real_sigmask(how, target)

def release_with_later_signal():
    result = real_release()
    if first_queued and len(later_calls) < len(later_errors):
        os.kill(os.getpid(), signal.SIGINT)
    return result

def restore_with_third_signal(signum, handler):
    global third_queued
    if (
        signal_count == 3
        and len(later_calls) == 1
        and not third_queued
        and handler is later_handler
    ):
        third_queued = True
        os.kill(os.getpid(), signal.SIGINT)
    return real_signal(signum, handler)

contained.signal.pthread_sigmask = queue_first_at_unmask
contained.signal.signal = restore_with_third_signal
restoration.release = release_with_later_signal
primary = RuntimeError("existing primary") if has_primary else None
result = 90
try:
    cleanup_errors = list(restoration)
    try:
        if primary is None:
            restoration.release_and_replay(
                latch,
                previous_handlers,
                cleanup_errors,
                primary_exception=None,
                error_label="contained subprocess cleanup failures",
            )
        else:
            try:
                raise primary
            finally:
                restoration.release_and_replay(
                    latch,
                    previous_handlers,
                    cleanup_errors,
                    primary_exception=sys.exception(),
                    error_label="contained subprocess cleanup failures",
                )
    except BaseException as exc:
        cleanup_group = getattr(exc, "cleanup_error_group", None)
        cleanup_matches = (
            cleanup_group is None
            if not later_errors
            else isinstance(cleanup_group, BaseExceptionGroup)
            and cleanup_group.exceptions == tuple(later_errors)
        )
        observed_mask = real_sigmask(signal.SIG_BLOCK, set())
        result = 0 if (
            type(exc) is InterruptedError
            and str(exc) == f"process runner interrupted by signal {signal.SIGTERM}"
            and exc is not primary
            and returning.calls == [signal.SIGTERM]
            and later_calls == [signal.SIGINT] * len(later_errors)
            and cleanup_matches
            and observed_mask == starting_mask
            and all(signal.getsignal(signum) is previous_handlers[signum] for signum in managed)
        ) else 91
finally:
    contained.signal.pthread_sigmask = real_sigmask
    contained.signal.signal = real_signal
    real_sigmask(signal.SIG_BLOCK, set(managed))
    for signum, handler in host_handlers.items():
        real_signal(signum, handler)
    real_sigmask(signal.SIG_SETMASK, host_mask)

os._exit(result)
"""

    completed = subprocess.run(
        [sys.executable, "-c", program, str(int(has_primary)), str(signal_count)],
        check=False,
        capture_output=True,
        timeout=5,
    )

    assert completed.returncode == 0, completed.stderr.decode(errors="replace")


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="atomic signal-mask arbitration requires pthread_sigmask",
)
@pytest.mark.parametrize("has_primary", (True, False), ids=("with-primary", "without-primary"))
@pytest.mark.parametrize("signal_count", (2, 3), ids=("two-signals", "three-signals"))
@pytest.mark.parametrize("later_mode", ("return", "raise"))
def test_swallowed_nested_signal_outcomes_remain_cleanup_evidence(
    has_primary: bool,
    signal_count: int,
    later_mode: str,
) -> None:
    program = """
import functools
import os
import signal
import sys

from rquant import contained_subprocess as contained

has_primary = sys.argv[1] == "1"
signal_count = int(sys.argv[2])
later_mode = sys.argv[3]
managed = (signal.SIGINT, signal.SIGTERM)
real_signal = contained.signal.signal
real_sigmask = contained.signal.pthread_sigmask
host_handlers = {signum: signal.getsignal(signum) for signum in managed}
host_mask = real_sigmask(signal.SIG_BLOCK, set())
starting_mask = host_mask.difference(managed)
real_sigmask(signal.SIG_SETMASK, starting_mask)

swallowed = []

class ReturningFirstHandler:
    def __init__(self):
        self.calls = []

    def handle(self, signum, _frame):
        self.calls.append(signum)
        for _index in range(signal_count - 1):
            try:
                os.kill(os.getpid(), signal.SIGINT)
            except BaseException as exc:
                swallowed.append(exc)

first_handler = ReturningFirstHandler()
later_calls = []
later_errors = [
    InterruptedError(f"exact later signal {index}")
    for index in range(2, signal_count + 1)
]

def handle_later(mode, errors, calls, signum, _frame):
    index = len(calls)
    calls.append(signum)
    if mode == "raise":
        raise errors[index]

later_handler = functools.partial(
    handle_later,
    later_mode,
    later_errors,
    later_calls,
)
real_signal(signal.SIGTERM, first_handler.handle)
real_signal(signal.SIGINT, later_handler)
latch = contained._ContainedSignalLatch()
previous_handlers, active_signals = contained._install_signal_latch(latch)
restoration = contained._restore_signal_handlers_atomically(
    previous_handlers,
    active_signals,
    latch,
)

first_queued = False

def queue_first_at_unmask(how, mask):
    global first_queued
    target = set(mask)
    if how == signal.SIG_SETMASK and target == starting_mask and not first_queued:
        first_queued = True
        os.kill(os.getpid(), signal.SIGTERM)
    return real_sigmask(how, target)

contained.signal.pthread_sigmask = queue_first_at_unmask
primary = RuntimeError("existing primary") if has_primary else None
result = 90
try:
    cleanup_errors = list(restoration)
    try:
        if primary is None:
            restoration.release_and_replay(
                latch,
                previous_handlers,
                cleanup_errors,
                primary_exception=None,
                error_label="contained subprocess cleanup failures",
            )
        else:
            try:
                raise primary
            finally:
                restoration.release_and_replay(
                    latch,
                    previous_handlers,
                    cleanup_errors,
                    primary_exception=sys.exception(),
                    error_label="contained subprocess cleanup failures",
                )
    except BaseException as exc:
        cleanup_group = getattr(exc, "cleanup_error_group", None)
        if later_mode == "raise":
            identities_match = all(
                observed is expected
                for observed, expected in zip(swallowed, later_errors, strict=True)
            )
        else:
            identities_match = all(
                type(error) is InterruptedError
                and str(error) == f"process runner interrupted by signal {signal.SIGINT}"
                for error in swallowed
            )
        result = 0 if (
            type(exc) is InterruptedError
            and str(exc) == f"process runner interrupted by signal {signal.SIGTERM}"
            and exc is not primary
            and first_handler.calls == [signal.SIGTERM]
            and later_calls == [signal.SIGINT] * (signal_count - 1)
            and len(swallowed) == signal_count - 1
            and identities_match
            and isinstance(cleanup_group, BaseExceptionGroup)
            and cleanup_group.exceptions == tuple(swallowed)
            and real_sigmask(signal.SIG_BLOCK, set()) == starting_mask
            and all(
                signal.getsignal(signum) is previous_handlers[signum]
                for signum in managed
            )
        ) else 91
finally:
    contained.signal.pthread_sigmask = real_sigmask
    real_sigmask(signal.SIG_BLOCK, set(managed))
    for signum, handler in host_handlers.items():
        real_signal(signum, handler)
    real_sigmask(signal.SIG_SETMASK, host_mask)

os._exit(result)
"""

    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            program,
            str(int(has_primary)),
            str(signal_count),
            later_mode,
        ],
        check=False,
        capture_output=True,
        timeout=5,
    )

    assert completed.returncode == 0, completed.stderr.decode(errors="replace")


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="atomic signal-mask arbitration requires pthread_sigmask",
)
@pytest.mark.parametrize("has_primary", (True, False), ids=("with-primary", "without-primary"))
@pytest.mark.parametrize("outer_mode", ("escape", "catch-reraise"))
def test_nested_exact_signal_exception_remains_authoritative(
    has_primary: bool,
    outer_mode: str,
) -> None:
    program = """
import os
import signal
import sys

from rquant import contained_subprocess as contained

has_primary = sys.argv[1] == "1"
outer_mode = sys.argv[2]
managed = (signal.SIGINT, signal.SIGTERM)
real_signal = contained.signal.signal
real_sigmask = contained.signal.pthread_sigmask
host_handlers = {signum: signal.getsignal(signum) for signum in managed}
host_mask = real_sigmask(signal.SIG_BLOCK, set())
starting_mask = host_mask.difference(managed)
real_sigmask(signal.SIG_SETMASK, starting_mask)

exact_error = (
    LookupError("nested signal escaped unchanged")
    if outer_mode == "escape"
    else RuntimeError("nested signal caught and re-raised")
)
outer_calls = []
inner_calls = []
caught_nested = []

def inner_handler(signum, _frame):
    inner_calls.append(signum)
    raise exact_error

def outer_handler(signum, _frame):
    outer_calls.append(signum)
    if outer_mode == "escape":
        os.kill(os.getpid(), signal.SIGINT)
        return
    try:
        os.kill(os.getpid(), signal.SIGINT)
    except BaseException as exc:
        caught_nested.append(exc)
        raise exact_error

real_signal(signal.SIGTERM, outer_handler)
real_signal(signal.SIGINT, inner_handler)
latch = contained._ContainedSignalLatch()
previous_handlers, active_signals = contained._install_signal_latch(latch)
restoration = contained._restore_signal_handlers_atomically(
    previous_handlers,
    active_signals,
    latch,
)

first_queued = False

def queue_first_at_unmask(how, mask):
    global first_queued
    target = set(mask)
    if how == signal.SIG_SETMASK and target == starting_mask and not first_queued:
        first_queued = True
        os.kill(os.getpid(), signal.SIGTERM)
    return real_sigmask(how, target)

contained.signal.pthread_sigmask = queue_first_at_unmask
primary = RuntimeError("existing primary") if has_primary else None
result = 90
try:
    cleanup_errors = list(restoration)
    try:
        if primary is None:
            restoration.release_and_replay(
                latch,
                previous_handlers,
                cleanup_errors,
                primary_exception=None,
                error_label="contained subprocess cleanup failures",
            )
        else:
            try:
                raise primary
            finally:
                restoration.release_and_replay(
                    latch,
                    previous_handlers,
                    cleanup_errors,
                    primary_exception=sys.exception(),
                    error_label="contained subprocess cleanup failures",
                )
    except BaseException as exc:
        expected_caught = [] if outer_mode == "escape" else [exact_error]
        result = 0 if (
            exc is exact_error
            and caught_nested == expected_caught
            and outer_calls == [signal.SIGTERM]
            and inner_calls == [signal.SIGINT]
            and getattr(exc, "cleanup_error_group", None) is None
            and real_sigmask(signal.SIG_BLOCK, set()) == starting_mask
            and all(
                signal.getsignal(signum) is previous_handlers[signum]
                for signum in managed
            )
        ) else 91
finally:
    contained.signal.pthread_sigmask = real_sigmask
    real_sigmask(signal.SIG_BLOCK, set(managed))
    for signum, handler in host_handlers.items():
        real_signal(signum, handler)
    real_sigmask(signal.SIG_SETMASK, host_mask)

os._exit(result)
"""

    completed = subprocess.run(
        [sys.executable, "-c", program, str(int(has_primary)), outer_mode],
        check=False,
        capture_output=True,
        timeout=5,
    )

    assert completed.returncode == 0, completed.stderr.decode(errors="replace")


def test_latched_signal_transfers_authority_to_outer_latch() -> None:
    outer_latch = contained._ContainedSignalLatch()

    replay_error = contained._latched_signal_replay_error(
        signal.SIGTERM,
        {signal.SIGTERM: outer_latch.handle},
    )

    assert replay_error is None
    assert outer_latch.first_signum == signal.SIGTERM


def test_latched_signal_replay_preserves_default_and_ignore_semantics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    signal_calls: list[tuple[int, object]] = []
    kill_calls: list[tuple[int, int]] = []

    monkeypatch.setattr(
        contained.signal,
        "signal",
        lambda signum, handler: signal_calls.append((signum, handler)),
    )
    monkeypatch.setattr(
        contained.os,
        "kill",
        lambda pid, signum: kill_calls.append((pid, signum)),
    )

    default_replay = contained._latched_signal_replay_error(
        signal.SIGTERM,
        {signal.SIGTERM: signal.SIG_DFL},
    )
    ignored_replay = contained._latched_signal_replay_error(
        signal.SIGTERM,
        {signal.SIGTERM: signal.SIG_IGN},
    )

    assert isinstance(default_replay, SystemExit)
    assert default_replay.code == 128 + signal.SIGTERM
    assert ignored_replay is None
    assert signal_calls == [(signal.SIGTERM, signal.SIG_DFL)]
    assert kill_calls == [(contained.os.getpid(), signal.SIGTERM)]


def test_signal_after_communicate_returns_is_latched_before_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_popen = contained.subprocess.Popen
    real_signal = contained.signal.signal
    previous_sigterm = signal.getsignal(signal.SIGTERM)
    handler_returned: list[bool] = []
    replayed: list[int] = []

    def previous_handler(signum: int, _frame: object) -> None:
        replayed.append(signum)

    def capturing_popen(*args: object, **kwargs: object) -> subprocess.Popen[str]:
        process = real_popen(*args, **kwargs)
        communicate = process.communicate
        emitted = False

        def communicate_with_signal(*args: object, **kwargs: object) -> tuple[str, str]:
            nonlocal emitted
            result = communicate(*args, **kwargs)
            if not emitted:
                emitted = True
                handler = signal.getsignal(signal.SIGTERM)
                assert callable(handler)
                handler(signal.SIGTERM, None)
                handler_returned.append(True)
            return result

        process.communicate = communicate_with_signal  # type: ignore[method-assign]
        return process

    real_signal(signal.SIGTERM, previous_handler)
    monkeypatch.setattr(contained.subprocess, "Popen", capturing_popen)
    try:
        with pytest.raises(InterruptedError, match="SIGTERM|signal 15"):
            contained.run_contained(
                [sys.executable, "-c", "pass"],
                cwd=tmp_path,
                deadline_monotonic=time.monotonic() + 2,
                may_spawn_background_descendants=False,
            )
    finally:
        real_signal(signal.SIGTERM, previous_sigterm)

    assert handler_returned == [True]
    assert replayed == [signal.SIGTERM]


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="atomic signal-mask arbitration requires pthread_sigmask",
)
def test_signal_during_partial_handler_restore_is_replayed_after_atomic_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = _CloseFailingKernelTracker()
    real_signal = contained.signal.signal
    real_sigpending = contained.signal.sigpending
    before = {signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)}
    replayed: list[int] = []
    queued_signals: set[int] = set()
    restoration_calls = 0
    injected = False

    def previous_handler(signum: int, _frame: object) -> None:
        assert restoration_calls >= 2
        replayed.append(signum)
        if signum == signal.SIGINT:
            raise KeyboardInterrupt

    def signal_with_restore_race(signum: int, handler: object) -> object:
        nonlocal restoration_calls, injected
        result = real_signal(signum, handler)  # type: ignore[arg-type]
        if handler is signal.SIG_IGN:
            queued_signals.discard(signum)
        installing_latch = isinstance(
            getattr(handler, "__self__", None), contained._ContainedSignalLatch
        )
        if handler is previous_handler and not installing_latch:
            restoration_calls += 1
            if restoration_calls == 1 and not injected:
                injected = True
                queued_signals.update((signal.SIGINT, signal.SIGTERM))
        return result

    def deterministic_pending() -> set[signal.Signals]:
        return {*real_sigpending(), *(signal.Signals(signum) for signum in queued_signals)}

    for signum in before:
        real_signal(signum, previous_handler)
    monkeypatch.setattr(contained.signal, "signal", signal_with_restore_race)
    monkeypatch.setattr(contained.signal, "sigpending", deterministic_pending)
    try:
        with pytest.raises(KeyboardInterrupt) as caught:
            contained.run_contained(
                [sys.executable, "-c", "pass"],
                cwd=tmp_path,
                deadline_monotonic=time.monotonic() + 2,
                kernel_tracker_factory=lambda: tracker,
                may_spawn_background_descendants=False,
            )
    finally:
        for signum, previous in before.items():
            real_signal(signum, previous)

    assert injected
    assert restoration_calls >= 2
    assert replayed == [signal.SIGINT]
    cleanup_group = getattr(caught.value, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert any("close boom" in str(error) for error in cleanup_group.exceptions)
    assert {signum: signal.getsignal(signum) for signum in before} == before


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="atomic signal-mask arbitration requires pthread_sigmask",
)
def test_signal_restore_arbitration_runs_after_process_deadline_expires(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_signal = contained.signal.signal
    real_sigpending = contained.signal.sigpending
    before = {signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)}
    replayed: list[int] = []
    latch = contained._ContainedSignalLatch()
    queued_signals: set[int] = set()
    injected = False

    def previous_handler(signum: int, _frame: object) -> None:
        replayed.append(signum)

    for signum in before:
        real_signal(signum, previous_handler)
    previous_handlers, active_signals = contained._install_signal_latch(latch)

    def signal_with_pending_delivery(signum: int, handler: object) -> object:
        nonlocal injected
        result = real_signal(signum, handler)  # type: ignore[arg-type]
        if handler is signal.SIG_IGN:
            queued_signals.discard(signum)
        if handler is previous_handler and not injected:
            injected = True
            queued_signals.add(signal.SIGINT)
        return result

    def deterministic_pending() -> set[signal.Signals]:
        return {*real_sigpending(), *(signal.Signals(signum) for signum in queued_signals)}

    monkeypatch.setattr(contained.signal, "signal", signal_with_pending_delivery)
    monkeypatch.setattr(contained.signal, "sigpending", deterministic_pending)
    try:
        errors = contained._restore_signal_handlers_atomically(
            previous_handlers,
            active_signals,
            latch,
        )
        errors.release()
    finally:
        for signum, previous in before.items():
            real_signal(signum, previous)

    assert injected
    assert errors == []
    assert latch.first_signum == signal.SIGINT
    assert replayed == []


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask") or not hasattr(signal, "set_wakeup_fd"),
    reason="host signal state verification requires POSIX signal APIs",
)
def test_run_contained_restores_host_signal_mask_wakeup_fd_and_handlers(
    tmp_path: Path,
) -> None:
    watched = {signal.SIGINT, signal.SIGTERM}
    before_handlers = {signum: signal.getsignal(signum) for signum in watched}
    before_mask = signal.pthread_sigmask(signal.SIG_BLOCK, set())
    read_fd, write_fd = contained.os.pipe()
    contained.os.set_blocking(write_fd, False)
    previous_wakeup_fd = signal.set_wakeup_fd(write_fd)
    try:
        completed = contained.run_contained(
            [sys.executable, "-c", "pass"],
            cwd=tmp_path,
            deadline_monotonic=time.monotonic() + 2,
            check=True,
            may_spawn_background_descendants=False,
        )
        observed_wakeup_fd = signal.set_wakeup_fd(-1)
        signal.set_wakeup_fd(observed_wakeup_fd)

        assert completed.returncode == 0
        assert signal.pthread_sigmask(signal.SIG_BLOCK, set()) == before_mask
        assert observed_wakeup_fd == write_fd
        assert {signum: signal.getsignal(signum) for signum in watched} == before_handlers
    finally:
        signal.set_wakeup_fd(previous_wakeup_fd)
        signal.pthread_sigmask(signal.SIG_SETMASK, before_mask)
        contained.os.close(write_fd)
        contained.os.close(read_fd)


def test_run_contained_no_signal_path_returns_normally(tmp_path: Path) -> None:
    completed = contained.run_contained(
        [sys.executable, "-c", "print('ok')"],
        cwd=tmp_path,
        deadline_monotonic=time.monotonic() + 2,
        check=True,
        may_spawn_background_descendants=False,
    )

    assert completed.returncode == 0
    assert completed.stdout.strip() == "ok"


def test_pipe_failure_closes_created_kernel_tracker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = _FakeKernelTracker(identity=contained.ProcessIdentity(1, (1, 0)))
    primary = OSError("pipe boom")
    monkeypatch.setattr(contained.os, "pipe", lambda: (_ for _ in ()).throw(primary))

    with pytest.raises(OSError) as caught:
        contained.run_contained(
            [sys.executable, "-c", "pass"],
            cwd=tmp_path,
            deadline_monotonic=time.monotonic() + 2,
            kernel_tracker_factory=lambda: tracker,
            may_spawn_background_descendants=False,
        )

    assert caught.value is primary
    assert tracker.closed


def test_popen_failure_closes_gate_descriptors_and_tracker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = _FakeKernelTracker(identity=contained.ProcessIdentity(1, (1, 0)))
    primary = OSError("spawn boom")
    real_pipe = contained.os.pipe
    gate_fds: list[int] = []

    def recording_pipe() -> tuple[int, int]:
        descriptors = real_pipe()
        gate_fds.extend(descriptors)
        return descriptors

    monkeypatch.setattr(contained.os, "pipe", recording_pipe)
    monkeypatch.setattr(
        contained.subprocess,
        "Popen",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(primary),
    )

    with pytest.raises(OSError) as caught:
        contained.run_contained(
            [sys.executable, "-c", "pass"],
            cwd=tmp_path,
            deadline_monotonic=time.monotonic() + 2,
            kernel_tracker_factory=lambda: tracker,
            may_spawn_background_descendants=False,
        )

    assert caught.value is primary
    assert tracker.closed
    assert len(gate_fds) == 2
    for descriptor in gate_fds:
        with pytest.raises(OSError):
            contained.os.fstat(descriptor)


def test_parent_gate_close_failure_reaps_blocked_root_and_closes_resources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = _FakeKernelTracker(identity=contained.ProcessIdentity(1, (1, 0)))
    primary = OSError("parent close boom")
    real_pipe = contained.os.pipe
    real_close = contained.os.close
    real_popen = contained.subprocess.Popen
    gate_fds: list[int] = []
    spawned: list[subprocess.Popen[str]] = []
    failed = False

    def recording_pipe() -> tuple[int, int]:
        descriptors = real_pipe()
        gate_fds.extend(descriptors)
        return descriptors

    def fail_first_parent_close(descriptor: int) -> None:
        nonlocal failed
        if gate_fds and descriptor == gate_fds[0] and not failed:
            failed = True
            raise primary
        real_close(descriptor)

    def capturing_popen(*args: object, **kwargs: object) -> subprocess.Popen[str]:
        process = real_popen(*args, **kwargs)
        spawned.append(process)
        monkeypatch.setattr(contained.os, "close", fail_first_parent_close)
        return process

    monkeypatch.setattr(contained.os, "pipe", recording_pipe)
    monkeypatch.setattr(contained.subprocess, "Popen", capturing_popen)

    with pytest.raises(OSError) as caught:
        contained.run_contained(
            [sys.executable, "-c", "pass"],
            cwd=tmp_path,
            deadline_monotonic=time.monotonic() + 2,
            kernel_tracker_factory=lambda: tracker,
            may_spawn_background_descendants=False,
        )

    assert caught.value is primary
    assert tracker.closed
    assert spawned and spawned[0].returncode is not None
    for descriptor in gate_fds:
        with pytest.raises(OSError):
            contained.os.fstat(descriptor)


def test_first_signal_is_replayed_when_tracker_close_fails_and_second_signal_arrives(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = _CloseFailingKernelTracker()
    real_cleanup = contained._cleanup_process_tree
    real_kill = contained.os.kill
    checks = 0
    replayed: list[int] = []

    def interrupt_on_second_check() -> bool:
        nonlocal checks
        checks += 1
        if checks == 2:
            handler = signal.getsignal(signal.SIGTERM)
            assert callable(handler)
            handler(signal.SIGTERM, None)
        return False

    def cleanup_with_consecutive_signal(*args: object, **kwargs: object) -> None:
        handler = signal.getsignal(signal.SIGINT)
        assert callable(handler)
        handler(signal.SIGINT, None)
        real_cleanup(*args, **kwargs)  # type: ignore[arg-type]

    def record_replay(pid: int, signum: int) -> None:
        if pid == contained.os.getpid():
            replayed.append(signum)
            return
        real_kill(pid, signum)

    monkeypatch.setattr(contained, "_cleanup_process_tree", cleanup_with_consecutive_signal)
    monkeypatch.setattr(contained.os, "kill", record_replay)

    with pytest.raises(SystemExit) as caught:
        contained.run_contained(
            [sys.executable, "-c", "pass"],
            cwd=tmp_path,
            deadline_monotonic=time.monotonic() + 2,
            cancellation_check=interrupt_on_second_check,
            kernel_tracker_factory=lambda: tracker,
            may_spawn_background_descendants=False,
        )

    assert caught.value.code == 128 + signal.SIGTERM
    assert replayed == [signal.SIGTERM]


def test_signal_is_replayed_when_process_cleanup_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_cleanup = contained._cleanup_process_tree
    real_kill = contained.os.kill
    checks = 0
    replayed: list[int] = []

    def interrupt_on_second_check() -> bool:
        nonlocal checks
        checks += 1
        if checks == 2:
            handler = signal.getsignal(signal.SIGTERM)
            assert callable(handler)
            handler(signal.SIGTERM, None)
        return False

    def failing_cleanup(*args: object, **kwargs: object) -> None:
        real_cleanup(*args, **kwargs)  # type: ignore[arg-type]
        raise contained.ContainedProcessError("kill boom")

    def record_replay(pid: int, signum: int) -> None:
        if pid == contained.os.getpid():
            replayed.append(signum)
            return
        real_kill(pid, signum)

    monkeypatch.setattr(contained, "_cleanup_process_tree", failing_cleanup)
    monkeypatch.setattr(contained.os, "kill", record_replay)

    with pytest.raises(SystemExit) as caught:
        contained.run_contained(
            [sys.executable, "-c", "pass"],
            cwd=tmp_path,
            deadline_monotonic=time.monotonic() + 2,
            cancellation_check=interrupt_on_second_check,
            may_spawn_background_descendants=False,
        )

    assert caught.value.code == 128 + signal.SIGTERM
    assert replayed == [signal.SIGTERM]


@pytest.mark.parametrize(
    "phase",
    ("gate_close", "tracker_join", "kernel_close", "handler_restore"),
)
def test_first_signal_arriving_during_cleanup_is_replayed_after_all_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
) -> None:
    before = {signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)}
    real_close = contained.os.close
    real_kill = contained.os.kill
    real_pipe = contained.os.pipe
    real_signal = contained.signal.signal
    gate_write = -1
    gate_cleanup_armed = False
    emitted = False
    replayed: list[int] = []

    def emit_two_signals() -> None:
        nonlocal emitted
        if emitted:
            return
        emitted = True
        first_error: BaseException | None = None
        for signum in (signal.SIGTERM, signal.SIGINT):
            handler = signal.getsignal(signum)
            assert callable(handler)
            try:
                handler(signum, None)
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise first_error

    class CleanupTracker(_CloseFailingKernelTracker):
        def close(self) -> None:
            if phase == "kernel_close":
                emit_two_signals()
            super().close()

    class CleanupThread:
        def __init__(self, **_kwargs: object) -> None:
            self.alive = False

        def start(self) -> None:
            self.alive = True

        def join(self, *, timeout: float) -> None:
            assert timeout >= 0
            if phase == "tracker_join":
                emit_two_signals()
            self.alive = False

        def is_alive(self) -> bool:
            return self.alive

    tracker = CleanupTracker()

    def recording_pipe() -> tuple[int, int]:
        nonlocal gate_write
        descriptors = real_pipe()
        if gate_write < 0:
            gate_write = descriptors[1]
        return descriptors

    def interrupting_write(descriptor: int, payload: bytes) -> int:
        nonlocal gate_cleanup_armed
        if phase == "gate_close" and descriptor == gate_write:
            gate_cleanup_armed = True
            raise RuntimeError("gate write boom")
        return original_write(descriptor, payload)

    original_write = contained.os.write

    def close_with_signal(descriptor: int) -> None:
        if phase == "gate_close" and gate_cleanup_armed and descriptor == gate_write:
            emit_two_signals()
        real_close(descriptor)

    def signal_with_cleanup_interrupt(signum: int, handler: object) -> object:
        installing_latch = isinstance(
            getattr(handler, "__self__", None), contained._ContainedSignalLatch
        )
        if phase == "handler_restore" and not installing_latch:
            emit_two_signals()
        return real_signal(signum, handler)  # type: ignore[arg-type]

    def record_replay(pid: int, signum: int) -> None:
        if pid == contained.os.getpid():
            replayed.append(signum)
            return
        real_kill(pid, signum)

    monkeypatch.setattr(contained.os, "pipe", recording_pipe)
    monkeypatch.setattr(contained.os, "write", interrupting_write)
    monkeypatch.setattr(contained.os, "close", close_with_signal)
    monkeypatch.setattr(contained.os, "kill", record_replay)
    monkeypatch.setattr(contained.signal, "signal", signal_with_cleanup_interrupt)
    if phase == "tracker_join":
        monkeypatch.setattr(contained.threading, "Thread", CleanupThread)

    try:
        with pytest.raises(SystemExit) as caught:
            contained.run_contained(
                [sys.executable, "-c", "pass"],
                cwd=tmp_path,
                deadline_monotonic=time.monotonic() + 2,
                kernel_tracker_factory=lambda: tracker,
                may_spawn_background_descendants=False,
            )
    finally:
        for signum, previous in before.items():
            real_signal(signum, previous)

    assert caught.value.code == 128 + signal.SIGTERM
    assert replayed == [signal.SIGTERM]
    cleanup_group = getattr(caught.value, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert any("close boom" in str(error) for error in cleanup_group.exceptions)
    assert {signum: signal.getsignal(signum) for signum in before} == before


def test_kernel_tracker_close_failure_restores_signal_handlers(tmp_path: Path) -> None:
    tracker = _CloseFailingKernelTracker()
    before = {signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)}

    with pytest.raises(contained.ContainedProcessError, match="close boom"):
        contained.run_contained(
            [sys.executable, "-c", "pass"],
            cwd=tmp_path,
            deadline_monotonic=time.monotonic() + 2,
            kernel_tracker_factory=lambda: tracker,
            may_spawn_background_descendants=False,
        )

    assert {signum: signal.getsignal(signum) for signum in before} == before


def test_execution_timeout_remains_primary_when_tracker_close_fails(tmp_path: Path) -> None:
    tracker = _CloseFailingKernelTracker()

    with pytest.raises(subprocess.TimeoutExpired) as caught:
        contained.run_contained(
            [sys.executable, "-c", "import time; time.sleep(5)"],
            cwd=tmp_path,
            deadline_monotonic=time.monotonic() + 0.5,
            kernel_tracker_factory=lambda: tracker,
            may_spawn_background_descendants=False,
        )

    cleanup_group = getattr(caught.value, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert any("close boom" in str(error) for error in cleanup_group.exceptions)


def test_execution_timeout_retains_structured_pidfd_close_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner = contained._LinuxSubreaperProcessTracker()
    read_fd, write_fd = contained.os.pipe()
    owner._pidfds[101] = read_fd
    real_close = contained.os.close
    failures: list[OSError] = []

    class OwnedFdTracker(_CloseFailingKernelTracker):
        def close(self) -> None:
            owner.close()

    def fail_owned_descriptor(descriptor: int) -> None:
        if descriptor == read_fd:
            failure = OSError(
                contained.errno.EIO,
                f"persistent outer pidfd close failure {len(failures) + 1}",
            )
            failures.append(failure)
            raise failure
        real_close(descriptor)

    monkeypatch.setattr(contained.os, "close", fail_owned_descriptor)
    try:
        with pytest.raises(subprocess.TimeoutExpired) as caught:
            contained.run_contained(
                [sys.executable, "-c", "import time; time.sleep(5)"],
                cwd=tmp_path,
                deadline_monotonic=time.monotonic() + 0.5,
                kernel_tracker_factory=OwnedFdTracker,
                may_spawn_background_descendants=False,
            )
        cleanup_group = getattr(caught.value, "cleanup_error_group", None)
        assert isinstance(cleanup_group, BaseExceptionGroup)
        tracker_error = next(
            error
            for error in cleanup_group.exceptions
            if isinstance(error, contained.ContainedProcessError) and "tracker" in str(error)
        )
        tracker_cleanup = getattr(tracker_error, "cleanup_error_group", None)
        assert isinstance(tracker_cleanup, BaseExceptionGroup)
        assert tracker_cleanup.exceptions[: len(failures)] == tuple(failures)
        assert len(failures) == contained._SIGNAL_STATE_ATTEMPTS
        assert owner._pidfds == {101: read_fd}
        contained.os.fstat(read_fd)
    finally:
        monkeypatch.setattr(contained.os, "close", real_close)
        if owner._pidfds:
            owner.close()
        _close_test_fd_if_open(read_fd)
        real_close(write_fd)


def test_primary_exception_object_is_preserved_when_cleanup_also_fails(tmp_path: Path) -> None:
    tracker = _CloseFailingKernelTracker()
    primary = RuntimeError("primary execution failure")
    real_inventory = contained.process_inventory
    calls = 0

    def failing_inventory(deadline: float) -> dict[int, contained._ProcessObservation]:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise primary
        return real_inventory(deadline)

    with pytest.raises(RuntimeError) as caught:
        contained.run_contained(
            [sys.executable, "-c", "pass"],
            cwd=tmp_path,
            deadline_monotonic=time.monotonic() + 2,
            inventory_provider=failing_inventory,
            kernel_tracker_factory=lambda: tracker,
            may_spawn_background_descendants=False,
        )

    assert caught.value is primary
    cleanup_group = getattr(caught.value, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert any("close boom" in str(error) for error in cleanup_group.exceptions)


def test_nested_run_restores_outer_then_original_signal_handlers(tmp_path: Path) -> None:
    before = {signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)}
    cancellation_checks = 0
    inner_handlers: dict[int, object] = {}

    def cancellation_check() -> bool:
        nonlocal cancellation_checks
        cancellation_checks += 1
        if cancellation_checks != 2:
            return False
        outer_handlers = {
            signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)
        }
        inner_handlers.update(outer_handlers)
        contained.run_contained(
            [sys.executable, "-c", "pass"],
            cwd=tmp_path,
            deadline_monotonic=time.monotonic() + 2,
            may_spawn_background_descendants=False,
        )
        assert {signum: signal.getsignal(signum) for signum in outer_handlers} == outer_handlers
        return False

    contained.run_contained(
        [sys.executable, "-c", "pass"],
        cwd=tmp_path,
        deadline_monotonic=time.monotonic() + 3,
        cancellation_check=cancellation_check,
        may_spawn_background_descendants=False,
    )

    assert inner_handlers
    assert {signum: signal.getsignal(signum) for signum in before} == before


def _linux_subreaper_state() -> int:
    libc = ctypes.CDLL(None, use_errno=True)
    current = ctypes.c_int()
    if (
        libc.prctl(
            contained._LinuxSubreaperProcessTracker._PR_GET_CHILD_SUBREAPER,
            ctypes.byref(current),
            0,
            0,
            0,
        )
        != 0
    ):
        raise OSError(ctypes.get_errno(), "could not read Linux child subreaper state")
    return int(current.value)


def _contained_signal_handlers() -> dict[int, object]:
    return {signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)}


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux containment lifecycle")
def test_linux_run_contained_rejects_non_main_thread_before_spawn(tmp_path: Path) -> None:
    marker = tmp_path / "non-main-thread-spawned"
    failures: list[BaseException] = []

    def invoke() -> None:
        try:
            contained.run_contained(
                [sys.executable, "-c", f"from pathlib import Path; Path({str(marker)!r}).touch()"],
                cwd=tmp_path,
                deadline_monotonic=time.monotonic() + 2,
                may_spawn_background_descendants=False,
            )
        except BaseException as exc:
            failures.append(exc)

    worker = threading.Thread(target=invoke)
    worker.start()
    worker.join(timeout=3)

    assert not worker.is_alive()
    assert len(failures) == 1
    assert isinstance(failures[0], contained.ContainedProcessError)
    assert "main thread" in str(failures[0])
    assert not marker.exists()


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux containment lifecycle")
def test_linux_run_contained_two_level_inner_failure_restores_boundary(tmp_path: Path) -> None:
    before_subreaper = _linux_subreaper_state()
    before_handlers = _contained_signal_handlers()
    entered = False

    def invoke_inner() -> bool:
        nonlocal entered
        if entered:
            return False
        entered = True
        with pytest.raises(subprocess.CalledProcessError):
            contained.run_contained(
                [sys.executable, "-c", "import sys; sys.exit(23)"],
                cwd=tmp_path,
                deadline_monotonic=time.monotonic() + 3,
                check=True,
                may_spawn_background_descendants=False,
            )
        return False

    completed = contained.run_contained(
        [sys.executable, "-c", "pass"],
        cwd=tmp_path,
        deadline_monotonic=time.monotonic() + 5,
        cancellation_check=invoke_inner,
        check=True,
        may_spawn_background_descendants=False,
    )

    assert completed.returncode == 0
    assert entered
    assert _linux_subreaper_state() == before_subreaper
    assert _contained_signal_handlers() == before_handlers


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux containment lifecycle")
def test_linux_run_contained_three_level_outer_failure_restores_boundary(tmp_path: Path) -> None:
    before_subreaper = _linux_subreaper_state()
    before_handlers = _contained_signal_handlers()
    entered_middle = False
    entered_inner = False

    def invoke_inner() -> bool:
        nonlocal entered_inner
        if entered_inner:
            return False
        entered_inner = True
        completed = contained.run_contained(
            [sys.executable, "-c", "pass"],
            cwd=tmp_path,
            deadline_monotonic=time.monotonic() + 2,
            check=True,
            may_spawn_background_descendants=False,
        )
        assert completed.returncode == 0
        raise RuntimeError("middle containment failure")

    def invoke_middle() -> bool:
        nonlocal entered_middle
        if entered_middle:
            return False
        entered_middle = True
        with pytest.raises(RuntimeError, match="middle containment failure"):
            contained.run_contained(
                [sys.executable, "-c", "pass"],
                cwd=tmp_path,
                deadline_monotonic=time.monotonic() + 3,
                cancellation_check=invoke_inner,
                check=True,
                may_spawn_background_descendants=False,
            )
        raise RuntimeError("outer containment failure")

    with pytest.raises(RuntimeError, match="outer containment failure"):
        contained.run_contained(
            [sys.executable, "-c", "pass"],
            cwd=tmp_path,
            deadline_monotonic=time.monotonic() + 5,
            cancellation_check=invoke_middle,
            check=True,
            may_spawn_background_descendants=False,
        )

    assert entered_middle
    assert entered_inner
    assert _linux_subreaper_state() == before_subreaper
    assert _contained_signal_handlers() == before_handlers


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux containment lifecycle")
def test_linux_nested_inner_timeout_preserves_outer_cleanup_boundary(tmp_path: Path) -> None:
    before_subreaper = _linux_subreaper_state()
    before_handlers = _contained_signal_handlers()
    entered = False

    def invoke_inner() -> bool:
        nonlocal entered
        if entered:
            return False
        entered = True
        with pytest.raises(subprocess.TimeoutExpired):
            contained.run_contained(
                [sys.executable, "-c", "import time; time.sleep(10)"],
                cwd=tmp_path,
                deadline_monotonic=time.monotonic() + 0.6,
                may_spawn_background_descendants=False,
            )
        return False

    completed = contained.run_contained(
        [sys.executable, "-c", "pass"],
        cwd=tmp_path,
        deadline_monotonic=time.monotonic() + 5,
        cancellation_check=invoke_inner,
        check=True,
        may_spawn_background_descendants=False,
    )

    assert completed.returncode == 0
    assert entered
    assert _linux_subreaper_state() == before_subreaper
    assert _contained_signal_handlers() == before_handlers


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux containment lifecycle")
def test_linux_nested_detached_child_cleanup_preserves_outer_boundary(tmp_path: Path) -> None:
    marker = tmp_path / "detached-child-marker"
    before_subreaper = _linux_subreaper_state()
    before_handlers = _contained_signal_handlers()
    entered = False
    detached = (
        "import subprocess,sys,time; "
        "subprocess.Popen([sys.executable,'-c',"
        "\"import pathlib,sys,time;time.sleep(.3);pathlib.Path(sys.argv[1]).write_text('x')\","
        "sys.argv[1]],start_new_session=True);time.sleep(.05)"
    )

    def invoke_inner() -> bool:
        nonlocal entered
        if entered:
            return False
        entered = True
        with pytest.raises(contained.ContainedProcessError):
            contained.run_contained(
                [sys.executable, "-c", detached, str(marker)],
                cwd=tmp_path,
                deadline_monotonic=time.monotonic() + 3,
                check=True,
                may_spawn_background_descendants=True,
            )
        return False

    completed = contained.run_contained(
        [sys.executable, "-c", "pass"],
        cwd=tmp_path,
        deadline_monotonic=time.monotonic() + 5,
        cancellation_check=invoke_inner,
        check=True,
        may_spawn_background_descendants=False,
    )

    time.sleep(0.5)
    assert completed.returncode == 0
    assert entered
    assert not marker.exists()
    assert _linux_subreaper_state() == before_subreaper
    assert _contained_signal_handlers() == before_handlers


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux subreaper concurrency")
def test_linux_real_subreaper_tracker_serializes_two_threads() -> None:
    before_subreaper = _linux_subreaper_state()
    first_enabled = threading.Event()
    release_first = threading.Event()
    second_enabled = threading.Event()
    failures: list[BaseException] = []

    def first() -> None:
        tracker = contained._LinuxSubreaperProcessTracker()
        try:
            tracker._enable_subreaper(time.monotonic() + 3)
            first_enabled.set()
            assert release_first.wait(timeout=2)
        except BaseException as exc:
            failures.append(exc)
        finally:
            tracker.close()

    def second() -> None:
        tracker = contained._LinuxSubreaperProcessTracker()
        try:
            assert first_enabled.wait(timeout=2)
            tracker._enable_subreaper(time.monotonic() + 3)
            second_enabled.set()
        except BaseException as exc:
            failures.append(exc)
        finally:
            tracker.close()

    first_thread = threading.Thread(target=first)
    second_thread = threading.Thread(target=second)
    first_thread.start()
    second_thread.start()
    assert first_enabled.wait(timeout=2)
    assert not second_enabled.wait(timeout=0.1)
    release_first.set()
    first_thread.join(timeout=4)
    second_thread.join(timeout=4)

    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    assert failures == []
    assert _linux_subreaper_state() == before_subreaper


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux subreaper lifecycle")
def test_linux_real_subreaper_foreign_close_then_owner_recovery() -> None:
    before_subreaper = _linux_subreaper_state()
    before_handlers = _contained_signal_handlers()
    tracker = contained._LinuxSubreaperProcessTracker()
    foreign_started = threading.Event()
    foreign_finished = threading.Event()
    foreign_errors: list[BaseException] = []

    tracker._enable_subreaper(time.monotonic() + 3)

    def foreign_close() -> None:
        foreign_started.set()
        try:
            tracker.close()
        except BaseException as exc:
            foreign_errors.append(exc)
        finally:
            foreign_finished.set()

    worker = threading.Thread(target=foreign_close)
    worker.start()
    assert foreign_started.wait(timeout=2)
    assert foreign_finished.wait(timeout=2)
    worker.join(timeout=2)
    assert not worker.is_alive()
    assert len(foreign_errors) == 1
    assert isinstance(foreign_errors[0], contained.ContainedProcessError)
    assert _linux_subreaper_state() == 1

    tracker.close()
    successor = contained._LinuxSubreaperProcessTracker()
    successor._enable_subreaper(time.monotonic() + 3)
    successor.close()

    assert _linux_subreaper_state() == before_subreaper
    assert _contained_signal_handlers() == before_handlers


def test_cleanup_permission_error_does_not_skip_root_reap(monkeypatch) -> None:
    class Process(_FinishedProcess):
        communicated = False

        def communicate(self, *, timeout: float) -> tuple[str, str]:
            self.communicated = True
            return super().communicate(timeout=timeout)

    process = Process()
    inventories = iter(
        (
            {100: _observation(100, 1, 1)},
            {100: _observation(100, 1, 1)},
            {100: _observation(100, 1, 1)},
            {100: _observation(100, 1, 1)},
            {},
        )
    )

    def deny_group(_pid: int, _signum: int) -> None:
        raise PermissionError("denied")

    monkeypatch.setattr(contained.os, "killpg", deny_group)
    monkeypatch.setattr(contained.os, "kill", lambda _pid, _signum: None)

    with pytest.raises(contained.ContainedProcessError, match="process group"):
        contained._cleanup_process_tree(
            process,  # type: ignore[arg-type]
            {},
            root_identity=contained.ProcessIdentity(100, (1, 0)),
            deadline=10,
            inventory_provider=lambda _deadline: next(inventories),
            clock=lambda: 1,
            sleep=lambda _seconds: None,
        )

    assert process.communicated


def test_blocked_user_tracker_does_not_prevent_root_reap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_inventory = contained.process_inventory
    real_popen = contained.subprocess.Popen
    tracker_entered = threading.Event()
    release_tracker = threading.Event()
    spawned: list[subprocess.Popen[str]] = []

    def blocking_inventory(
        deadline: float,
        **kwargs: object,
    ) -> dict[int, contained._ProcessObservation]:
        if threading.current_thread().name.startswith("rquant-containment-"):
            tracker_entered.set()
            release_tracker.wait(timeout=2)
            return {}
        return real_inventory(deadline, **kwargs)  # type: ignore[arg-type]

    def capturing_popen(*args: object, **kwargs: object) -> subprocess.Popen[str]:
        process = real_popen(*args, **kwargs)
        spawned.append(process)
        return process

    monkeypatch.setattr(contained, "process_inventory", blocking_inventory)
    monkeypatch.setattr(contained.subprocess, "Popen", capturing_popen)
    try:
        with pytest.raises(subprocess.TimeoutExpired) as caught:
            contained.run_contained(
                [sys.executable, "-c", "import time; time.sleep(10)"],
                cwd=tmp_path,
                deadline_monotonic=time.monotonic() + 0.5,
                inventory_provider=blocking_inventory,
                may_spawn_background_descendants=False,
            )
        cleanup_group = getattr(caught.value, "cleanup_error_group", None)
        assert isinstance(cleanup_group, BaseExceptionGroup)
        assert any("tracker did not stop" in str(error) for error in cleanup_group.exceptions)
        assert tracker_entered.is_set()
        assert spawned and spawned[0].returncode is not None
    finally:
        release_tracker.set()


@pytest.mark.skipif(sys.platform != "darwin", reason="Darwin capability gate")
def test_darwin_background_capable_command_is_rejected_before_spawn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spawned = False

    def forbidden_spawn(*_args: object, **_kwargs: object) -> object:
        nonlocal spawned
        spawned = True
        raise AssertionError("background-capable command must not start")

    monkeypatch.setattr(contained.subprocess, "Popen", forbidden_spawn)

    with pytest.raises(contained.ContainedProcessError, match="Darwin.*background"):
        contained.run_contained(
            [sys.executable, "-c", "import os; os.setsid()"],
            cwd=tmp_path,
            deadline_monotonic=time.monotonic() + 1,
            may_spawn_background_descendants=True,
        )

    assert not spawned


@pytest.mark.skipif(sys.platform != "darwin", reason="Darwin capability gate")
def test_darwin_native_detacher_is_refused_before_root_can_fork(tmp_path: Path) -> None:
    compiler = shutil.which("cc")
    if compiler is None:
        pytest.skip("native compiler is unavailable")
    source = tmp_path / "detach.c"
    executable = tmp_path / "detach"
    marker = tmp_path / "escaped"
    source.write_text(
        """
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
int main(int argc, char **argv) {
    pid_t child = fork();
    if (child == 0) {
        if (fork() == 0) {
            setsid();
            unsetenv("RQUANT_CONTAINMENT_TOKEN");
            for (int fd = 0; fd < 1024; fd++) close(fd);
            usleep(200000);
            int out = open(argv[1], O_CREAT | O_WRONLY, 0600);
            if (out >= 0) close(out);
        }
        _exit(0);
    }
    _exit(argc < 2);
}
""",
        encoding="ascii",
    )
    subprocess.run([compiler, str(source), "-o", str(executable)], check=True)

    with pytest.raises(contained.ContainedProcessError, match="startup refused"):
        contained.run_contained(
            [str(executable), str(marker)],
            cwd=tmp_path,
            deadline_monotonic=time.monotonic() + 1,
            may_spawn_background_descendants=True,
        )

    time.sleep(0.25)
    assert not marker.exists()


def test_short_command_budget_reserves_three_quarters_for_containment_cleanup() -> None:
    assert contained._cleanup_reserve_seconds(0.2) == pytest.approx(0.15)
    assert contained._cleanup_reserve_seconds(0.6) == pytest.approx(0.3)
    assert contained._cleanup_reserve_seconds(2.0) == pytest.approx(1.0)


def test_successful_root_with_live_detached_descendant_fails_closed(
    tmp_path,
) -> None:
    marker = tmp_path / "late"
    child = (
        "import subprocess,sys,time; "
        "subprocess.Popen([sys.executable,'-c',"
        "\"import pathlib,sys,time;time.sleep(.25);pathlib.Path(sys.argv[1]).write_text('x')\","
        "sys.argv[1]],start_new_session=True);time.sleep(.05)"
    )

    try:
        contained.run_contained(
            [sys.executable, "-c", child, str(marker)],
            cwd=tmp_path,
            deadline_monotonic=contained.time.monotonic() + 1,
            check=True,
            may_spawn_background_descendants=True,
        )
    except contained.ContainedProcessError:
        pass
    else:
        raise AssertionError("detached descendant was accepted as a successful command")

    contained.time.sleep(0.35)
    assert not marker.exists()


def test_immediate_cancellation_happens_after_kernel_registration_but_before_gate(
    tmp_path: Path,
) -> None:
    marker = tmp_path / "gate-opened"
    tracker = _FakeKernelTracker(identity=contained.ProcessIdentity(1, (1, 0)))

    def cancel() -> bool:
        return True

    with pytest.raises(contained.ContainedProcessError):
        contained.run_contained(
            [sys.executable, "-c", f"from pathlib import Path; Path({str(marker)!r}).touch()"],
            cwd=tmp_path,
            deadline_monotonic=time.monotonic() + 1,
            inventory_provider=lambda _deadline: {},
            cancellation_check=cancel,
            kernel_tracker_factory=_tracker_factory(tracker),
            may_spawn_background_descendants=False,
        )

    time.sleep(0.05)
    assert not marker.exists()
    assert tracker.closed


def test_kernel_registration_failure_keeps_startup_gate_closed(tmp_path: Path) -> None:
    marker = tmp_path / "gate-opened"
    tracker = _FakeKernelTracker()

    with pytest.raises(contained.ContainedProcessError, match="registration"):
        contained.run_contained(
            [sys.executable, "-c", f"from pathlib import Path; Path({str(marker)!r}).touch()"],
            cwd=tmp_path,
            deadline_monotonic=time.monotonic() + 1,
            inventory_provider=lambda _deadline: {},
            kernel_tracker_factory=_tracker_factory(tracker),
            may_spawn_background_descendants=False,
        )

    time.sleep(0.05)
    assert not marker.exists()
    assert tracker.closed


def test_empty_startup_inventory_keeps_gate_closed_after_kernel_registration(
    tmp_path: Path,
) -> None:
    marker = tmp_path / "gate-opened"
    tracker = _FakeKernelTracker(identity=contained.ProcessIdentity(1, (1, 0)))

    with pytest.raises(contained.ContainedProcessError, match="root identity"):
        contained.run_contained(
            [sys.executable, "-c", f"from pathlib import Path; Path({str(marker)!r}).touch()"],
            cwd=tmp_path,
            deadline_monotonic=time.monotonic() + 1,
            inventory_provider=lambda _deadline: {},
            kernel_tracker_factory=_tracker_factory(tracker),
            may_spawn_background_descendants=False,
        )

    time.sleep(0.05)
    assert not marker.exists()
    assert tracker.closed


def test_kernel_track_error_fails_closed_and_stops_root(tmp_path: Path) -> None:
    marker = tmp_path / "late"
    tracker = _FakeKernelTracker(
        identity=contained.ProcessIdentity(1, (1, 0)),
        poll_error=contained.ContainedProcessError("NOTE_TRACKERR"),
    )

    with pytest.raises(contained.ContainedProcessError, match="NOTE_TRACKERR"):
        contained.run_contained(
            [
                sys.executable,
                "-c",
                "import time; time.sleep(.1); from pathlib import Path; "
                f"Path({str(marker)!r}).touch()",
            ],
            cwd=tmp_path,
            deadline_monotonic=time.monotonic() + 1,
            inventory_provider=lambda _deadline: {},
            kernel_tracker_factory=_tracker_factory(tracker),
            may_spawn_background_descendants=False,
        )

    time.sleep(0.15)
    assert not marker.exists()
    assert tracker.closed


def _contained_run_baseline_seconds(cwd: Path) -> float:
    """Measure one trivial contained run so budgets can track the host.

    `run_contained` reports a deadline that expires as `TimeoutExpired` and a
    containment violation as `ContainedProcessError`. A case that wants the
    second outcome has to give detection more room than the run itself needs,
    and "more room" is a property of the machine: a fixed 0.6s let the deadline
    win on a shared x64 runner and turned a containment assertion into a
    timeout.
    """
    started = time.monotonic()
    contained.run_contained(
        [sys.executable, "-c", "pass"],
        cwd=cwd,
        deadline_monotonic=time.monotonic() + 120,
        may_spawn_background_descendants=False,
    )
    return time.monotonic() - started


def test_immediate_setsid_descendant_never_escapes_over_repeated_trials(
    tmp_path: Path,
) -> None:
    child = (
        "import os,subprocess,sys;"
        "from pathlib import Path;Path(sys.argv[2]).touch();"
        "os.environ.pop('RQUANT_CONTAINMENT_TOKEN',None);"
        "subprocess.Popen([sys.executable,'-c',"
        "\"import pathlib,sys,time;time.sleep(.08);pathlib.Path(sys.argv[1]).write_text('x')\","
        "sys.argv[1]],start_new_session=True);os._exit(0)"
    )
    markers: list[Path] = []

    # The trial starts a CPython that spawns a second one and exits; containment
    # then has to notice the orphan and kill it. Budget that as a multiple of
    # what one contained run costs here, plus the descendant's own 0.08s sleep,
    # so the assertion stays "containment caught it" on every machine instead of
    # decaying into "the deadline expired first".
    per_trial_budget = max(0.6, 6 * _contained_run_baseline_seconds(tmp_path) + 0.3)

    trials = 1 if sys.platform == "darwin" else 25
    for trial in range(trials):
        marker = tmp_path / f"escaped-{trial}"
        started = tmp_path / f"started-{trial}"
        markers.append(marker)
        with pytest.raises(contained.ContainedProcessError):
            contained.run_contained(
                [sys.executable, "-c", child, str(marker), str(started)],
                cwd=tmp_path,
                deadline_monotonic=time.monotonic() + per_trial_budget,
                may_spawn_background_descendants=True,
            )
        assert started.exists() is (sys.platform != "darwin")

    time.sleep(0.15)
    assert not any(marker.exists() for marker in markers)


@pytest.mark.skipif(sys.platform != "darwin", reason="Darwin pipe identity contract")
def test_darwin_pipe_marker_survives_missing_intermediate_and_reparent(
    tmp_path: Path,
) -> None:
    read_fd, write_fd = contained.os.pipe()
    root: contained.subprocess.Popen[str] | None = None
    grandchild_pid: int | None = None
    pid_file = tmp_path / "grandchild.pid"
    marker = contained._darwin_pipe_marker_for_fd(contained.os.getpid(), read_fd)
    grandchild = "import time;time.sleep(10)"
    intermediate = (
        "import os,subprocess,sys;"
        "os.environ.pop('RQUANT_CONTAINMENT_TOKEN',None);"
        "p=subprocess.Popen([sys.executable,'-c',sys.argv[2]],start_new_session=True);"
        "open(sys.argv[1],'w').write(str(p.pid))"
    )
    root_code = (
        "import subprocess,sys,time;"
        "p=subprocess.Popen([sys.executable,'-c',sys.argv[2],sys.argv[1],sys.argv[3]]);"
        "p.wait();time.sleep(10)"
    )
    try:
        root = contained.subprocess.Popen(
            [sys.executable, "-c", root_code, str(pid_file), intermediate, grandchild],
            stdout=write_fd,
            stderr=write_fd,
            text=True,
            start_new_session=True,
        )
        contained.os.close(write_fd)
        write_fd = -1
        deadline = time.monotonic() + 3
        while not pid_file.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert pid_file.exists()
        grandchild_pid = int(pid_file.read_text(encoding="ascii"))
        while time.monotonic() < deadline:
            observation = contained._darwin_process_observation(grandchild_pid)
            if observation is not None and observation.parent_pid != root.pid:
                break
            time.sleep(0.01)
        else:
            raise AssertionError("grandchild was not reparented after intermediate exit")

        assert contained._darwin_process_has_pipe_marker(
            grandchild_pid,
            frozenset({marker}),
            deadline=deadline,
        )
    finally:
        if grandchild_pid is not None:
            with contained.suppress(ProcessLookupError):
                contained.os.kill(grandchild_pid, signal.SIGKILL)
        if root is not None:
            with contained.suppress(ProcessLookupError):
                contained.os.killpg(root.pid, signal.SIGKILL)
            with contained.suppress(contained.subprocess.TimeoutExpired):
                root.communicate(timeout=1)
        if write_fd >= 0:
            contained.os.close(write_fd)
        contained.os.close(read_fd)


class _BlockingClosedKqueue:
    def __init__(self) -> None:
        self.entered = threading.Event()
        self.in_control = threading.Event()
        self.closed_while_active = False

    def control(self, _changes, _max_events: int, _timeout: float):
        self.entered.set()
        self.in_control.set()
        time.sleep(0.03)
        self.in_control.clear()
        return []

    def close(self) -> None:
        self.closed_while_active = self.in_control.is_set()


@pytest.mark.skipif(sys.platform != "darwin", reason="Darwin kqueue shutdown contract")
def test_darwin_tracker_close_joins_before_closing_live_kqueue() -> None:
    tracker = contained._DarwinKqueueProcessTracker()
    tracker._initialize_queue()
    assert tracker._queue is not None
    tracker._queue.close()
    queue = _BlockingClosedKqueue()
    tracker._queue = queue  # type: ignore[assignment]
    tracker._deadline = time.monotonic() + 2
    tracker._thread = threading.Thread(target=tracker._track)
    tracker._thread.start()
    assert queue.entered.wait(timeout=1)

    tracker.close()

    assert tracker._thread is None
    assert tracker._error is None
    assert not queue.closed_while_active


@pytest.mark.skipif(sys.platform != "darwin", reason="Darwin kqueue shutdown contract")
def test_darwin_tracker_retains_queue_after_persistent_close_failure() -> None:
    tracker = contained._DarwinKqueueProcessTracker()
    tracker._initialize_queue()
    assert tracker._queue is not None
    tracker._queue.close()
    read_fd, write_fd = contained.os.pipe()
    real_close = contained.os.close

    class PersistentCloseQueue:
        def __init__(self) -> None:
            self.persistent = True
            self.failures: list[OSError] = []

        def fileno(self) -> int:
            return read_fd

        def close(self) -> None:
            if self.persistent:
                failure = OSError(
                    contained.errno.EIO,
                    f"kqueue close failure {len(self.failures) + 1}",
                )
                self.failures.append(failure)
                raise failure
            real_close(read_fd)

    queue = PersistentCloseQueue()
    tracker._queue = queue  # type: ignore[assignment]
    try:
        with pytest.raises(contained.ContainedProcessError) as caught:
            tracker.close()
        cleanup_group = getattr(caught.value, "cleanup_error_group", None)
        assert isinstance(cleanup_group, BaseExceptionGroup)
        assert cleanup_group.exceptions[: len(queue.failures)] == tuple(queue.failures)
        assert len(queue.failures) == contained._SIGNAL_STATE_ATTEMPTS
        assert tracker._owns_queue
        contained.os.fstat(read_fd)

        queue.persistent = False
        tracker.close()
        assert not tracker._owns_queue
        with pytest.raises(OSError) as closed:
            contained.os.fstat(read_fd)
        assert closed.value.errno == contained.errno.EBADF
    finally:
        queue.persistent = False
        _close_test_fd_if_open(read_fd)
        real_close(write_fd)


@pytest.mark.skipif(sys.platform != "darwin", reason="Darwin kqueue tracking contract")
def test_darwin_tracker_keeps_pipe_marked_grandchild_after_parent_chain_breaks(
    monkeypatch,
) -> None:
    tracker = contained._DarwinKqueueProcessTracker()
    tracker._initialize_queue()
    assert tracker._queue is not None
    tracker._queue.close()
    root = contained.ProcessIdentity(100, (1, 0), kernel_unique_id=1000)
    grandchild = contained.ProcessIdentity(102, (3, 0), kernel_unique_id=1002)

    class _ForkThenStopQueue:
        calls = 0

        def control(self, _changes, _max_events: int, _timeout: float):
            self.calls += 1
            if self.calls == 1:
                return [SimpleNamespace(fflags=contained.select.KQ_NOTE_FORK)]
            tracker._stop.set()
            return []

        def close(self) -> None:
            return None

    tracker._queue = _ForkThenStopQueue()  # type: ignore[assignment]
    tracker._root_pid = root.pid
    tracker._root_started = root.started
    tracker._known[root.pid] = root
    tracker._deadline = time.monotonic() + 1
    inventory = {
        root.pid: contained._ProcessObservation(identity=root, parent_pid=1),
        grandchild.pid: contained._ProcessObservation(
            identity=grandchild,
            parent_pid=1,
            containment_token=True,
            parent_kernel_unique_id=9999,
        ),
    }
    monkeypatch.setattr(contained, "_darwin_process_inventory", lambda *_args, **_kwargs: inventory)
    registered: list[contained.ProcessIdentity] = []
    monkeypatch.setattr(
        tracker,
        "_register_process",
        lambda identity, *, deadline: registered.append(identity) or deadline > 0,
    )

    tracker._track()

    assert tracker._known[grandchild.pid] == grandchild
    assert registered == [grandchild]


@pytest.mark.skipif(sys.platform != "darwin", reason="Darwin pipe anchor contract")
def test_darwin_pipe_identity_remains_anchored_through_final_inventory(
    monkeypatch,
    tmp_path: Path,
) -> None:
    tracker = _FakeKernelTracker(identity=contained.ProcessIdentity(1, (1, 0)))
    marker_fds: list[int] = []

    def marker_for_fd(_pid: int, fd: int) -> contained.DarwinPipeMarker:
        contained.os.fstat(fd)
        marker_fds.append(fd)
        return (fd * 2 + 1, fd * 2 + 2)

    def inventory(_deadline: float) -> dict[int, contained._ProcessObservation]:
        for fd in marker_fds:
            contained.os.fstat(fd)
        identity = tracker.registered_identity
        if identity is None:
            return {}
        return {
            identity.pid: contained._ProcessObservation(
                identity=identity,
                parent_pid=contained.os.getpid(),
            )
        }

    monkeypatch.setattr(contained, "_darwin_pipe_marker_for_fd", marker_for_fd)

    result = contained.run_contained(
        [sys.executable, "-c", "pass"],
        cwd=tmp_path,
        deadline_monotonic=time.monotonic() + 2,
        inventory_provider=inventory,
        kernel_tracker_factory=_tracker_factory(tracker),
        may_spawn_background_descendants=False,
    )

    assert result.returncode == 0
    assert len(marker_fds) == 2
    for fd in marker_fds:
        with pytest.raises(OSError):
            contained.os.fstat(fd)


@pytest.mark.skipif(sys.platform != "darwin", reason="Darwin pipe anchor contract")
def test_darwin_anchor_dup_is_owned_before_inheritable_update(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    tracker = _FakeKernelTracker(identity=contained.ProcessIdentity(1, (1, 0)))
    real_dup = contained.os.dup
    real_set_inheritable = contained.os.set_inheritable
    real_close_descriptors = contained._close_file_descriptors
    duplicated: list[int] = []
    cleanup_inventories: list[
        tuple[tuple[int, ...], tuple[int, ...], bool, tuple[BaseException, ...]]
    ] = []
    failure = OSError("anchor inheritable update failed")

    def capture_real_dup(fd: int) -> int:
        duplicate = real_dup(fd)
        duplicated.append(duplicate)
        return duplicate

    def fail_anchor_inheritable(fd: int, inheritable: bool) -> None:
        if duplicated and fd == duplicated[-1]:
            contained.os.fstat(fd)
            raise failure
        real_set_inheritable(fd, inheritable)

    def capture_cleanup_inventory(
        descriptors: list[int],
        cleanup_errors: list[BaseException],
    ) -> bool:
        before = tuple(descriptors)
        closed = real_close_descriptors(descriptors, cleanup_errors)
        cleanup_inventories.append((before, tuple(descriptors), closed, tuple(cleanup_errors)))
        return closed

    monkeypatch.setattr(contained.os, "dup", capture_real_dup)
    monkeypatch.setattr(contained.os, "set_inheritable", fail_anchor_inheritable)
    monkeypatch.setattr(
        contained,
        "_close_file_descriptors",
        capture_cleanup_inventory,
    )
    try:
        with pytest.raises(OSError) as caught:
            contained.run_contained(
                [sys.executable, "-c", "pass"],
                cwd=tmp_path,
                deadline_monotonic=time.monotonic() + 2,
                inventory_provider=lambda _deadline: {},
                kernel_tracker_factory=_tracker_factory(tracker),
                may_spawn_background_descendants=False,
            )
        assert duplicated
        anchor = duplicated[0]
        with pytest.raises(OSError) as closed:
            contained.os.fstat(anchor)
        assert closed.value.errno == contained.errno.EBADF
    finally:
        for descriptor in duplicated:
            try:
                contained.os.fstat(descriptor)
            except OSError as exc:
                if exc.errno == contained.errno.EBADF:
                    continue
                raise
            contained.os.close(descriptor)

    assert caught.value is failure
    assert cleanup_inventories == [((anchor,), (), True, ())]
    assert getattr(caught.value, "cleanup_error_group", None) is None
    assert tracker.closed


def test_anchor_close_failure_retains_descriptor_inventory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    read_fd, write_fd = contained.os.pipe()
    real_close = contained.os.close
    descriptors = [read_fd]
    cleanup_errors: list[BaseException] = []
    failure = OSError(contained.errno.EIO, "anchor close boom")
    attempts = 0

    def fail_anchor_close(fd: int) -> None:
        nonlocal attempts
        if fd == read_fd:
            attempts += 1
            raise failure
        real_close(fd)

    monkeypatch.setattr(contained.os, "close", fail_anchor_close)
    try:
        closed = contained._close_file_descriptors(descriptors, cleanup_errors)

        assert not closed
        assert descriptors == [read_fd]
        assert cleanup_errors == [failure]
        assert attempts == contained._SIGNAL_STATE_ATTEMPTS
        contained.os.fstat(read_fd)
    finally:
        real_close(read_fd)
        real_close(write_fd)


def test_anchor_close_accepts_already_closed_descriptor() -> None:
    read_fd, write_fd = contained.os.pipe()
    contained.os.close(read_fd)
    descriptors = [read_fd]
    cleanup_errors: list[BaseException] = []
    try:
        assert contained._close_file_descriptors(descriptors, cleanup_errors)
        assert descriptors == []
        assert cleanup_errors == []
    finally:
        contained.os.close(write_fd)


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="atomic signal-mask arbitration requires pthread_sigmask",
)
def test_unclosed_anchor_defers_first_signal_replay_fail_closed() -> None:
    real_signal = contained.signal.signal
    real_sigmask = contained.signal.pthread_sigmask
    watched = (signal.SIGINT, signal.SIGTERM)
    before_handlers = {signum: signal.getsignal(signum) for signum in watched}
    before_mask = real_sigmask(signal.SIG_BLOCK, set())
    latch = contained._ContainedSignalLatch()
    replayed: list[int] = []
    close_failure = OSError(contained.errno.EIO, "persistent anchor close boom")

    def previous_handler(signum: int, _frame: object) -> None:
        replayed.append(signum)
        raise KeyboardInterrupt

    for signum in watched:
        real_signal(signum, previous_handler)
    previous_handlers, active_signals = contained._install_signal_latch(latch)
    latch.handle(signal.SIGTERM, None)
    try:
        with pytest.raises(contained._ContainedSignal) as caught:
            contained._finish_signal_restoration(
                previous_handlers,
                active_signals,
                latch,
                [close_failure],
                primary_exception=None,
                error_label="contained subprocess cleanup failures",
                replay_ready=False,
            )
        observed_mask = real_sigmask(signal.SIG_BLOCK, set())
        observed_handlers = {signum: signal.getsignal(signum) for signum in watched}
    finally:
        for signum, previous in before_handlers.items():
            real_signal(signum, previous)
        real_sigmask(signal.SIG_SETMASK, before_mask)

    assert caught.value.signum == signal.SIGTERM
    assert replayed == []
    assert set(active_signals) <= observed_mask
    assert observed_handlers == previous_handlers
    cleanup_group = getattr(caught.value, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert close_failure in cleanup_group.exceptions


def test_final_inventory_signal_replays_after_darwin_anchor_fds_close(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    tracker = _FakeKernelTracker(identity=contained.ProcessIdentity(1, (1, 0)))
    real_popen = contained.subprocess.Popen
    real_signal = contained.signal.signal
    previous_sigterm = signal.getsignal(signal.SIGTERM)
    marker_fds: list[int] = []
    communicated = False
    injected = False
    replayed = InterruptedError("final inventory signal replay")

    def previous_handler(_signum: int, _frame: object) -> None:
        for fd in marker_fds:
            with pytest.raises(OSError):
                contained.os.fstat(fd)
        raise replayed

    def marker_for_fd(_pid: int, fd: int) -> contained.DarwinPipeMarker:
        contained.os.fstat(fd)
        marker_fds.append(fd)
        return (fd * 2 + 1, fd * 2 + 2)

    def capturing_popen(*args: object, **kwargs: object) -> subprocess.Popen[str]:
        process = real_popen(*args, **kwargs)
        communicate = process.communicate

        def communicate_then_mark(*args: object, **kwargs: object) -> tuple[str, str]:
            nonlocal communicated
            result = communicate(*args, **kwargs)
            communicated = True
            return result

        process.communicate = communicate_then_mark  # type: ignore[method-assign]
        return process

    def inventory(_deadline: float) -> dict[int, contained._ProcessObservation]:
        nonlocal injected
        identity = tracker.registered_identity
        if communicated and not injected:
            injected = True
            handler = signal.getsignal(signal.SIGTERM)
            assert handler is not previous_handler
            assert callable(handler)
            handler(signal.SIGTERM, None)
        if identity is None:
            return {}
        return {
            identity.pid: contained._ProcessObservation(
                identity=identity,
                parent_pid=contained.os.getpid(),
            )
        }

    real_signal(signal.SIGTERM, previous_handler)
    monkeypatch.setattr(contained.sys, "platform", "darwin")
    monkeypatch.setattr(contained, "_darwin_pipe_marker_for_fd", marker_for_fd)
    monkeypatch.setattr(contained.subprocess, "Popen", capturing_popen)
    try:
        with pytest.raises(InterruptedError) as caught:
            contained.run_contained(
                [sys.executable, "-c", "pass"],
                cwd=tmp_path,
                deadline_monotonic=time.monotonic() + 2,
                inventory_provider=inventory,
                kernel_tracker_factory=_tracker_factory(tracker),
                may_spawn_background_descendants=False,
            )
    finally:
        real_signal(signal.SIGTERM, previous_sigterm)

    assert caught.value is replayed
    assert injected
    assert len(marker_fds) == 2
    for fd in marker_fds:
        with pytest.raises(OSError):
            contained.os.fstat(fd)


def test_final_inventory_signal_waits_for_retryable_darwin_anchor_close(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    tracker = _FakeKernelTracker(identity=contained.ProcessIdentity(1, (1, 0)))
    real_close = contained.os.close
    real_popen = contained.subprocess.Popen
    real_signal = contained.signal.signal
    previous_sigterm = signal.getsignal(signal.SIGTERM)
    marker_fds: list[int] = []
    communicated = False
    signal_injected = False
    close_injected = False
    close_failure = OSError(contained.errno.EIO, "transient anchor close boom")
    replayed = InterruptedError("final inventory signal replay")

    def previous_handler(_signum: int, _frame: object) -> None:
        for fd in marker_fds:
            with pytest.raises(OSError) as closed:
                contained.os.fstat(fd)
            assert closed.value.errno == contained.errno.EBADF
        raise replayed

    def marker_for_fd(_pid: int, fd: int) -> contained.DarwinPipeMarker:
        contained.os.fstat(fd)
        marker_fds.append(fd)
        return (fd * 2 + 1, fd * 2 + 2)

    def capturing_popen(*args: object, **kwargs: object) -> subprocess.Popen[str]:
        process = real_popen(*args, **kwargs)
        communicate = process.communicate

        def communicate_then_mark(*args: object, **kwargs: object) -> tuple[str, str]:
            nonlocal communicated
            result = communicate(*args, **kwargs)
            communicated = True
            return result

        process.communicate = communicate_then_mark  # type: ignore[method-assign]
        return process

    def inventory(_deadline: float) -> dict[int, contained._ProcessObservation]:
        nonlocal signal_injected
        identity = tracker.registered_identity
        if communicated and not signal_injected:
            signal_injected = True
            handler = signal.getsignal(signal.SIGTERM)
            assert handler is not previous_handler
            assert callable(handler)
            handler(signal.SIGTERM, None)
        if identity is None:
            return {}
        return {
            identity.pid: contained._ProcessObservation(
                identity=identity,
                parent_pid=contained.os.getpid(),
            )
        }

    def fail_first_anchor_close(fd: int) -> None:
        nonlocal close_injected
        if fd in marker_fds and not close_injected:
            close_injected = True
            raise close_failure
        real_close(fd)

    real_signal(signal.SIGTERM, previous_handler)
    monkeypatch.setattr(contained.sys, "platform", "darwin")
    monkeypatch.setattr(contained, "_darwin_pipe_marker_for_fd", marker_for_fd)
    monkeypatch.setattr(contained.subprocess, "Popen", capturing_popen)
    monkeypatch.setattr(contained.os, "close", fail_first_anchor_close)
    try:
        with pytest.raises(InterruptedError) as caught:
            contained.run_contained(
                [sys.executable, "-c", "pass"],
                cwd=tmp_path,
                deadline_monotonic=time.monotonic() + 2,
                inventory_provider=inventory,
                kernel_tracker_factory=_tracker_factory(tracker),
                may_spawn_background_descendants=False,
            )
    finally:
        real_signal(signal.SIGTERM, previous_sigterm)
        for fd in marker_fds:
            try:
                real_close(fd)
            except OSError as exc:
                assert exc.errno == contained.errno.EBADF

    assert caught.value is replayed
    assert signal_injected
    assert close_injected
    cleanup_group = getattr(caught.value, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert close_failure in cleanup_group.exceptions


def test_p15b_production_paths_use_only_shared_contained_subprocess() -> None:
    root = Path(__file__).resolve().parents[2]
    production_paths = (
        root / "scripts" / "bootstrap-lab-daemon.py",
        root / "scripts" / "bootstrap-production-deploy.py",
        root / "scripts" / "preflight-lab-runtime.py",
        root / "scripts" / "run-lab-daemon.py",
        # The mainline research manifest owns a lightweight Git probe. This
        # closure invariant applies to the daemon and release boundaries.
        root / "src" / "rquant" / "release_generation.py",
        root / "src" / "rquant" / "lab_launchd_install.py",
        root / "src" / "rquant" / "ops" / "production_deploy.py",
    )

    violations = {
        str(path.relative_to(root)): token
        for path in production_paths
        for token in ("subprocess.run(", "subprocess.Popen(")
        if token in path.read_text(encoding="utf-8")
    }

    assert violations == {}
