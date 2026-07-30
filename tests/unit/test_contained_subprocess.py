from __future__ import annotations

import dis
import errno
import fcntl
import inspect
import os
import select
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


class _ContainedReturnFault:
    def __init__(self, names: set[str], error: BaseException) -> None:
        self._names = names
        self._error = error
        self.triggered = False

    def trace(self, frame: object, event: str, _arg: object) -> object:
        code = getattr(frame, "f_code", None)
        if (
            not self.triggered
            and event == "return"
            and getattr(code, "co_filename", None) == contained.__file__
            and getattr(code, "co_name", None) in self._names
        ):
            self.triggered = True
            raise self._error
        return self.trace


class _ContainedCReturnFault:
    def __init__(self, target: object, error: BaseException) -> None:
        self._target = target
        self._error = error
        self.triggered = False

    def profile(self, frame: object, event: str, arg: object) -> None:
        if (
            not self.triggered
            and event == "c_return"
            and arg is self._target
            and getattr(getattr(frame, "f_code", None), "co_filename", None) == contained.__file__
        ):
            self.triggered = True
            raise self._error


class _ContainedPostCallOpcodeFault:
    def __init__(self, code: object, variable: str, error: BaseException) -> None:
        instructions = tuple(dis.get_instructions(code))
        self._code = code
        self._offset = next(
            current.offset
            for previous, current in zip(instructions, instructions[1:], strict=False)
            if previous.opname == "CALL"
            and current.opname == "STORE_FAST"
            and current.argval == variable
        )
        self._error = error
        self.triggered = False

    def trace(self, frame: object, event: str, _arg: object) -> object:
        if getattr(frame, "f_code", None) is self._code:
            if event == "call":
                frame.f_trace_opcodes = True  # type: ignore[attr-defined]
            elif event == "opcode" and getattr(frame, "f_lasti", None) == self._offset:
                self.triggered = True
                raise self._error
        return self.trace


def _install_acquisition_fault(
    kind: str,
    *,
    c_target: object,
    code: object,
    variable: str,
    error: BaseException,
) -> tuple[object, object]:
    if kind == "c_return":
        fault = _ContainedCReturnFault(c_target, error)
        hook = fault.profile
        sys.setprofile(hook)
        return fault, hook
    fault = _ContainedPostCallOpcodeFault(code, variable, error)
    hook = fault.trace
    sys.settrace(hook)
    return fault, hook


def _assert_acquisition_hook_restored(kind: str, hook: object) -> None:
    observed = sys.getprofile() if kind == "c_return" else sys.gettrace()
    assert observed is hook


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
    ((False, False), (True, False), (False, True), (True, True)),
    ids=("none", "trace", "profile", "both"),
)
def test_execution_hook_round_trip_restores_real_hook_identity(
    use_trace: bool,
    use_profile: bool,
) -> None:
    original_trace = sys.gettrace()
    original_profile = sys.getprofile()

    def trace_hook(_frame: object, _event: str, _arg: object) -> object:
        return trace_hook

    def profile_hook(_frame: object, _event: str, _arg: object) -> None:
        return None

    expected_trace = trace_hook if use_trace else None
    expected_profile = profile_hook if use_profile else None
    try:
        sys.settrace(expected_trace)
        sys.setprofile(expected_profile)

        outer_hooks = contained._suspend_execution_hooks()
        assert sys.gettrace() is None
        assert sys.getprofile() is None
        contained._finish_execution_hook_restoration(
            outer_hooks,
            primary_exception=None,
            error_label="test hook restoration failures",
        )

        assert sys.gettrace() is expected_trace
        assert sys.getprofile() is expected_profile
    finally:
        _restore_execution_hooks_for_test(original_trace, original_profile)


def test_nested_execution_hook_round_trip_restores_outer_real_hooks() -> None:
    original_trace = sys.gettrace()
    original_profile = sys.getprofile()

    def trace_hook(_frame: object, _event: str, _arg: object) -> object:
        return trace_hook

    def profile_hook(_frame: object, _event: str, _arg: object) -> None:
        return None

    try:
        sys.settrace(trace_hook)
        sys.setprofile(profile_hook)

        outer_hooks = contained._suspend_execution_hooks()
        inner_hooks = contained._suspend_execution_hooks()
        contained._finish_execution_hook_restoration(
            inner_hooks,
            primary_exception=None,
            error_label="inner test hook restoration failures",
        )
        assert sys.gettrace() is None
        assert sys.getprofile() is None

        contained._finish_execution_hook_restoration(
            outer_hooks,
            primary_exception=None,
            error_label="outer test hook restoration failures",
        )
        assert sys.gettrace() is trace_hook
        assert sys.getprofile() is profile_hook
    finally:
        _restore_execution_hooks_for_test(original_trace, original_profile)


@pytest.mark.skipif(not hasattr(select, "kqueue"), reason="real kqueue hook restoration contract")
def test_real_profile_callback_failure_does_not_skip_trace_restoration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_trace = sys.gettrace()
    original_profile = sys.getprofile()
    tracker = contained._DarwinKqueueProcessTracker()
    queue = select.kqueue()
    queue_fd = queue.fileno()
    callback_failure = RuntimeError("profile callback interrupted trace restoration")
    triggered = False

    def trace_hook(_frame: object, _event: str, _arg: object) -> object:
        return trace_hook

    def profile_hook(frame: object, event: str, _arg: object) -> None:
        nonlocal triggered
        if (
            not triggered
            and event == "call"
            and getattr(frame, "f_code", None) is contained._restore_execution_hook_bounded.__code__
            and getattr(frame, "f_locals", {}).get("label") == "trace"
        ):
            triggered = True
            raise callback_failure

    monkeypatch.setattr(contained.select, "kqueue", lambda: queue)
    try:
        sys.settrace(trace_hook)
        sys.setprofile(profile_hook)

        with pytest.raises(RuntimeError) as caught:
            tracker._initialize_queue()

        assert caught.value is callback_failure
        assert triggered
        assert sys.gettrace() is trace_hook
        assert sys.getprofile() is profile_hook
        _clear_execution_hooks()
        tracker.close()
        with pytest.raises(OSError) as closed:
            os.fstat(queue_fd)
        assert closed.value.errno == errno.EBADF
    finally:
        _clear_execution_hooks()
        with contained.suppress(BaseException):
            tracker.close()
        _close_test_fd_if_open(queue_fd)
        _restore_execution_hooks_for_test(original_trace, original_profile)


def test_real_profile_callback_failure_preserves_other_hook_and_cleanup() -> None:
    original_trace = sys.gettrace()
    original_profile = sys.getprofile()
    read_fd, write_fd = os.pipe()
    acquired_fd = -1
    primary = RuntimeError("existing acquisition primary")
    callback_failure = RuntimeError("profile callback persistently rejected verification")
    armed = False

    def trace_hook(_frame: object, _event: str, _arg: object) -> object:
        return trace_hook

    def profile_hook(frame: object, event: str, arg: object) -> None:
        if (
            armed
            and event == "c_call"
            and arg is sys.gettrace
            and getattr(getattr(frame, "f_code", None), "co_filename", None) == contained.__file__
        ):
            raise callback_failure

    try:
        sys.settrace(trace_hook)
        sys.setprofile(profile_hook)
        hooks = contained._suspend_execution_hooks()
        armed = True
        acquired_fd = os.dup(read_fd)

        with pytest.raises(RuntimeError) as caught:
            try:
                contained._finish_execution_hook_restoration(
                    hooks,
                    primary_exception=primary,
                    error_label="test acquisition hook restoration failures",
                )
            finally:
                os.close(acquired_fd)

        assert caught.value is primary
        assert sys.gettrace() is trace_hook
        assert sys.getprofile() is None
        cleanup_group = getattr(caught.value, "cleanup_error_group", None)
        assert isinstance(cleanup_group, BaseExceptionGroup)
        assert cleanup_group.exceptions[0] is callback_failure
        assert any("profile execution hook" in str(error) for error in cleanup_group.exceptions[1:])
        with pytest.raises(OSError) as closed:
            os.fstat(acquired_fd)
        assert closed.value.errno == errno.EBADF
    finally:
        _clear_execution_hooks()
        _close_test_fd_if_open(acquired_fd)
        _close_test_fd_if_open(read_fd)
        _close_test_fd_if_open(write_fd)
        _restore_execution_hooks_for_test(original_trace, original_profile)


@pytest.mark.skipif(not hasattr(select, "kqueue"), reason="real kqueue hook restoration contract")
def test_hook_error_recording_boundary_retries_both_real_hooks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_trace = sys.gettrace()
    original_profile = sys.getprofile()
    queue = select.kqueue()
    queue_fd = queue.fileno()
    tracker = contained._DarwinKqueueProcessTracker()
    getter_failure = OSError("profile getter boundary failed")
    recording_failure = RuntimeError("hook error recording boundary failed")
    getter_failed = False
    recording_failed = False

    counting_queue = _CountingQueue(queue_fd, queue.close)

    def trace_hook(frame: object, event: str, _arg: object) -> object:
        nonlocal recording_failed
        if (
            getter_failed
            and not recording_failed
            and event == "call"
            and getattr(frame, "f_code", None) is contained._record_cleanup_error.__code__
        ):
            recording_failed = True
            raise recording_failure
        return trace_hook

    def profile_hook(frame: object, event: str, arg: object) -> None:
        nonlocal getter_failed
        if (
            not getter_failed
            and event == "c_call"
            and arg is sys.getprofile
            and getattr(frame, "f_code", None) is contained._restore_execution_hooks.__code__
        ):
            getter_failed = True
            raise getter_failure

    monkeypatch.setattr(contained.select, "kqueue", lambda: counting_queue)
    try:
        sys.settrace(trace_hook)
        sys.setprofile(profile_hook)

        with pytest.raises(RuntimeError) as caught:
            tracker._initialize_queue()

        assert caught.value is recording_failure
        assert getter_failed
        assert recording_failed
        assert sys.gettrace() is trace_hook
        assert sys.getprofile() is profile_hook

        tracker.close()
        assert counting_queue.close_count == 1
        with pytest.raises(OSError) as closed:
            os.fstat(queue_fd)
        assert closed.value.errno == errno.EBADF
    finally:
        _clear_execution_hooks()
        with contained.suppress(BaseException):
            tracker.close()
        _close_test_fd_if_open(queue_fd)
        _restore_execution_hooks_for_test(original_trace, original_profile)


@pytest.mark.skipif(not hasattr(select, "kqueue"), reason="real kqueue hook restoration contract")
def test_hook_driver_return_boundary_gets_another_bounded_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_trace = sys.gettrace()
    original_profile = sys.getprofile()
    queue = select.kqueue()
    queue_fd = queue.fileno()
    tracker = contained._DarwinKqueueProcessTracker()
    boundary_failure = RuntimeError("hook driver return boundary failed")
    failed = False

    counting_queue = _CountingQueue(queue_fd, queue.close)

    def trace_hook(_frame: object, _event: str, _arg: object) -> object:
        return trace_hook

    def profile_hook(frame: object, event: str, _arg: object) -> None:
        nonlocal failed
        if (
            not failed
            and event == "return"
            and getattr(frame, "f_code", None) is contained._restore_execution_hooks.__code__
        ):
            failed = True
            raise boundary_failure

    monkeypatch.setattr(contained.select, "kqueue", lambda: counting_queue)
    try:
        sys.settrace(trace_hook)
        sys.setprofile(profile_hook)

        with pytest.raises(RuntimeError) as caught:
            tracker._initialize_queue()

        assert caught.value is boundary_failure
        assert failed
        assert sys.gettrace() is trace_hook
        assert sys.getprofile() is profile_hook

        tracker.close()
        assert counting_queue.close_count == 1
        with pytest.raises(OSError) as closed:
            os.fstat(queue_fd)
        assert closed.value.errno == errno.EBADF
    finally:
        _clear_execution_hooks()
        with contained.suppress(BaseException):
            tracker.close()
        _close_test_fd_if_open(queue_fd)
        _restore_execution_hooks_for_test(original_trace, original_profile)


@pytest.mark.skipif(not hasattr(select, "kqueue"), reason="real kqueue hook restoration contract")
@pytest.mark.parametrize("profile_event", ("c_call", "c_return"))
def test_hook_final_validation_boundary_gets_another_bounded_attempt(
    monkeypatch: pytest.MonkeyPatch,
    profile_event: str,
) -> None:
    original_trace = sys.gettrace()
    original_profile = sys.getprofile()
    queue = select.kqueue()
    queue_fd = queue.fileno()
    tracker = contained._DarwinKqueueProcessTracker()
    boundary_failure = RuntimeError("hook final validation boundary failed")
    failed = False

    counting_queue = _CountingQueue(queue_fd, queue.close)

    def trace_hook(_frame: object, _event: str, _arg: object) -> object:
        return trace_hook

    def profile_hook(frame: object, event: str, arg: object) -> None:
        nonlocal failed
        if (
            not failed
            and event == profile_event
            and arg is sys.getprofile
            and getattr(frame, "f_code", None)
            is contained._restore_execution_hooks_bounded.__code__
        ):
            failed = True
            raise boundary_failure

    monkeypatch.setattr(contained.select, "kqueue", lambda: counting_queue)
    try:
        sys.settrace(trace_hook)
        sys.setprofile(profile_hook)

        with pytest.raises(RuntimeError) as caught:
            tracker._initialize_queue()

        assert caught.value is boundary_failure
        assert failed
        assert sys.gettrace() is trace_hook
        assert sys.getprofile() is profile_hook

        tracker.close()
        assert counting_queue.close_count == 1
        with pytest.raises(OSError) as closed:
            os.fstat(queue_fd)
        assert closed.value.errno == errno.EBADF
    finally:
        _clear_execution_hooks()
        with contained.suppress(BaseException):
            tracker.close()
        _close_test_fd_if_open(queue_fd)
        _restore_execution_hooks_for_test(original_trace, original_profile)


@pytest.mark.skipif(not hasattr(select, "kqueue"), reason="real kqueue hook restoration contract")
@pytest.mark.parametrize("trace_event", ("call", "return", "opcode"))
def test_hook_error_capture_boundaries_preserve_both_exact_errors(
    monkeypatch: pytest.MonkeyPatch,
    trace_event: str,
) -> None:
    original_trace = sys.gettrace()
    original_profile = sys.getprofile()
    queue = select.kqueue()
    queue_fd = queue.fileno()
    tracker = contained._DarwinKqueueProcessTracker()
    getter_failure = OSError(f"profile getter before trace {trace_event}")
    trace_failure = RuntimeError(f"trace {trace_event} capture boundary failed")
    getter_failed = False
    trace_failed = False
    monitoring_tool_id: int | None = None

    counting_queue = _CountingQueue(queue_fd, queue.close)

    def trace_hook(frame: object, event: str, _arg: object) -> object:
        nonlocal trace_failed
        if (
            getter_failed
            and getattr(frame, "f_code", None) is contained._restore_execution_hook_bounded.__code__
            and frame.f_locals.get("label") == "trace"  # type: ignore[attr-defined]
            and not trace_failed
            and event == trace_event
        ):
            trace_failed = True
            raise trace_failure
        return trace_hook

    def instruction_hook(code: object, _offset: int) -> None:
        nonlocal trace_failed
        if (
            not trace_failed
            and code is contained._restore_execution_hook_bounded.__code__
            and sys._getframe(1).f_locals.get("label") == "trace"
        ):
            trace_failed = True
            raise trace_failure

    def profile_hook(frame: object, event: str, arg: object) -> None:
        nonlocal getter_failed
        if (
            not getter_failed
            and event == "c_call"
            and arg is sys.getprofile
            and getattr(frame, "f_code", None) is contained._restore_execution_hooks.__code__
        ):
            getter_failed = True
            raise getter_failure

    monkeypatch.setattr(contained.select, "kqueue", lambda: counting_queue)
    try:
        if trace_event == "opcode":
            monitoring_tool_id = sys.monitoring.OPTIMIZER_ID
            sys.monitoring.use_tool_id(monitoring_tool_id, "rquant hook restoration test")
            sys.monitoring.register_callback(
                monitoring_tool_id,
                sys.monitoring.events.INSTRUCTION,
                instruction_hook,
            )
            sys.monitoring.set_local_events(
                monitoring_tool_id,
                contained._restore_execution_hook_bounded.__code__,
                sys.monitoring.events.INSTRUCTION,
            )
        sys.settrace(trace_hook)
        sys.setprofile(profile_hook)

        with pytest.raises((OSError, RuntimeError)) as caught:
            tracker._initialize_queue()

        cleanup_group = getattr(caught.value, "cleanup_error_group", None)
        observed = (caught.value,)
        if isinstance(cleanup_group, BaseExceptionGroup):
            observed = (*observed, *cleanup_group.exceptions)
        assert any(error is getter_failure for error in observed)
        assert any(error is trace_failure for error in observed)
        assert getter_failed
        assert trace_failed
        assert sys.gettrace() is trace_hook
        assert sys.getprofile() is profile_hook

        tracker.close()
        assert counting_queue.close_count == 1
        with pytest.raises(OSError) as closed:
            os.fstat(queue_fd)
        assert closed.value.errno == errno.EBADF
    finally:
        _clear_execution_hooks()
        if monitoring_tool_id is not None:
            sys.monitoring.set_local_events(
                monitoring_tool_id,
                contained._restore_execution_hook_bounded.__code__,
                0,
            )
            sys.monitoring.register_callback(
                monitoring_tool_id,
                sys.monitoring.events.INSTRUCTION,
                None,
            )
            sys.monitoring.free_tool_id(monitoring_tool_id)
        with contained.suppress(BaseException):
            tracker.close()
        _close_test_fd_if_open(queue_fd)
        _restore_execution_hooks_for_test(original_trace, original_profile)


@pytest.mark.skipif(
    not hasattr(select, "kqueue") or not hasattr(signal, "SIGUSR1"),
    reason="real kqueue and asynchronous signal hook restoration contract",
)
def test_real_signal_during_hook_restoration_keeps_both_hook_identities(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_trace = sys.gettrace()
    original_profile = sys.getprofile()
    previous_handler = signal.getsignal(signal.SIGUSR1)
    tracker = contained._DarwinKqueueProcessTracker()
    queue = select.kqueue()
    queue_fd = queue.fileno()
    signal_failure = InterruptedError("signal interrupted hook restoration")
    sent = False

    def trace_hook(_frame: object, _event: str, _arg: object) -> object:
        return trace_hook

    def signal_handler(_signum: int, _frame: object) -> None:
        raise signal_failure

    def profile_hook(frame: object, event: str, _arg: object) -> None:
        nonlocal sent
        if (
            not sent
            and event == "call"
            and getattr(frame, "f_code", None) is contained._restore_execution_hook_bounded.__code__
            and getattr(frame, "f_locals", {}).get("label") == "trace"
        ):
            sent = True
            os.kill(os.getpid(), signal.SIGUSR1)

    monkeypatch.setattr(contained.select, "kqueue", lambda: queue)
    signal.signal(signal.SIGUSR1, signal_handler)
    try:
        sys.settrace(trace_hook)
        sys.setprofile(profile_hook)

        with pytest.raises(InterruptedError) as caught:
            tracker._initialize_queue()

        assert caught.value is signal_failure
        assert sent
        assert sys.gettrace() is trace_hook
        assert sys.getprofile() is profile_hook
        _clear_execution_hooks()
        tracker.close()
        with pytest.raises(OSError) as closed:
            os.fstat(queue_fd)
        assert closed.value.errno == errno.EBADF
    finally:
        _clear_execution_hooks()
        signal.signal(signal.SIGUSR1, previous_handler)
        with contained.suppress(BaseException):
            tracker.close()
        _close_test_fd_if_open(queue_fd)
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


def test_linux_subreaper_restore_failure_releases_process_wide_lock(monkeypatch) -> None:
    tracker = contained._LinuxSubreaperProcessTracker()
    tracker._owns_subreaper_lock = True
    tracker._previous_subreaper = 0
    tracker._subreaper_changed = True
    released: list[bool] = []

    class _FailingLibc:
        @staticmethod
        def prctl(*_args) -> int:
            return -1

    class _FakeLock:
        @staticmethod
        def release() -> None:
            released.append(True)

    monkeypatch.setattr(contained.ctypes, "CDLL", lambda *_args, **_kwargs: _FailingLibc())
    monkeypatch.setattr(contained, "_LINUX_SUBREAPER_LOCK", _FakeLock())

    with pytest.raises(contained.ContainedProcessError, match="restore child subreaper"):
        tracker.close()

    assert released == [True]
    assert not tracker._owns_subreaper_lock


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


def test_linux_pidfd_has_no_return_event_before_pending_registration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = contained._LinuxSubreaperProcessTracker()
    read_fd, write_fd = contained.os.pipe()
    fault = _ContainedReturnFault(
        {"<lambda>"},
        RuntimeError("pidfd acquisition callback return failed"),
    )

    def open_pidfd(_pid: int, _flags: int) -> int:
        return read_fd

    monkeypatch.setattr(contained.os, "pidfd_open", open_pidfd, raising=False)
    try:
        sys.settrace(fault.trace)
        tracker._bind_pid(contained.ProcessIdentity(101, (1, 0)))
        sys.settrace(None)

        assert not fault.triggered
        assert tracker._pidfds == {101: read_fd}
        assert tracker._pending_pidfds == []
        tracker.close()
        with pytest.raises(OSError) as closed:
            contained.os.fstat(read_fd)
        assert closed.value.errno == contained.errno.EBADF
    finally:
        sys.settrace(None)
        with contained.suppress(BaseException):
            tracker.close()
        _close_test_fd_if_open(read_fd)
        contained.os.close(write_fd)


@pytest.mark.parametrize("hook_kind", ("c_return", "opcode"))
def test_linux_pidfd_registration_is_atomic_to_execution_hooks(
    monkeypatch: pytest.MonkeyPatch,
    hook_kind: str,
) -> None:
    tracker = contained._LinuxSubreaperProcessTracker()
    read_fd, write_fd = contained.os.pipe()
    before = _open_file_descriptors()
    boundary_failure = RuntimeError(f"pidfd {hook_kind} boundary failed")
    pidfd_open: object = fcntl.fcntl
    if hook_kind == "opcode":

        def duplicate_pidfd(pid: int, _flags: int) -> int:
            return contained.os.dup(pid)

        pidfd_open = duplicate_pidfd
    monkeypatch.setattr(contained.os, "pidfd_open", pidfd_open, raising=False)
    fault, hook = _install_acquisition_fault(
        hook_kind,
        c_target=fcntl.fcntl,
        code=tracker._bind_pid.__func__.__code__,
        variable="descriptor",
        error=boundary_failure,
    )
    try:
        tracker._bind_pid(contained.ProcessIdentity(read_fd, (1, 0)))

        assert not fault.triggered  # type: ignore[attr-defined]
        _assert_acquisition_hook_restored(hook_kind, hook)
        assert len(tracker._pidfds) == 1
        assert tracker._pending_pidfds == []
        tracker.close()
        assert _open_file_descriptors() == before
    finally:
        _clear_execution_hooks()
        with contained.suppress(BaseException):
            tracker.close()
        for descriptor in _open_file_descriptors() - before:
            _close_test_fd_if_open(descriptor)
        _close_test_fd_if_open(read_fd)
        _close_test_fd_if_open(write_fd)


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

    trials = 1 if sys.platform == "darwin" else 25
    for trial in range(trials):
        marker = tmp_path / f"escaped-{trial}"
        started = tmp_path / f"started-{trial}"
        markers.append(marker)
        with pytest.raises(contained.ContainedProcessError):
            contained.run_contained(
                [sys.executable, "-c", child, str(marker), str(started)],
                cwd=tmp_path,
                deadline_monotonic=time.monotonic() + 0.6,
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


@pytest.mark.skipif(sys.platform != "darwin", reason="Darwin kqueue ownership contract")
@pytest.mark.parametrize("hook_kind", ("c_return", "opcode"))
def test_darwin_kqueue_registration_is_atomic_to_execution_hooks(
    monkeypatch: pytest.MonkeyPatch,
    hook_kind: str,
) -> None:
    tracker = contained._DarwinKqueueProcessTracker()
    queue = contained.select.kqueue()
    queue_fd = queue.fileno()
    available_queues = [queue]
    acquire_queue = available_queues.pop
    monkeypatch.setattr(contained.select, "kqueue", acquire_queue)
    before = _open_file_descriptors()
    boundary_failure = RuntimeError(f"kqueue {hook_kind} boundary failed")
    fault, hook = _install_acquisition_fault(
        hook_kind,
        c_target=acquire_queue,
        code=tracker._initialize_queue.__func__.__code__,
        variable="queue",
        error=boundary_failure,
    )
    try:
        tracker._initialize_queue()

        assert not fault.triggered  # type: ignore[attr-defined]
        _assert_acquisition_hook_restored(hook_kind, hook)
        assert tracker._owns_queue
        tracker.close()
        with pytest.raises(OSError) as closed:
            contained.os.fstat(queue_fd)
        assert closed.value.errno == contained.errno.EBADF
    finally:
        _clear_execution_hooks()
        with contained.suppress(BaseException):
            tracker.close()
        for unclaimed in available_queues:
            unclaimed.close()
        _close_test_fd_if_open(queue_fd)
        for descriptor in _open_file_descriptors() - before:
            _close_test_fd_if_open(descriptor)


@pytest.mark.skipif(sys.platform != "darwin", reason="Darwin kqueue ownership contract")
def test_execution_hook_disable_failure_precedes_kqueue_acquisition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = contained._DarwinKqueueProcessTracker()
    trace_hook = object()
    profile_hook = object()
    hooks = {"trace": trace_hook, "profile": profile_hook}
    disable_failure = RuntimeError("trace disable failed after mutation")
    acquisitions = 0

    def settrace(hook: object) -> None:
        hooks["trace"] = hook
        if hook is None:
            raise disable_failure

    def setprofile(hook: object) -> None:
        hooks["profile"] = hook

    def acquire_queue() -> object:
        nonlocal acquisitions
        acquisitions += 1
        return object()

    monkeypatch.setattr(contained.sys, "gettrace", lambda: hooks["trace"])
    monkeypatch.setattr(contained.sys, "getprofile", lambda: hooks["profile"])
    monkeypatch.setattr(contained.sys, "settrace", settrace)
    monkeypatch.setattr(contained.sys, "setprofile", setprofile)
    monkeypatch.setattr(contained.select, "kqueue", acquire_queue)

    with pytest.raises(RuntimeError) as caught:
        tracker._initialize_queue()

    assert caught.value is disable_failure
    assert acquisitions == 0
    assert hooks == {"trace": trace_hook, "profile": profile_hook}
    assert not tracker._owns_queue


@pytest.mark.skipif(sys.platform != "darwin", reason="Darwin kqueue ownership contract")
def test_kqueue_hook_restore_failure_retains_registered_queue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = contained._DarwinKqueueProcessTracker()
    read_fd, write_fd = contained.os.pipe()
    real_close = contained.os.close
    trace_hook = object()
    profile_hook = object()
    hooks = {"trace": trace_hook, "profile": profile_hook}
    restore_failure = RuntimeError("profile restore failed")
    restore_attempts = 0

    class Queue:
        def fileno(self) -> int:
            return read_fd

        def close(self) -> None:
            real_close(read_fd)

    def settrace(hook: object) -> None:
        hooks["trace"] = hook

    def setprofile(hook: object) -> None:
        nonlocal restore_attempts
        if hook is profile_hook:
            restore_attempts += 1
            raise restore_failure
        hooks["profile"] = hook

    monkeypatch.setattr(contained.sys, "gettrace", lambda: hooks["trace"])
    monkeypatch.setattr(contained.sys, "getprofile", lambda: hooks["profile"])
    monkeypatch.setattr(contained.sys, "settrace", settrace)
    monkeypatch.setattr(contained.sys, "setprofile", setprofile)
    monkeypatch.setattr(contained.select, "kqueue", Queue)
    try:
        with pytest.raises(RuntimeError) as caught:
            tracker._initialize_queue()

        assert caught.value is restore_failure
        assert restore_attempts == contained._SIGNAL_STATE_ATTEMPTS
        assert hooks["trace"] is trace_hook
        assert hooks["profile"] is None
        assert tracker._owns_queue
        assert tracker._queue is not None

        tracker.close()
        with pytest.raises(OSError) as closed:
            contained.os.fstat(read_fd)
        assert closed.value.errno == contained.errno.EBADF
    finally:
        with contained.suppress(BaseException):
            tracker.close()
        _close_test_fd_if_open(read_fd)
        real_close(write_fd)


@pytest.mark.skipif(sys.platform != "darwin", reason="Darwin kqueue ownership contract")
def test_hook_restore_failure_cannot_displace_kqueue_acquisition_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = contained._DarwinKqueueProcessTracker()
    trace_hook = object()
    profile_hook = object()
    hooks = {"trace": trace_hook, "profile": profile_hook}
    acquisition_failure = OSError("kqueue acquisition failed")
    restore_failure = RuntimeError("trace restore failed")

    def settrace(hook: object) -> None:
        if hook is trace_hook:
            raise restore_failure
        hooks["trace"] = hook

    def setprofile(hook: object) -> None:
        hooks["profile"] = hook

    def fail_acquisition() -> object:
        raise acquisition_failure

    monkeypatch.setattr(contained.sys, "gettrace", lambda: hooks["trace"])
    monkeypatch.setattr(contained.sys, "getprofile", lambda: hooks["profile"])
    monkeypatch.setattr(contained.sys, "settrace", settrace)
    monkeypatch.setattr(contained.sys, "setprofile", setprofile)
    monkeypatch.setattr(contained.select, "kqueue", fail_acquisition)

    with pytest.raises(OSError) as caught:
        tracker._initialize_queue()

    assert caught.value is acquisition_failure
    cleanup_group = getattr(caught.value, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert restore_failure in cleanup_group.exceptions


@pytest.mark.skipif(sys.platform != "darwin", reason="Darwin kqueue ownership contract")
def test_darwin_kqueue_return_exception_keeps_tracker_shell_owned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    read_fd, write_fd = contained.os.pipe()
    real_close = contained.os.close
    boundary_failure = RuntimeError("kqueue acquisition return failed")
    fault = _ContainedReturnFault(
        {"_initialize_queue"},
        boundary_failure,
    )

    class Queue:
        def fileno(self) -> int:
            return read_fd

        def close(self) -> None:
            real_close(read_fd)

    queue = Queue()
    monkeypatch.setattr(contained.select, "kqueue", lambda: queue)
    tracker = contained._DarwinKqueueProcessTracker()
    try:
        sys.settrace(fault.trace)
        with pytest.raises(RuntimeError) as caught:
            tracker._initialize_queue()
        sys.settrace(None)

        assert fault.triggered
        assert caught.value is boundary_failure

        tracker.close()
        assert not tracker._owns_queue
        with pytest.raises(OSError) as closed:
            contained.os.fstat(read_fd)
        assert closed.value.errno == contained.errno.EBADF
    finally:
        sys.settrace(None)
        with contained.suppress(BaseException):
            tracker.close()
        _close_test_fd_if_open(read_fd)
        real_close(write_fd)


@pytest.mark.skipif(sys.platform != "darwin", reason="Darwin kqueue ownership contract")
def test_darwin_kqueue_post_acquisition_exception_retains_close_ownership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    read_fd, write_fd = contained.os.pipe()
    real_close = contained.os.close
    boundary_failure = RuntimeError("kqueue post-acquisition boundary failed")
    close_failure = OSError(contained.errno.EIO, "kqueue rollback close failed")
    fault = _ContainedReturnFault({"_initialize_queue"}, boundary_failure)

    class RetryingQueue:
        def __init__(self) -> None:
            self.close_fails = True
            self.close_attempts = 0

        def fileno(self) -> int:
            return read_fd

        def close(self) -> None:
            self.close_attempts += 1
            if self.close_fails:
                raise close_failure
            real_close(read_fd)

    queue = RetryingQueue()

    monkeypatch.setattr(contained.select, "kqueue", lambda: queue)
    tracker = contained._DarwinKqueueProcessTracker()
    try:
        sys.settrace(fault.trace)
        with pytest.raises(RuntimeError) as caught:
            tracker._initialize_queue()
        sys.settrace(None)

        assert caught.value is boundary_failure

        with pytest.raises(contained.ContainedProcessError) as close_caught:
            tracker.close()
        assert queue.close_attempts == contained._SIGNAL_STATE_ATTEMPTS
        assert tracker._owns_queue
        cleanup_group = getattr(close_caught.value, "cleanup_error_group", None)
        assert isinstance(cleanup_group, BaseExceptionGroup)
        assert cleanup_group.exceptions[0] is close_failure
        assert "remains open" in str(cleanup_group.exceptions[-1])

        queue.close_fails = False
        tracker.close()
        assert not tracker._owns_queue
        with pytest.raises(OSError) as closed:
            contained.os.fstat(read_fd)
        assert closed.value.errno == contained.errno.EBADF
    finally:
        sys.settrace(None)
        queue.close_fails = False
        with contained.suppress(BaseException):
            tracker.close()
        _close_test_fd_if_open(read_fd)
        real_close(write_fd)


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
        lambda identity: registered.append(identity) or True,
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


@pytest.mark.skipif(sys.platform != "darwin", reason="Darwin pipe anchor contract")
def test_darwin_anchor_has_no_return_event_before_pending_registration(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    tracker = _FakeKernelTracker(identity=contained.ProcessIdentity(1, (1, 0)))
    real_dup = contained.os.dup
    duplicated: list[int] = []
    fault = _ContainedReturnFault(
        {"<lambda>"},
        RuntimeError("anchor acquisition callback return failed"),
    )

    def duplicate_anchor(descriptor: int) -> int:
        anchor = real_dup(descriptor)
        duplicated.append(anchor)
        return anchor

    def live_inventory(_deadline: float) -> dict[int, contained._ProcessObservation]:
        identity = tracker.registered_identity
        if identity is None:
            return {}
        observation = contained._process_observation(identity.pid)
        if observation is None:
            return {}
        return {
            identity.pid: contained._ProcessObservation(
                identity=identity,
                parent_pid=observation.parent_pid,
            )
        }

    monkeypatch.setattr(contained.os, "dup", duplicate_anchor)
    try:
        sys.settrace(fault.trace)
        result = contained.run_contained(
            [sys.executable, "-c", "import time; time.sleep(0.2)"],
            cwd=tmp_path,
            deadline_monotonic=time.monotonic() + 2,
            inventory_provider=live_inventory,
            kernel_tracker_factory=_tracker_factory(tracker),
            may_spawn_background_descendants=False,
        )
        sys.settrace(None)

        assert not fault.triggered
        assert result.returncode == 0
        assert len(duplicated) == 2
        for descriptor in duplicated:
            with pytest.raises(OSError) as closed:
                contained.os.fstat(descriptor)
            assert closed.value.errno == contained.errno.EBADF
    finally:
        sys.settrace(None)
        for descriptor in duplicated:
            _close_test_fd_if_open(descriptor)


@pytest.mark.skipif(sys.platform != "darwin", reason="Darwin pipe anchor contract")
@pytest.mark.parametrize("hook_kind", ("c_return", "opcode"))
def test_darwin_anchor_registration_is_atomic_to_execution_hooks(
    tmp_path: Path,
    hook_kind: str,
) -> None:
    tracker = _FakeKernelTracker(identity=contained.ProcessIdentity(1, (1, 0)))
    before = _open_file_descriptors()
    boundary_failure = RuntimeError(f"anchor {hook_kind} boundary failed")
    fault, hook = _install_acquisition_fault(
        hook_kind,
        c_target=contained.os.dup,
        code=contained.run_contained.__code__,
        variable="anchor",
        error=boundary_failure,
    )

    def live_inventory(_deadline: float) -> dict[int, contained._ProcessObservation]:
        identity = tracker.registered_identity
        if identity is None:
            return {}
        observation = contained._process_observation(identity.pid)
        if observation is None:
            return {}
        return {
            identity.pid: contained._ProcessObservation(
                identity=identity,
                parent_pid=observation.parent_pid,
            )
        }

    try:
        result = contained.run_contained(
            [sys.executable, "-c", "import time; time.sleep(0.2)"],
            cwd=tmp_path,
            deadline_monotonic=time.monotonic() + 2,
            inventory_provider=live_inventory,
            kernel_tracker_factory=_tracker_factory(tracker),
            may_spawn_background_descendants=False,
        )

        assert not fault.triggered  # type: ignore[attr-defined]
        _assert_acquisition_hook_restored(hook_kind, hook)
        assert result.returncode == 0
        assert _open_file_descriptors() == before
    finally:
        _clear_execution_hooks()
        for descriptor in _open_file_descriptors() - before:
            _close_test_fd_if_open(descriptor)


@pytest.mark.skipif(sys.platform != "darwin", reason="Darwin pipe anchor contract")
@pytest.mark.parametrize("close_fails", (False, True), ids=("closed", "retained"))
def test_darwin_anchor_post_dup_trace_preserves_pending_ownership(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    close_fails: bool,
) -> None:
    tracker = _FakeKernelTracker(identity=contained.ProcessIdentity(1, (1, 0)))
    real_dup = contained.os.dup
    real_close = contained.os.close
    real_finish = contained._finish_signal_restoration
    boundary_failure = RuntimeError("anchor post-dup boundary failed")
    close_failure = OSError(contained.errno.EIO, "anchor pending close failed")
    duplicated: list[int] = []
    replay_states: list[bool] = []
    close_attempts = 0
    fault = _NextContainedLineFault(boundary_failure)

    def acquire_anchor(descriptor: int) -> int:
        anchor = real_dup(descriptor)
        duplicated.append(anchor)
        fault.arm()
        return anchor

    def close_anchor(descriptor: int) -> None:
        nonlocal close_attempts
        if close_fails and descriptor in duplicated:
            close_attempts += 1
            raise close_failure
        real_close(descriptor)

    def capture_replay_state(*args: object, **kwargs: object) -> None:
        replay_states.append(bool(kwargs.get("replay_ready", True)))
        real_finish(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(contained.os, "dup", acquire_anchor)
    monkeypatch.setattr(contained.os, "close", close_anchor)
    monkeypatch.setattr(contained, "_finish_signal_restoration", capture_replay_state)
    try:
        sys.settrace(fault.trace)
        with pytest.raises(RuntimeError) as caught:
            contained.run_contained(
                [sys.executable, "-c", "pass"],
                cwd=tmp_path,
                deadline_monotonic=time.monotonic() + 2,
                inventory_provider=lambda _deadline: {},
                kernel_tracker_factory=_tracker_factory(tracker),
                may_spawn_background_descendants=False,
            )
        sys.settrace(None)

        assert caught.value is boundary_failure
        assert len(duplicated) == 1
        if close_fails:
            assert close_attempts >= contained._SIGNAL_STATE_ATTEMPTS
            assert replay_states == [False]
            contained.os.fstat(duplicated[0])
            cleanup_group = getattr(caught.value, "cleanup_error_group", None)
            assert isinstance(cleanup_group, BaseExceptionGroup)
            assert close_failure in cleanup_group.exceptions
        else:
            assert replay_states == [True]
            with pytest.raises(OSError) as closed:
                contained.os.fstat(duplicated[0])
            assert closed.value.errno == contained.errno.EBADF
    finally:
        sys.settrace(None)
        monkeypatch.setattr(contained.os, "close", real_close)
        for descriptor in duplicated:
            _close_test_fd_if_open(descriptor)


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
        root / "src" / "rquant" / "research_manifest.py",
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
