from __future__ import annotations

import shutil
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace

import pytest

from rquant import contained_subprocess as contained


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


def test_signal_latch_records_first_signal_without_raising_from_handler() -> None:
    latch = contained._ContainedSignalLatch()

    latch.handle(signal.SIGTERM, None)
    latch.handle(signal.SIGINT, None)

    assert latch.first_signum == signal.SIGTERM


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="atomic signal-mask arbitration requires pthread_sigmask",
)
def test_signal_latch_install_failure_restores_handlers_and_mask(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_signal = contained.signal.signal
    watched = (signal.SIGINT, signal.SIGTERM)
    before_handlers = {signum: signal.getsignal(signum) for signum in watched}
    before_mask = signal.pthread_sigmask(signal.SIG_BLOCK, set())
    installs = 0
    primary = OSError("handler install boom")

    def fail_second_install(signum: int, handler: object) -> object:
        nonlocal installs
        installing_latch = isinstance(
            getattr(handler, "__self__", None), contained._ContainedSignalLatch
        )
        if installing_latch:
            installs += 1
            if installs == 2:
                raise primary
        return real_signal(signum, handler)  # type: ignore[arg-type]

    monkeypatch.setattr(contained.signal, "signal", fail_second_install)
    try:
        with pytest.raises(OSError) as caught:
            contained._install_signal_latch(contained._ContainedSignalLatch())
        observed_mask = signal.pthread_sigmask(signal.SIG_BLOCK, set())
        observed_handlers = {signum: signal.getsignal(signum) for signum in watched}
    finally:
        for signum, previous in before_handlers.items():
            real_signal(signum, previous)
        signal.pthread_sigmask(signal.SIG_SETMASK, before_mask)

    assert caught.value is primary
    assert observed_mask == before_mask
    assert observed_handlers == before_handlers


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
            assert handler is previous_handler
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
def test_restore_boundary_signal_latched_by_failed_handler_is_replayed(
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

    def previous_handler(_signum: int, _frame: object) -> None:
        raise replayed

    for signum in watched:
        real_signal(signum, previous_handler)
    previous_handlers, active_signals = contained._install_signal_latch(latch)

    def fail_sigterm_restore(signum: int, handler: object) -> object:
        if signum == signal.SIGTERM and handler is previous_handler:
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
    cleanup_group = getattr(caught.value, "cleanup_error_group", None)
    assert isinstance(cleanup_group, BaseExceptionGroup)
    assert restore_failure in cleanup_group.exceptions


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


@pytest.mark.skipif(sys.platform != "darwin", reason="Darwin kqueue shutdown contract")
def test_darwin_tracker_close_joins_before_closing_live_kqueue() -> None:
    tracker = contained._DarwinKqueueProcessTracker()
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


@pytest.mark.skipif(sys.platform != "darwin", reason="Darwin kqueue tracking contract")
def test_darwin_tracker_keeps_pipe_marked_grandchild_after_parent_chain_breaks(
    monkeypatch,
) -> None:
    tracker = contained._DarwinKqueueProcessTracker()
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
