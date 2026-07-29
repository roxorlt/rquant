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


def test_signal_latch_raises_once_and_defers_consecutive_signal() -> None:
    latch = contained._ContainedSignalLatch()

    with pytest.raises(contained._ContainedSignal) as first:
        latch.handle(signal.SIGTERM, None)

    latch.handle(signal.SIGINT, None)

    assert first.value.signum == signal.SIGTERM
    assert latch.first_signum == signal.SIGTERM


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
        with pytest.raises(contained.ContainedProcessError, match="tracker did not stop"):
            contained.run_contained(
                [sys.executable, "-c", "import time; time.sleep(10)"],
                cwd=tmp_path,
                deadline_monotonic=time.monotonic() + 0.5,
                inventory_provider=blocking_inventory,
                may_spawn_background_descendants=False,
            )
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
