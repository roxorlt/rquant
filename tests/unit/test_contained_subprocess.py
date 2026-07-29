from __future__ import annotations

import signal
import sys

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
        )
    except contained.ContainedProcessError:
        pass
    else:
        raise AssertionError("detached descendant was accepted as a successful command")

    contained.time.sleep(0.35)
    assert not marker.exists()
