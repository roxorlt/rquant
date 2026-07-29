from __future__ import annotations

import signal
import sys
import time
from pathlib import Path

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


def test_cancellation_after_spawn_still_inventories_before_cleanup(
    tmp_path: Path,
) -> None:
    events: list[str] = []

    def inventory(_deadline: float) -> dict[int, contained._ProcessObservation]:
        events.append("inventory")
        return {}

    def cancel() -> bool:
        events.append("cancel")
        return True

    with pytest.raises(contained.ContainedProcessError):
        contained.run_contained(
            [sys.executable, "-c", "import time; time.sleep(1)"],
            cwd=tmp_path,
            deadline_monotonic=time.monotonic() + 1,
            inventory_provider=inventory,
            cancellation_check=cancel,
        )

    assert events[0] == "inventory"


def test_immediate_setsid_descendant_never_escapes_over_repeated_trials(
    tmp_path: Path,
) -> None:
    child = (
        "import os,subprocess,sys;"
        "subprocess.Popen([sys.executable,'-c',"
        "\"import pathlib,sys,time;time.sleep(.08);pathlib.Path(sys.argv[1]).write_text('x')\","
        "sys.argv[1]],start_new_session=True);os._exit(0)"
    )
    markers: list[Path] = []

    for trial in range(101):
        marker = tmp_path / f"escaped-{trial}"
        markers.append(marker)
        with pytest.raises(contained.ContainedProcessError):
            contained.run_contained(
                [sys.executable, "-c", child, str(marker)],
                cwd=tmp_path,
                deadline_monotonic=time.monotonic() + 0.6,
            )

    time.sleep(0.15)
    assert not any(marker.exists() for marker in markers)


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
