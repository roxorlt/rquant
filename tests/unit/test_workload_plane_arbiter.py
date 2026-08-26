"""Real multiprocess contracts for the research/maintenance lifecycle arbiter."""

from __future__ import annotations

import fcntl
import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ARBITER = ROOT / "deploy/libexec/rquant-workload-arbiter"
RESEARCH_REJECTED = 75
MAINTENANCE_TIMEOUT = 74


def _command(root: Path, plane: str, code: str, *, timeout: float = 2.0) -> list[str]:
    command = [
        sys.executable,
        str(ARBITER),
        plane,
        "--test-root",
        str(root),
    ]
    if plane == "research":
        command.append("--test-skip-admission")
    else:
        command.extend(("--timeout-seconds", str(timeout)))
    command.extend(("--", sys.executable, "-c", code))
    return command


def _wait_for(path: Path, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return
        time.sleep(0.01)
    raise AssertionError(f"timed out waiting for {path}")


def test_multiple_research_processes_share_the_plane_lock(tmp_path: Path) -> None:
    started = time.monotonic()
    first = subprocess.Popen(_command(tmp_path, "research", "import time; time.sleep(.35)"))
    second = subprocess.Popen(_command(tmp_path, "research", "import time; time.sleep(.35)"))

    assert first.wait(timeout=2) == 0
    assert second.wait(timeout=2) == 0
    assert time.monotonic() - started < 0.65


def test_research_rejects_immediately_while_maintenance_is_active(
    tmp_path: Path,
) -> None:
    marker = tmp_path / "maintenance-ready"
    maintenance = subprocess.Popen(
        _command(
            tmp_path,
            "maintenance",
            f"from pathlib import Path; import time; Path({str(marker)!r}).touch(); time.sleep(.5)",
        )
    )
    _wait_for(marker)

    started = time.monotonic()
    research = subprocess.run(
        _command(tmp_path, "research", "raise SystemExit(0)"),
        check=False,
        timeout=2,
    )

    assert research.returncode == RESEARCH_REJECTED
    assert time.monotonic() - started < 0.3
    assert maintenance.wait(timeout=2) == 0


def test_maintenance_preempts_research_and_then_runs(tmp_path: Path) -> None:
    research_ready = tmp_path / "research-ready"
    maintenance_ran = tmp_path / "maintenance-ran"
    research = subprocess.Popen(
        _command(
            tmp_path,
            "research",
            "from pathlib import Path; import time; "
            f"Path({str(research_ready)!r}).touch(); time.sleep(10)",
        )
    )
    _wait_for(research_ready)

    maintenance = subprocess.run(
        _command(
            tmp_path,
            "maintenance",
            f"from pathlib import Path; Path({str(maintenance_ran)!r}).touch()",
        ),
        check=False,
        timeout=3,
    )

    assert maintenance.returncode == 0
    assert maintenance_ran.exists()
    assert research.wait(timeout=2) == RESEARCH_REJECTED


def test_crashed_research_releases_kernel_lock(tmp_path: Path) -> None:
    marker = tmp_path / "research-ready"
    research = subprocess.Popen(
        _command(
            tmp_path,
            "research",
            f"from pathlib import Path; import time; Path({str(marker)!r}).touch(); time.sleep(1)",
        )
    )
    _wait_for(marker)
    research.kill()
    research.wait(timeout=2)

    maintenance = subprocess.run(
        _command(tmp_path, "maintenance", "raise SystemExit(0)"),
        check=False,
        timeout=2,
    )

    assert maintenance.returncode == 0
    assert list((tmp_path / "run/rquant-workload-isolation/research-pids").iterdir()) == []


def test_registry_binds_pid_starttime_and_boot_id(tmp_path: Path) -> None:
    marker = tmp_path / "research-ready"
    research = subprocess.Popen(
        _command(
            tmp_path,
            "research",
            f"from pathlib import Path; import time; Path({str(marker)!r}).touch(); time.sleep(2)",
        )
    )
    _wait_for(marker)
    registry = tmp_path / "run/rquant-workload-isolation/research-pids"
    entries = list(registry.iterdir())
    assert len(entries) == 1
    payload = json.loads(entries[0].read_text(encoding="ascii"))

    assert payload["schema_version"] == 1
    assert payload["pid"] == research.pid
    assert payload["starttime_ticks"]
    assert payload["boot_id"] == "test-boot-id"

    research.terminate()
    assert research.wait(timeout=2) == RESEARCH_REJECTED


def test_pid_reuse_collision_is_reaped_without_signalling_live_process(
    tmp_path: Path,
) -> None:
    assert (
        subprocess.run(
            _command(tmp_path, "maintenance", "raise SystemExit(0)"),
            check=False,
            timeout=2,
        ).returncode
        == 0
    )
    lock_root = tmp_path / "run/rquant-workload-isolation"
    identities = lock_root / "process-identities"
    identity = {
        "pid": os.getpid(),
        "starttime_ticks": "current-process-start",
        "boot_id": "test-boot-id",
    }
    (identities / str(os.getpid())).write_text(json.dumps(identity), encoding="ascii")
    stale = {
        "schema_version": 1,
        "pid": os.getpid(),
        "plane": "research",
        "starttime_ticks": "reused-pid-old-start",
        "boot_id": "test-boot-id",
    }
    registry = lock_root / "research-pids" / str(os.getpid())
    registry.write_text(json.dumps(stale), encoding="ascii")

    maintenance = subprocess.run(
        _command(tmp_path, "maintenance", "raise SystemExit(0)"),
        check=False,
        timeout=2,
    )

    assert maintenance.returncode == 0
    assert not registry.exists()
    os.kill(os.getpid(), 0)


def test_maintenance_lock_wait_is_bounded(tmp_path: Path) -> None:
    lock_root = tmp_path / "run/rquant-workload-isolation"
    lock_root.mkdir(parents=True)
    research_lock = lock_root / "research-active.lock"
    research_lock.touch()
    descriptor = os.open(research_lock, os.O_RDWR)
    fcntl.flock(descriptor, fcntl.LOCK_SH)
    try:
        started = time.monotonic()
        maintenance = subprocess.run(
            _command(
                tmp_path,
                "maintenance",
                "raise SystemExit(0)",
                timeout=0.2,
            ),
            check=False,
            timeout=2,
        )
    finally:
        os.close(descriptor)

    assert maintenance.returncode == MAINTENANCE_TIMEOUT
    assert 0.15 <= time.monotonic() - started < 1.0


# ---------------------------------------------------------------------------------------
# P1-5 / ruling D-6: the arbiter no longer runs Python between fork and exec
# ---------------------------------------------------------------------------------------


def _arbiter_source() -> str:
    return ARBITER.read_text(encoding="utf-8")


def test_the_arbiter_never_passes_a_python_callable_to_popen() -> None:
    """The `ctypes` `prctl` call inside `preexec_fn` was the most dangerous of the pair.

    It allocated, opened a shared library and raised in a forked child that holds a copy
    of the parent's allocator locks without the parent's threads. `setpriv --pdeathsig`
    performs the same `PR_SET_PDEATHSIG` in C, before the exec, from a root-owned binary.
    """

    code = "\n".join(
        line
        for line in _arbiter_source().splitlines()
        if not line.lstrip().startswith("#")
    )

    assert "preexec_fn" not in code
    assert "ctypes" not in code
    assert "_linux_parent_death_signal" not in code


def test_the_arbiter_wraps_the_child_in_the_parent_death_launcher_on_linux() -> None:
    source = _arbiter_source()

    assert "PRIVILEGE_LAUNCHER = \"/usr/bin/setpriv\"" in source
    assert '"--pdeathsig"' in source
    assert '"SIGKILL"' in source


def test_the_arbiter_launcher_argv_matches_the_shared_launcher_contract() -> None:
    """The arbiter cannot import `rquant`, so its argv is restated. Pin the two together."""

    import importlib.util

    from rquant import privilege_launcher

    spec = importlib.util.spec_from_loader(
        "rquant_workload_arbiter_under_test",
        loader=importlib.machinery.SourceFileLoader(
            "rquant_workload_arbiter_under_test",
            str(ARBITER),
        ),
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    command = ["/usr/bin/env", "true"]
    assert tuple(module.parent_death_argv(command, platform="linux")) == (
        privilege_launcher.build_parent_death_argv(
            launcher_path=privilege_launcher.PRODUCTION_PRIVILEGE_LAUNCHER,
            command=command,
        )
    )


def test_the_arbiter_leaves_a_non_linux_command_unwrapped() -> None:
    import importlib.util

    spec = importlib.util.spec_from_loader(
        "rquant_workload_arbiter_under_test",
        loader=importlib.machinery.SourceFileLoader(
            "rquant_workload_arbiter_under_test",
            str(ARBITER),
        ),
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    command = ["/usr/bin/env", "true"]
    assert module.parent_death_argv(command, platform="darwin") == command
