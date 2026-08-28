"""Real multiprocess contracts for the research/maintenance lifecycle arbiter."""

from __future__ import annotations

import fcntl
import importlib.machinery
import importlib.util
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from types import ModuleType

ROOT = Path(__file__).resolve().parents[2]
ARBITER = ROOT / "deploy/libexec/rquant-workload-arbiter"
RESEARCH_REJECTED = 75
MAINTENANCE_TIMEOUT = 74


def _command(
    root: Path,
    plane: str,
    code: str,
    *,
    timeout: float = 2.0,
    admission: list[str] | None = None,
) -> list[str]:
    command = [
        sys.executable,
        str(ARBITER),
        plane,
        "--test-root",
        str(root),
    ]
    if plane == "research":
        if admission is None:
            command.append("--test-skip-admission")
        else:
            command.extend(("--test-admission-command", json.dumps(admission)))
    else:
        command.extend(("--timeout-seconds", str(timeout)))
    command.extend(("--", sys.executable, "-c", code))
    return command


def _load_arbiter() -> ModuleType:
    """Import the installed helper by path. It is not importable as `rquant.*` by design."""

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
    return module


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

    from rquant import privilege_launcher

    module = _load_arbiter()

    command = ["/usr/bin/env", "true"]
    assert tuple(module.parent_death_argv(command, platform="linux")) == (
        privilege_launcher.build_parent_death_argv(
            launcher_path=privilege_launcher.PRODUCTION_PRIVILEGE_LAUNCHER,
            command=command,
        )
    )


def test_the_arbiter_leaves_a_non_linux_command_unwrapped() -> None:
    module = _load_arbiter()

    command = ["/usr/bin/env", "true"]
    assert module.parent_death_argv(command, platform="darwin") == command


# ---------------------------------------------------------------------------------------
# Codex round-3 verdict RQ-WI-R2-P1-01: nothing out of the checkout runs before the wrapper
# ---------------------------------------------------------------------------------------

#: Every spelling of "the mutable checkout" that must not survive anywhere in the helper.
#: This is stronger than any `ExecStart` assertion: it constrains the arbiter's own internal
#: constants, which no unit file can show.
CHECKOUT_MARKERS = (
    "/home/lighthouse/rquant",
    ".venv",
    "-m rquant.",
    "site-packages",
)


def test_the_arbiter_names_no_checkout_path_at_all() -> None:
    """The helper is root-owned and hash-pinned; it must not reach back into the checkout.

    Until the round-3 verdict the arbiter ran `.venv/bin/python -m rquant.workload_isolation`
    after taking the research locks and before exec'ing the verified child, which handed the
    unit's whole sandbox and `EnvironmentFile` to unverified code.
    """

    code = "\n".join(
        line
        for line in _arbiter_source().splitlines()
        if not line.lstrip().startswith("#")
    )

    offenders = sorted(marker for marker in CHECKOUT_MARKERS if marker in code)
    assert offenders == []


def test_the_arbiter_admission_argv_is_the_fixed_root_owned_wrapper() -> None:
    """Pin the admission argv to the wrapper's own constant rather than restating it."""

    from rquant.runtime_exec_wrapper._verify import PROTECTED_ROLES, RUNTIME_PYZ_PATH

    module = _load_arbiter()

    assert tuple(module._ADMISSION_COMMAND) == (
        "/usr/bin/python3.11",
        "-I",
        "-S",
        RUNTIME_PYZ_PATH,
        "--role",
        "workload_admission",
    )
    assert "workload_admission" in PROTECTED_ROLES


def test_a_rejecting_admission_still_surfaces_as_seventy_five(tmp_path: Path) -> None:
    """The exit-code contract the ten research units already carry must not move.

    Each of them declares `SuccessExitStatus=0 75`, so a rejected admission has to stay a
    75 from the arbiter — and the unit's own child must never have been started.
    """

    child_ran = tmp_path / "child-ran"
    rejected = subprocess.run(
        _command(
            tmp_path,
            "research",
            f"from pathlib import Path; Path({str(child_ran)!r}).touch()",
            admission=[sys.executable, "-c", "raise SystemExit(1)"],
        ),
        check=False,
        timeout=10,
    )

    assert rejected.returncode == RESEARCH_REJECTED
    assert not child_ran.exists()
    assert list((tmp_path / "run/rquant-workload-isolation/research-pids").iterdir()) == []


def test_an_accepting_admission_lets_the_verified_child_run(tmp_path: Path) -> None:
    """The other half of the same seam: exit 0 admits, and the child really runs."""

    child_ran = tmp_path / "child-ran"
    admitted = subprocess.run(
        _command(
            tmp_path,
            "research",
            f"from pathlib import Path; Path({str(child_ran)!r}).touch()",
            admission=[sys.executable, "-c", "raise SystemExit(0)"],
        ),
        check=False,
        timeout=10,
    )

    assert admitted.returncode == 0
    assert child_ran.exists()


def test_the_admission_child_inherits_no_lock_descriptor(tmp_path: Path) -> None:
    """The four plane locks are `O_CLOEXEC`, so the admission probe cannot release them.

    The probe walks its own descriptor table with `fstat` rather than listing `/proc/self/fd`
    or `/dev/fd`, so it opens nothing itself and the observation works on both platforms.
    """

    report = tmp_path / "admission-fds.json"
    probe = (
        "import json, os\n"
        "from pathlib import Path\n"
        "observed = []\n"
        "for fd in range(256):\n"
        "    try:\n"
        "        info = os.fstat(fd)\n"
        "    except OSError:\n"
        "        continue\n"
        "    observed.append([fd, info.st_dev, info.st_ino])\n"
        f"Path({str(report)!r}).write_text(json.dumps(observed), encoding='ascii')\n"
    )
    completed = subprocess.run(
        _command(
            tmp_path,
            "research",
            "raise SystemExit(0)",
            admission=[sys.executable, "-c", probe],
        ),
        check=False,
        timeout=10,
    )

    assert completed.returncode == 0
    observed = json.loads(report.read_text(encoding="ascii"))
    assert sorted(descriptor for descriptor, _device, _inode in observed) == [0, 1, 2]

    lock_root = tmp_path / "run/rquant-workload-isolation"
    locks = {
        (path.stat().st_dev, path.stat().st_ino)
        for name in _load_arbiter()._LOCK_NAMES
        if (path := lock_root / name).exists()
    }
    assert len(locks) == 4
    assert locks.isdisjoint({(device, inode) for _fd, device, inode in observed})


def test_the_admission_injection_is_limited_to_test_root_research() -> None:
    """The seam is guarded exactly like `--test-skip-admission`: production cannot reach it."""

    injection = json.dumps(["/usr/bin/true"])
    for arguments in (
        ["maintenance", "--test-admission-command", injection],
        ["research", "--test-admission-command", injection],
    ):
        refused = subprocess.run(
            [sys.executable, str(ARBITER), *arguments, "--", "/usr/bin/true"],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )

        assert refused.returncode == 2, refused.stderr
        assert "limited to test-root research" in refused.stderr


def test_the_two_admission_test_seams_are_mutually_exclusive(tmp_path: Path) -> None:
    refused = subprocess.run(
        [
            sys.executable,
            str(ARBITER),
            "research",
            "--test-root",
            str(tmp_path),
            "--test-skip-admission",
            "--test-admission-command",
            json.dumps(["/usr/bin/true"]),
            "--",
            "/usr/bin/true",
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert refused.returncode == 2, refused.stderr
    assert "mutually exclusive" in refused.stderr


def test_an_injected_admission_command_must_be_a_list_of_non_empty_strings(
    tmp_path: Path,
) -> None:
    for injection in ("not json", json.dumps([]), json.dumps(["", "x"]), json.dumps([7])):
        refused = subprocess.run(
            [
                sys.executable,
                str(ARBITER),
                "research",
                "--test-root",
                str(tmp_path),
                "--test-admission-command",
                injection,
                "--",
                "/usr/bin/true",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )

        assert refused.returncode == 2, refused.stderr
        assert "non-empty" in refused.stderr


# ---------------------------------------------------------------------------------------
# R3A-SPEC-02: the arbiter's own interpreter, before any of its code runs
# ---------------------------------------------------------------------------------------

#: The exact first line of the installed helper. `-I` drops `PYTHON*` from the environment,
#: drops the user site directory, and keeps the script's own directory off `sys.path`; `-S`
#: drops `site` altogether, and with it every `.pth` hook. Linux passes everything after the
#: interpreter path in a `#!` line as a single argument, so the two flags have to be spelled
#: as one token.
ARBITER_SHEBANG = "#!/usr/bin/python3 -IS"


def test_the_arbiter_interpreter_is_isolated_before_it_runs_a_line_of_its_own() -> None:
    """Independent review R3A-SPEC-02: `site.py` ran before `_prepare_root` could check anything.

    `/usr/bin/python3` on the production host has `site.ENABLE_USER_SITE` true, and
    `/home/lighthouse/.local/lib/python3.11/site-packages` is a directory the service user
    owns. `ProtectHome=read-only` stops it being written from inside the unit, not from
    outside, and it stops writes rather than reads — so a `usercustomize.py` placed there
    would have been imported by `site` at interpreter start-up, ahead of the lock root check,
    ahead of the admission probe, with the unit's whole sandbox and `EnvironmentFile`. The
    same line closes the `PYTHONPATH` / `PYTHONHOME` route, which an `EnvironmentFile` the
    service user can write could otherwise have supplied.
    """

    assert _arbiter_source().splitlines()[0] == ARBITER_SHEBANG


def test_the_arbiter_shebang_flags_really_isolate_this_interpreter() -> None:
    """Pin the spelling to its meaning, so `-IS` cannot rot into a token that parses but idles."""

    flags = ARBITER_SHEBANG.split(" ", 1)[1]
    observed = subprocess.run(
        [
            sys.executable,
            flags,
            "-c",
            "import json, sys; print(json.dumps({"
            "'isolated': bool(sys.flags.isolated),"
            "'no_site': bool(sys.flags.no_site),"
            "'user_site': bool(sys.flags.no_user_site),"
            "}))",
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
        env={"PATH": "/usr/bin:/bin", "PYTHONPATH": "/should-be-ignored"},
    )

    assert observed.returncode == 0, observed.stderr
    assert json.loads(observed.stdout) == {
        "isolated": True,
        "no_site": True,
        "user_site": True,
    }


def test_the_arbiter_still_runs_with_site_processing_disabled() -> None:
    """The flags are only safe if the helper needs nothing `site` would have provided.

    It imports the standard library and nothing else, but `fcntl` lives in `lib-dynload` and
    a stdlib-only claim is worth executing rather than asserting: this starts the real file
    under the real flags and lets `argparse` prove the module body imported.
    """

    observed = subprocess.run(
        [sys.executable, "-I", "-S", str(ARBITER), "--help"],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
        env={"PATH": "/usr/bin:/bin"},
    )

    assert observed.returncode == 0, observed.stderr
    assert "research" in observed.stdout
