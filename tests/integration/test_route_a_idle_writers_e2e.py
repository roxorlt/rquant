"""#271: what the resident roles write when the host has nothing to do.

Production samples the runtime writing all night with the market shut. Package V took the
notifier's two writes off the clock; this file is the acceptance for the rest of them, and
it measures rather than argues:

* `runtime-health.all.v1` hashed its own `observed_at` into `generation_id` and published
  every ten seconds; `lab-jobs.serving.v1` did the same every thirty, through an ETA
  restated as of the moment it was asked;
* so `serving.publisher.v1`, whose inputs those two are, rebuilt a whole `serving.duckdb`
  generation every thirty seconds -- write, verify, hash, fsync, pointer switch;
* `signal-router.all-strategies.v1` rewrote three watermark rows every two seconds and
  `paper-broker.shadow-main.v1` one, because the column that moved was `updated_at`;
* `watchlist-quote.source.v1` published a spool batch every five seconds with no content
  gate, including on the paths that never called a provider at all;
* the daily orchestrator took a writer lease three times a minute with no run to advance.

The world is the one package J and the all-roles sandbox file already build: two really
installed generations, a real staged and published authority chain, the wrapper's own argv
and child environment, credentials delivered the way `LoadCredentialEncrypted` delivers
them, and a clock at 06:00 Shanghai on a date the bundle's calendar does not open.

Two things make it an *idle* world rather than a frozen one, and both matter:

* the clock advances by the role's own manifest interval on every iteration, so a defect
  that hashes "now" into an identity still fires here -- with a frozen clock every one of
  the six would look fixed;
* every peer's heartbeat is rewritten at each boundary the way a live peer would rewrite
  it, clock and counters moving, so `runtime-health.all.v1` reads genuinely different
  bytes on every iteration. That is the whole shape of its defect.

What is measured is the runtime root itself -- name, mode, size and modification time of
every file under it, `tree_state`, the same end-state instrument the all-roles sandbox file
uses -- with the heartbeat directories taken out, because a heartbeat is what a heartbeat
is for. It sees a write a syscall wrapper cannot: SQLite's commits come from C, and a
commit on these `journal_mode=WAL`, `synchronous=FULL` databases lands in a `-wal` file
whose size and mtime move. On Linux `/proc/self/io` is read as well and reported.
"""

from __future__ import annotations

import gc
import json
import os
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from threading import Event
from typing import Any
from unittest import mock

import pytest

import rquant.runtime_service_builtin as builtin_module
import rquant.runtime_service_main as service_main
from tests.integration.test_route_a_all_roles_sandbox_e2e import (
    CANNOT_BUILD,
    ROLE_UNITS,
    START_ORDER,
    credentials_root,
    instance_of,
    launch,
    minute_snapshot,
    provisioned_recovery,
    relocated_minute_snapshot,
)
from tests.integration.test_route_a_all_roles_sandbox_e2e import (
    relocated as relocated_argv,
)
from tests.integration.test_route_a_legacy_binding_e2e import RouteAWorld
from tests.integration.test_route_a_live_chain_idle_e2e import (
    FROZEN_NOW,
    _instance_name,
    cold_chain,
)

#: re-exported so pytest resolves the fixtures this file drives its world with
__all__ = [
    "cold_chain",
    "credentials_root",
    "minute_snapshot",
    "provisioned_recovery",
    "relocated_minute_snapshot",
]

pytestmark = pytest.mark.integration

#: Iterations per role: three for the role to settle into the steady state a resident
#: process spends its day in, then the sixty ruling 33 asks about.
SETTLING_ITERATIONS = 3
IDLE_ITERATIONS = 60

#: The two oneshots are not loops, and `page_control` resolves its runtime root from a
#: frozen constant rather than from argv, so there is no argv this harness can hand it --
#: the all-roles file measures that on its own.
NOT_A_LOOP = frozenset({"runtime_recovery", "runtime_recovery_rehearsal", "page_control"})

#: Roles whose every iteration in *this* world raises, and the document each one is
#: missing. A failing iteration writes its heartbeat and nothing else, so measuring one
#: here would prove nothing about the defect -- it is recorded, exactly, so that a role
#: that starts succeeding, or starts failing for a different reason, fails this file. The
#: production inputs fixture produces no lab jobs database, no experiment registry, no
#: minute batch and no published signal authority, and building those is four other
#: acceptances. Each of these four is measured for sixty idle iterations through its own
#: real builder instead, in the unit file named beside it.
DEGRADED_IN_THIS_WORLD: dict[str, str] = {
    #: tests/unit/test_runtime_builder_serving.py
    "serving_publisher": "signals reader failed",
    #: tests/unit/test_lab_jobs_serving_authority.py
    "lab_jobs_publisher": "unable to open database file",
    #: tests/unit/test_paper_signal_consumer.py
    "paper_broker": "current pointer is unavailable",
    "paper_constraint_publisher": "paper constraints require a visible market-minute batch",
    "auction_universe_publisher": "calendar has no next open date",
    "feature_live": "read-only spool source identity is missing",
    "notifier": "rquant_ro.duckdb",
    "promotions_publisher": "experiment registry does not exist",
    "lab_artifact_catalog": "artifact ancestor is missing",
}
#: `candidate_publisher` 从这张表里去掉了（#278）。它原来每轮都抛
#: `candidate input is unavailable or contains a symlink`——两个 document-driven 实例在这个
#: 世界里没有封存文档可读。改成 `session_document` 之后，非交易日的那一轮**什么都不做**
#: （连日历都在 build 时读过一次），于是它进了「每一轮都成功」的那一组，并且在这里被真正
#: 测出空闲写次数：应当是 0。


def _heartbeat_files(runtime_root: Path) -> tuple[Path, ...]:
    return tuple(sorted((runtime_root / "control").rglob("heartbeats/*.json")))


#: SQLite's own sidecars. Opening a connection creates the wal-index and truncates the
#: write-ahead log whether or not anything is committed, and closing the last one
#: checkpoints and removes them -- so their mtimes, and the mtime of the directory they
#: live in, move on an iteration that only *read*. Every store in this runtime closes its
#: connection at the end of each operation, which is what makes the main database file the
#: sound instrument: SQLite checkpoints into it as the last connection goes, so its size
#: and mtime move exactly when something was committed. That is package V's instrument,
#: and this is the same one over every file in the tree.
_SQLITE_SIDECARS = ("-wal", "-shm", "-journal")


def _durable_state(runtime_root: Path) -> tuple[tuple[str, int, int, int], ...]:
    """Every regular file under the runtime root that is durable state.

    Left out, and each for its own reason: the heartbeats, because a heartbeat is the one
    write a resident role is *supposed* to make every iteration; SQLite's sidecars, per
    the note above; and directories, whose mtime moves when a sidecar is created or
    removed inside them. Nothing a role durably wrote can hide behind any of those -- a
    file it creates, replaces or grows shows up as its own entry.
    """

    entries: list[tuple[str, int, int, int]] = []
    for path in sorted(runtime_root.rglob("*")):
        relative = path.relative_to(runtime_root)
        if "heartbeats" in relative.parts:
            continue
        if path.name.endswith(_SQLITE_SIDECARS):
            continue
        if path.is_dir() and not path.is_symlink():
            continue
        try:
            observed = path.lstat()
        except FileNotFoundError:
            # A sidecar SQLite created and removed inside one walk. Recorded as gone
            # rather than crashing the walk, so it still counts as a change if it was
            # there a moment ago -- which is the point.
            entries.append((str(path.relative_to(runtime_root)), -1, -1, -1))
            continue
        entries.append(
            (
                str(path.relative_to(runtime_root)),
                observed.st_mode,
                observed.st_size,
                observed.st_mtime_ns,
            )
        )
    return tuple(entries)


def _proc_io() -> dict[str, int] | None:
    """`syscw` and `write_bytes` for this process, where the platform will say."""

    try:
        text = Path("/proc/self/io").read_text(encoding="utf-8")
    except OSError:
        return None
    values: dict[str, int] = {}
    for line in text.splitlines():
        name, _, raw = line.partition(":")
        if name in {"syscw", "write_bytes"}:
            values[name] = int(raw.strip())
    return values


def _tick_peer_heartbeats(runtime_root: Path, *, at: datetime, skip: Path | None) -> None:
    """Move every other role's heartbeat on, the way an *idle* live peer would.

    Exactly three fields move when a resident role succeeds at doing nothing: the two
    clocks and the lifetime `total_successes` tally. Everything else a heartbeat carries
    is either the state it is in or the work it did, and an idle iteration does no work --
    `processed_count` is this iteration's count and stays zero, the cursor does not move,
    the backlog stays where it was. Writing it any other way would be simulating a busy
    host and calling it idle. The duration window is left alone as well, because the
    heartbeat model checks `last_step_duration_seconds` and the p95 against it.
    """

    stamp = (at - timedelta(seconds=1)).isoformat()
    for path in _heartbeat_files(runtime_root):
        if skip is not None and path == skip:
            continue
        document = json.loads(path.read_text(encoding="utf-8"))
        if document.get("status") == "stopped":
            continue
        document["heartbeat_at"] = stamp
        if document.get("last_success_at") is not None:
            document["last_success_at"] = stamp
        document["total_successes"] = int(document.get("total_successes", 0)) + 1
        path.write_text(
            json.dumps(document, ensure_ascii=True, separators=(",", ":"), sort_keys=True),
            encoding="utf-8",
        )


class _IdleIterations(Event):
    """Run the loop a fixed number of times, sampling the runtime root after each one.

    `wait()` is the loop's own post-iteration call, which makes it the iteration boundary:
    the sample is taken there, the clock is moved on by this role's interval, and the peers
    are given the heartbeat a live peer would have written by then.
    """

    def __init__(
        self,
        *,
        limit: int,
        runtime_root: Path,
        clock: list[datetime],
        interval: timedelta,
        own_heartbeat: Path | None,
    ) -> None:
        super().__init__()
        self.limit = limit
        self.runtime_root = runtime_root
        self.clock = clock
        self.interval = interval
        self.own_heartbeat = own_heartbeat
        self.iterations = 0
        self.samples: list[tuple[tuple[str, int, int, int], ...]] = []
        self.io: list[dict[str, int] | None] = []

    def is_set(self) -> bool:
        return super().is_set() or self.iterations >= self.limit

    def wait(self, timeout: float | None = None) -> bool:  # noqa: ARG002 - not a sleep
        self.iterations += 1
        # A SQLite connection an earlier role in this same process left open checkpoints
        # its write-ahead log into the main database when the interpreter finally collects
        # it -- which modifies a file nobody wrote to, at whatever moment the collector
        # happens to run. The all-roles sandbox file records the same artifact. Collecting
        # here puts it before the sample instead of between two of them. Under systemd
        # each role is its own process and none of this exists.
        gc.collect()
        self.samples.append(_durable_state(self.runtime_root))
        self.io.append(_proc_io())
        self.clock[0] = self.clock[0] + self.interval
        _tick_peer_heartbeats(self.runtime_root, at=self.clock[0], skip=self.own_heartbeat)
        if self.iterations >= self.limit:
            self.set()
        return True


class IdleRun:
    """What one role did over its iterations, and which of them wrote anything."""

    def __init__(self, role: str, instance: str, interval: timedelta) -> None:
        self.role = role
        self.instance = instance
        self.interval = interval
        self.iterations = 0
        self.changed: tuple[int, ...] = ()
        self.idle_changes: tuple[tuple[int, tuple[str, ...]], ...] = ()
        self.refusal: BaseException | None = None
        self.traceback: str | None = None
        self.io_delta: dict[str, int] | None = None
        self.successes = 0
        self.failures = 0
        self.last_error: str | None = None

    @property
    def every_iteration_succeeded(self) -> bool:
        return self.failures == 0 and self.successes >= self.iterations

    @property
    def writes_per_minute(self) -> float:
        """Iterations that changed the runtime root, over the idle window, per minute."""

        window = self.interval.total_seconds() * IDLE_ITERATIONS
        return 60.0 * len(self.idle_changes) / window

    def __repr__(self) -> str:  # pragma: no cover - diagnostics only
        return (
            f"IdleRun(role={self.role!r}, iterations={self.iterations}, "
            f"changed={self.changed}, refusal={self.refusal!r})"
        )


def _interval_of(route: RouteAWorld, instance: str) -> tuple[timedelta, Any]:
    manifest = next(
        manifest
        for manifest in route.profile.manifests
        if _instance_name(manifest.service_id) == instance
    )
    return timedelta(seconds=manifest.interval_seconds), manifest


def _changed_paths(
    before: tuple[tuple[str, int, int, int], ...],
    after: tuple[tuple[str, int, int, int], ...],
) -> tuple[str, ...]:
    kept = {entry[0]: entry[1:] for entry in before}
    now = {entry[0]: entry[1:] for entry in after}
    moved = {name for name in kept.keys() & now.keys() if kept[name] != now[name]}
    return tuple(sorted(set(kept) ^ set(now) | moved))


def run_idle(
    route: RouteAWorld,
    role: str,
    *,
    instance: str,
    credentials: Path | None,
) -> IdleRun:
    """One role, `SETTLING_ITERATIONS + IDLE_ITERATIONS` passes, nothing else running."""

    interval, manifest = _interval_of(route, instance)
    run = IdleRun(role, instance, interval)
    resolved = launch(route, role, instance, credentials)
    argv = relocated_argv(route, list(resolved["module_argv"]))
    control_root = Path(argv[argv.index("--control-root") + 1])
    from rquant.runtime_service_control import RuntimeServiceControl

    own_heartbeat = RuntimeServiceControl._path_for(control_root, manifest.service_spec)

    clock = [FROZEN_NOW]
    stop = _IdleIterations(
        limit=SETTLING_ITERATIONS + IDLE_ITERATIONS,
        runtime_root=route.runtime_root,
        clock=clock,
        interval=interval,
        own_heartbeat=own_heartbeat,
    )
    real_event = service_main.Event
    real_registry = builtin_module.build_builtin_registry
    service_main.Event = lambda: stop  # type: ignore[assignment]
    builtin_module.build_builtin_registry = (  # type: ignore[assignment]
        lambda **kwargs: real_registry(clock=lambda: clock[0], **kwargs)
    )
    before_io = _proc_io()
    try:
        with mock.patch.dict(os.environ, dict(resolved["environment"]), clear=True):
            arguments = service_main.build_parser().parse_args(argv)
            service_main.run(arguments)
    except BaseException as error:  # noqa: BLE001 - recorded, then reported
        run.refusal = error
        run.traceback = "".join(
            traceback.format_exception(type(error), error, error.__traceback__)
        )
    finally:
        service_main.Event = real_event  # type: ignore[assignment]
        builtin_module.build_builtin_registry = real_registry  # type: ignore[assignment]

    heartbeat = RuntimeServiceControl.read_heartbeat(control_root, manifest.service_spec)
    if heartbeat is not None:
        run.successes = heartbeat.total_successes
        run.failures = heartbeat.total_failures
        run.last_error = heartbeat.last_error
    run.iterations = stop.iterations
    run.changed = tuple(
        index
        for index in range(1, len(stop.samples))
        if stop.samples[index] != stop.samples[index - 1]
    )
    run.idle_changes = tuple(
        (index, _changed_paths(stop.samples[index - 1], stop.samples[index]))
        for index in run.changed
        if index >= SETTLING_ITERATIONS
    )
    after_io = stop.io[-1] if stop.io else None
    settled_io = stop.io[SETTLING_ITERATIONS - 1] if len(stop.io) >= SETTLING_ITERATIONS else None
    if before_io is not None and after_io is not None and settled_io is not None:
        run.io_delta = {name: after_io[name] - settled_io[name] for name in after_io}
    return run


def _loop_roles() -> tuple[str, ...]:
    return tuple(
        role
        for role in START_ORDER
        if role not in NOT_A_LOOP and role not in CANNOT_BUILD
    )


def test_every_resident_role_idle_for_sixty_iterations_writes_only_its_heartbeat(
    minute_snapshot: bytes,
    cold_chain: RouteAWorld,
    relocated_minute_snapshot: None,
    credentials_root: dict[str, Path],
    provisioned_recovery: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Sixty idle iterations per role, and nothing under the runtime root moves."""

    runs: list[IdleRun] = []
    for role in _loop_roles():
        for instance in instance_of(cold_chain, role):
            runs.append(
                run_idle(
                    cold_chain,
                    role,
                    instance=instance,
                    credentials=credentials_root.get(instance),
                )
            )

    refused = {run.role: run.traceback for run in runs if run.refusal is not None}
    assert refused == {}, "\n\n".join(f"== {role} ==\n{text}" for role, text in refused.items())
    stalled = {run.role for run in runs if run.iterations != SETTLING_ITERATIONS + IDLE_ITERATIONS}
    assert stalled == set(), stalled

    measured = [run for run in runs if run.every_iteration_succeeded]
    with capsys.disabled():  # pragma: no cover - the report's own table
        print()
        print(f"{'role':32} {'interval':>9} {'ok':>4} {'idle writes':>12} {'per minute':>11}")
        for run in runs:
            mark = "yes" if run.every_iteration_succeeded else "-"
            print(
                f"{run.role:32} {run.interval.total_seconds():8.0f}s {mark:>4} "
                f"{len(run.idle_changes):12} {run.writes_per_minute:11.2f}"
            )
            if run.io_delta is not None:
                print(f"{'':32} /proc/self/io {run.io_delta}")
            touched = sorted({name for _index, names in run.idle_changes for name in names})
            for name in touched:
                print(f"{'':32}   wrote {name}")

    #: which roles actually did their work on every one of their iterations, and which
    #: only reached their loop. A role that raises writes a heartbeat and nothing else, so
    #: it has to be separated out rather than counted as a quiet one.
    degraded = {run.role: run.last_error or "" for run in runs if not run.every_iteration_succeeded}
    assert set(degraded) == set(DEGRADED_IN_THIS_WORLD), degraded
    for role, reason in DEGRADED_IN_THIS_WORLD.items():
        assert reason in degraded[role], (role, degraded[role])

    wrote = {
        f"{run.role}[{run.instance[:10]}]": run.idle_changes
        for run in measured
        if run.idle_changes
    }
    assert wrote == {}, wrote
    #: and the roles this file is actually about were among the ones measured
    assert {run.role for run in measured} >= {
        "runtime_health_publisher",
        "signal_router",
        "watchlist_quote_source",
    }


def test_the_roster_this_file_drives_is_every_resident_role_it_can_start() -> None:
    """Which roles are measured, and the exact reason for each one that is not."""

    measured = set(_loop_roles())
    assert measured | NOT_A_LOOP | set(CANNOT_BUILD) == set(ROLE_UNITS)
    assert measured & NOT_A_LOOP == set()
    assert measured & set(CANNOT_BUILD) == set()
    #: the three this world cannot build are the three the all-roles acceptance already
    #: records, each stopping on a document the production inputs fixture does not produce
    assert set(CANNOT_BUILD) == {
        "shadow_session",
        "daily_pipeline_orchestrator",
        "artifact_retention",
    }
    assert len(measured) == 19
