"""#268 acceptance: the open does not cost ten minutes of the production monitor.

2026-09-14, the first trading day with all twenty runtime units resident. At 09:25 the
replica sync copied ten gigabytes, `rquant-monitor` started and scanned the main database,
the four read-side roles opened the new replica generation (the notifier's `minute_bar`
aggregate among them) and the 09:30 backup copied and gzipped ten gigabytes more. Load
went 14 -> 21, several processes sat in D state, and **the production monitor produced no
poll for ten minutes after the open**; `rquant-monitor-watchdog` timed out six times
where the two preceding role-free days had timed out none.

Stopping the readers made it worse rather than better: seven of them were inside an
uninterruptible DuckDB read, `TimeoutStopSec=60` expired, systemd killed them, and
`OnFailure` turned an operator's own `systemctl stop` into an alert.

Two claims are asserted here, in package P's world with its real authority chain, its real
wrapper argv and its real five-minute replica:

1. three consecutive replacements of the replica cost one read, not three, and the
   heartbeat says the newer generations were seen and deliberately not read;
2. a role interrupted mid-read by SIGTERM exits within five seconds with code 0 and a
   `stopped` heartbeat, so systemd never reaches `SIGKILL` and never reports a failure.
"""

from __future__ import annotations

import os
import signal
import threading
import time as time_module
from datetime import timedelta
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

import rquant.runtime_service_builtin as builtin_module
import rquant.runtime_service_main as service_main
from rquant.readside_replica_gate import (
    AUCTION_GAP_CANDIDATE_PROFILE,
    DEFAULT_NO_READ_WINDOW,
    NOTIFIER_PAGE_PROJECTION_PROFILE,
)
from rquant.runtime_read_interrupt import (
    READ_INTERRUPT_STOP_REASON,
    READ_INTERRUPTS,
    reset_read_interrupts,
)
from rquant.runtime_service_control import RuntimeServiceControl, RuntimeServiceStatus
from tests.integration.test_route_a_legacy_binding_e2e import PRODUCTION_ROOT
from tests.integration.test_route_a_readside_io_cost_e2e import (
    _StopAfterIterations,
    counted_replica_reads,
)
from tests.integration.test_route_a_readside_replica_e2e import (
    AUCTION_GAP_SERVICE_ID,
    CANDIDATE_ROLE,
    PRIOR_DATES,
    PUBLISH_AT,
    REPLICA_SYNCED_AT,
    ReplicaWorld,
    _instance_name,
    _write_replica,
    locked_main_database,
    session_world,
)

#: re-exported so pytest resolves package P's world, its writer and package Q's counter
__all__ = ["counted_replica_reads", "locked_main_database", "session_world"]

pytestmark = pytest.mark.integration

#: how long a stop may take from the signal to the process returning. The unit's
#: `TimeoutStopSec` is sixty seconds and the owner may raise it; this is the code-side
#: property, and it has to hold whatever the unit says.
_STOP_BUDGET_SECONDS = 5.0

#: a query that will not finish on its own inside the budget, so that "it exited in time"
#: can only mean the interrupt arrived
_ENDLESS = "SELECT count(*) FROM range(400000000000) WHERE range % 7 = 0"


def _run(
    world: ReplicaWorld,
    role: str,
    *,
    instance: str,
    now: Any,
    iterations: int,
    between: Any = None,
) -> Any:
    """One process, `iterations` loop passes, with the wrapper's own argv and environment.

    A trimmed sibling of package Q's `_run_iterations`: it does **not** assert that every
    iteration ran, because half of this file is about a run that stops early on purpose.
    """

    resolved = world.world.resolve(role, instance)
    argv = list(resolved["module_argv"])
    index = argv.index("--control-root") + 1
    argv[index] = str(world.runtime_root / Path(argv[index]).relative_to(PRODUCTION_ROOT))
    arguments = service_main.build_parser().parse_args(argv)

    stop = _StopAfterIterations(iterations)
    if between is not None:
        real_wait = stop.wait

        def wait_and_act(timeout: float | None = None) -> bool:
            outcome = real_wait(timeout)
            between(stop.iterations)
            return outcome

        stop.wait = wait_and_act  # type: ignore[method-assign]

    real_event = service_main.Event
    real_registry = builtin_module.build_builtin_registry
    service_main.Event = lambda: stop  # type: ignore[assignment]
    builtin_module.build_builtin_registry = (  # type: ignore[assignment]
        lambda **kwargs: real_registry(clock=lambda: now, **kwargs)
    )
    try:
        with mock.patch.dict(os.environ, dict(resolved["environment"]), clear=True):
            code = service_main.run(arguments)
    finally:
        service_main.Event = real_event  # type: ignore[assignment]
        builtin_module.build_builtin_registry = real_registry  # type: ignore[assignment]

    control_root = Path(argv[argv.index("--control-root") + 1])
    manifest = next(
        item for item in world.profile.manifests if _instance_name(item.service_id) == instance
    )
    return code, stop.iterations, RuntimeServiceControl.read_heartbeat(
        control_root, manifest.service_spec
    )


# ---------------------------------------------------------------------------------------
# 1. Three replacements, one read
# ---------------------------------------------------------------------------------------


def test_three_consecutive_replica_replacements_cost_one_read(
    session_world: ReplicaWorld,
    locked_main_database: Any,
    counted_replica_reads: dict[str, int],
) -> None:
    """The shape of a trading day: a new ten-gigabyte generation every five minutes.

    `rquant-replica-sync.timer` replaces `rquant_ro.duckdb` every five minutes, so package
    Q's "open it only when the generation changed" is "open it every five minutes" -- three
    opens across the fifteen minutes this run covers, and on 09-14 four roles doing that at
    once on top of the replica `cp`, the monitor's startup scan and the backup is what took
    the monitor off the disk for ten minutes.

    The floor makes it one. The two generations that arrive afterwards are *seen* -- the
    `lstat` still happens, the heartbeat still reports it -- and the answer this role
    already has stands, because prior sessions' `daily_bar` volumes do not change while
    today's session opens.
    """

    replica = session_world.inputs.readonly_replica_database_path
    replacements: list[int] = []

    def replace_every_other_pass(iteration: int) -> None:
        if iteration % 2 or iteration >= 7:
            return
        staged = replica.parent / f"{replica.name}.tmp.{iteration}"
        _write_replica(
            staged,
            trade_dates=PRIOR_DATES,
            #: still before the publisher's clock: a replica stamped after the iteration
            #: that reads it is future evidence and the batch is refused
            synced_at=REPLICA_SYNCED_AT + timedelta(seconds=iteration),
        )
        os.replace(staged, replica)
        replacements.append(iteration)

    instance = _instance_name(AUCTION_GAP_SERVICE_ID)
    code, ran, heartbeat = _run(
        session_world,
        CANDIDATE_ROLE,
        instance=instance,
        now=PUBLISH_AT,
        iterations=8,
        between=replace_every_other_pass,
    )

    assert code == 0
    assert ran == 8
    assert replacements == [2, 4, 6], "the harness must have replaced the replica three times"
    assert counted_replica_reads["auction_gap"] == 1
    assert heartbeat is not None
    assert heartbeat.total_successes == 8
    assert heartbeat.degraded_reasons == ()
    assert heartbeat.replica_opened is False
    assert heartbeat.replica_skipped_by_floor is True


def test_the_floor_is_the_roles_own_window_and_the_notifier_carries_the_open_window() -> None:
    """What each of the four profiles says, asserted rather than left to the docstring.

    The no-read window is the notifier's alone, and that is a decision rather than an
    oversight: `reference-slow.source.v1` captures inside 09:20-09:25 and
    `candidate.auction_gap.v1` assembles inside 09:26-09:30, both *inside* 09:20-09:40. A
    blanket window would not slow those two down, it would stop them working.
    """

    from datetime import UTC, datetime

    from rquant.readside_replica_gate import (
        AUCTION_UNIVERSE_PUBLISHER_PROFILE,
        REFERENCE_SLOW_SOURCE_PROFILE,
    )

    assert NOTIFIER_PAGE_PROJECTION_PROFILE.min_reread_interval == timedelta(minutes=15)
    assert NOTIFIER_PAGE_PROJECTION_PROFILE.no_read_window == DEFAULT_NO_READ_WINDOW
    #: 01:30 UTC is 09:30 in the market clock `may_fetch_market_minute` is decided in
    assert NOTIFIER_PAGE_PROJECTION_PROFILE.suspends_reads_at(
        datetime(2026, 9, 14, 1, 30, tzinfo=UTC)
    )
    assert not NOTIFIER_PAGE_PROJECTION_PROFILE.suspends_reads_at(
        datetime(2026, 9, 14, 1, 40, tzinfo=UTC)
    )

    windowed = (
        REFERENCE_SLOW_SOURCE_PROFILE,
        AUCTION_GAP_CANDIDATE_PROFILE,
        AUCTION_UNIVERSE_PUBLISHER_PROFILE,
    )
    for profile in windowed:
        assert profile.no_read_window is None
        assert profile.min_reread_interval > timedelta(0)
    assert AUCTION_GAP_CANDIDATE_PROFILE.min_reread_interval == timedelta(minutes=4)


# ---------------------------------------------------------------------------------------
# 2. A stop that arrives during a read
# ---------------------------------------------------------------------------------------


class _StallingConnection:
    """The role's own connection, with one query in front of its first real one.

    Nothing of the role's code changes: it opens the replica through `duckdb.connect` as
    always, holds it in the interrupt registry as always, and converts `duckdb.Error` into
    its own integrity error as always. Only the query is long enough to be caught inside.
    """

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.stalled = threading.Event()

    def interrupt(self) -> None:
        self._inner.interrupt()

    def execute(self, *args: Any, **kwargs: Any) -> Any:
        if not self.stalled.is_set():
            self.stalled.set()
            self._inner.execute(_ENDLESS).fetchall()
        return self._inner.execute(*args, **kwargs)

    def close(self) -> None:
        self._inner.close()


@pytest.fixture
def stalling_replica_read(monkeypatch: pytest.MonkeyPatch) -> _StallingConnection:
    """Make the auction-gap publisher's own replica read take longer than any stop budget."""

    import duckdb

    holder: dict[str, _StallingConnection] = {}
    original = duckdb.connect

    def connect(*args: Any, **kwargs: Any) -> Any:
        connection = original(*args, **kwargs)
        if "read_only" not in kwargs:
            return connection
        stalling = _StallingConnection(connection)
        holder.setdefault("connection", stalling)
        return stalling

    monkeypatch.setattr(duckdb, "connect", connect)
    reset_read_interrupts()
    yield holder  # type: ignore[misc]
    reset_read_interrupts()


def test_a_role_interrupted_mid_read_exits_in_time_with_code_zero(
    session_world: ReplicaWorld,
    locked_main_database: Any,
    stalling_replica_read: Any,
) -> None:
    """SIGTERM during a database read: gone in seconds, exit 0, heartbeat `stopped`.

    Before this, the query held the main thread inside C and the Python handler that sets
    `stop_event` did not run at all until it returned; on 09-14 seven roles were in that
    state at once, every one of them outlived `TimeoutStopSec=60`, and every `SIGKILL` was
    reported as `Result=timeout` and relayed as an alert.

    `_ENDLESS` does not finish, so "it exited" can only mean the interrupt arrived, and
    "exit 0 with a `stopped` heartbeat" can only mean it took the clean path out.
    """

    sent_at: dict[str, float] = {}

    def signal_once_the_read_has_started() -> None:
        deadline = time_module.monotonic() + 30.0
        while time_module.monotonic() < deadline:
            stalling = stalling_replica_read.get("connection")
            if stalling is not None and stalling.stalled.is_set() and READ_INTERRUPTS.open_reads:
                sent_at["monotonic"] = time_module.monotonic()
                os.kill(os.getpid(), signal.SIGTERM)
                return
            time_module.sleep(0.02)

    watcher = threading.Thread(target=signal_once_the_read_has_started, daemon=True)
    instance = _instance_name(AUCTION_GAP_SERVICE_ID)

    watcher.start()
    code, ran, heartbeat = _run(
        session_world,
        CANDIDATE_ROLE,
        instance=instance,
        now=PUBLISH_AT,
        iterations=4,
    )
    returned_at = time_module.monotonic()
    watcher.join(timeout=5.0)

    assert sent_at, "the harness never caught the role inside its read"
    #: signal to process return, which is what `TimeoutStopSec` is counting
    elapsed = returned_at - sent_at["monotonic"]
    assert code == 0, "an interrupted read must not make the process exit nonzero"
    assert ran < 4, "the loop must stop on the signal rather than run every iteration"
    assert elapsed < _STOP_BUDGET_SECONDS, f"the stop took {elapsed:.2f}s"
    assert heartbeat is not None
    assert heartbeat.status is RuntimeServiceStatus.STOPPED
    assert heartbeat.stop_reason == READ_INTERRUPT_STOP_REASON
    #: the abandoned round is not recorded as a fault an operator caused
    assert heartbeat.total_failures == 0
    assert heartbeat.last_error is None
