"""#256 acceptance: a read-side role that runs all day without re-reading 10 GB all day.

On 2026-09-08 and 2026-09-09 the 17:00 daily pipeline stalled in its `daily_state` stage
while the runtime roles were running, and finished within a minute of their being stopped.
Memory was not the constraint -- 9 GB free on 09-09 -- the page cache was: four roles read
`data/rquant_ro.duckdb` (about 10 GB) on every loop iteration, the notifier's every two
seconds, and with the 15-minute backup and the 5-minute replica copy the cache could not
hold both databases, so the daily's own scans fell to disk.

Package P's world is what this runs in: two installed generations, a real staged and
published authority chain, the wrapper's own argv and child environment, a market calendar
that opens the session, a real five-minute replica carrying the five prior sessions'
`daily_bar` rows, and a real second DuckDB connection holding the main database in write
mode for the whole test. What this file adds is *more than one iteration in one process*,
because "read it only when it changed" is a claim about the second iteration and package
P's harness stops after the first.
"""

from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path
from threading import Event
from typing import Any
from unittest import mock

import pytest

import rquant.auction_gap_candidate_input as auction_gap_module
import rquant.auction_universe_source as auction_universe_module
import rquant.runtime_service_builtin as builtin_module
import rquant.runtime_service_main as service_main
from rquant.runtime_service_control import RuntimeServiceControl
from tests.integration.test_route_a_all_roles_sandbox_e2e import ROLE_UNITS, sandbox_of
from tests.integration.test_route_a_legacy_binding_e2e import PRODUCTION_ROOT
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

#: re-exported so pytest resolves package P's world and its writer in this module
__all__ = ["locked_main_database", "session_world"]

pytestmark = pytest.mark.integration

AUCTION_UNIVERSE_SERVICE_ID = "auction-universe.publisher.v1"
UNIVERSE_ROLE = "auction_universe_publisher"

#: the four roles the production profile binds to the replica (#250)
READ_SIDE_ROLES = (
    "auction_universe_publisher",
    "candidate_publisher",
    "notifier",
    "reference_slow_source",
)


class _StopAfterIterations(Event):
    """Let the loop run exactly `limit` steps, then stop without waiting out the interval.

    Package P's `_StopAfterOneIteration` proves a role entered its loop. This one keeps the
    *same process and the same step closure* alive for several iterations, which is the
    only place a "did this iteration have to open the database" claim can be observed: the
    gate is per-run in-memory state, so a harness that restarts the process between
    iterations measures nothing.
    """

    def __init__(self, limit: int) -> None:
        super().__init__()
        self.iterations = 0
        self.limit = limit

    def is_set(self) -> bool:
        return super().is_set() or self.iterations >= self.limit

    def wait(self, timeout: float | None = None) -> bool:  # noqa: ARG002 - not a sleep
        self.iterations += 1
        if self.iterations >= self.limit:
            self.set()
        return True


def _run_iterations(
    world: ReplicaWorld,
    role: str,
    *,
    instance: str,
    now: Any,
    iterations: int,
    between: Any = None,
) -> Any:
    """One process, `iterations` loop passes, with the wrapper's own argv and environment.

    `between` runs after each pass, which is where the replica is replaced.
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

    assert stop.iterations == iterations, f"{role} ran {stop.iterations} of {iterations}"
    control_root = Path(argv[argv.index("--control-root") + 1])
    manifest = next(
        item for item in world.profile.manifests if _instance_name(item.service_id) == instance
    )
    return code, RuntimeServiceControl.read_heartbeat(control_root, manifest.service_spec)


@pytest.fixture
def counted_replica_reads(monkeypatch: pytest.MonkeyPatch) -> dict[str, int]:
    """Every place a read-side role actually opens the replica, counted."""

    counts = {"auction_gap": 0, "auction_universe": 0}
    original_gap = auction_gap_module._query_daily_volume_rows
    original_universe = auction_universe_module._query_codes

    def counted_gap(*args: object, **kwargs: object) -> object:
        counts["auction_gap"] += 1
        return original_gap(*args, **kwargs)

    def counted_universe(*args: object, **kwargs: object) -> object:
        counts["auction_universe"] += 1
        return original_universe(*args, **kwargs)

    monkeypatch.setattr(auction_gap_module, "_query_daily_volume_rows", counted_gap)
    monkeypatch.setattr(auction_universe_module, "_query_codes", counted_universe)
    return counts


# ---------------------------------------------------------------------------------------
# The rule: read it when it changed, and only then
# ---------------------------------------------------------------------------------------


def test_the_auction_gap_publisher_reads_the_replica_once_over_four_iterations(
    session_world: ReplicaWorld,
    locked_main_database: Any,
    counted_replica_reads: dict[str, int],
) -> None:
    """Four passes of the 09:26-09:30 window over one generation, one read.

    At the manifest's five-second interval the window is about 48 passes; before this
    every one of them queried the replica's whole `daily_bar`.
    """

    instance = _instance_name(AUCTION_GAP_SERVICE_ID)
    code, heartbeat = _run_iterations(
        session_world,
        CANDIDATE_ROLE,
        instance=instance,
        now=PUBLISH_AT,
        iterations=4,
    )

    assert code == 0
    assert counted_replica_reads["auction_gap"] == 1
    assert heartbeat is not None
    assert heartbeat.total_successes == 4
    assert heartbeat.degraded_reasons == ()
    #: the last iteration recognised the generation and did not open the database
    assert heartbeat.replica_opened is False


def test_an_atomic_replacement_costs_exactly_one_more_read(
    session_world: ReplicaWorld,
    locked_main_database: Any,
    counted_replica_reads: dict[str, int],
) -> None:
    """`sync-readonly-replica.sh` `mv`s a new file over the name every five minutes.

    One read for the generation the run started on, one for the generation that replaced
    it, and nothing for the three iterations that followed either of them.
    """

    replica = session_world.inputs.readonly_replica_database_path
    staged = replica.parent / f"{replica.name}.tmp.1"

    def replace_after_the_third(iteration: int) -> None:
        if iteration != 3:
            return
        _write_replica(
            staged,
            trade_dates=PRIOR_DATES,
            #: still before the publisher's clock: a replica stamped after the
            #: iteration that reads it is future evidence and the batch is refused
            synced_at=REPLICA_SYNCED_AT + timedelta(minutes=1),
        )
        os.replace(staged, replica)

    instance = _instance_name(AUCTION_GAP_SERVICE_ID)
    code, heartbeat = _run_iterations(
        session_world,
        CANDIDATE_ROLE,
        instance=instance,
        now=PUBLISH_AT,
        iterations=6,
        between=replace_after_the_third,
    )

    assert code == 0
    assert counted_replica_reads["auction_gap"] == 2
    assert heartbeat is not None
    assert heartbeat.total_successes == 6
    assert heartbeat.degraded_reasons == ()
    assert heartbeat.replica_opened is False


def test_the_auction_universe_publisher_reads_the_replica_once_over_three_iterations(
    session_world: ReplicaWorld,
    locked_main_database: Any,
    counted_replica_reads: dict[str, int],
) -> None:
    """The second read-side publisher, at its own thirty-second interval."""

    instance = _instance_name(AUCTION_UNIVERSE_SERVICE_ID)
    code, heartbeat = _run_iterations(
        session_world,
        UNIVERSE_ROLE,
        instance=instance,
        now=PUBLISH_AT,
        iterations=3,
    )

    assert code == 0
    assert counted_replica_reads["auction_universe"] <= 1
    assert heartbeat is not None
    assert heartbeat.total_successes == 3
    assert heartbeat.last_error is None


def test_the_heartbeat_says_what_the_last_iteration_did_with_the_replica(
    session_world: ReplicaWorld,
    locked_main_database: Any,
    counted_replica_reads: dict[str, int],
) -> None:
    """The measurement ruling 25 asks for, on the file model, read back off disk.

    An iteration that did not open the database read no bytes on any platform, so this
    one is exactly zero. `rchar` from `/proc/self/io` is what fills the field in on an
    iteration that *did* open it, and `None` on a platform that will not say.
    """

    instance = _instance_name(AUCTION_GAP_SERVICE_ID)
    _code, heartbeat = _run_iterations(
        session_world,
        CANDIDATE_ROLE,
        instance=instance,
        now=PUBLISH_AT,
        iterations=2,
    )

    assert heartbeat is not None
    assert heartbeat.replica_opened is False
    assert heartbeat.replica_read_bytes == 0
    #: and it is a file field: nothing here reaches what serving publishes (#237)
    from rquant.runtime_service_control import RuntimeServiceHeartbeatProjection

    projected = RuntimeServiceHeartbeatProjection.from_heartbeat(heartbeat)
    assert not hasattr(projected, "replica_opened")


# ---------------------------------------------------------------------------------------
# What this package did not need: a path
# ---------------------------------------------------------------------------------------


def test_no_read_side_role_needs_a_new_writable_path_for_any_of_this(
    session_world: ReplicaWorld,
) -> None:
    """The gate remembers in memory, so no unit under `deploy/systemd/` changes.

    Every writable grant the four replica-reading roles hold is inside the runtime root,
    and none of them may write the directory the replica lives in -- which is what a
    cache on disk beside the database, or a copy of it, would have required.
    """

    replica_directory = session_world.inputs.readonly_replica_database_path.parent

    for role in READ_SIDE_ROLES:
        assert role in ROLE_UNITS, role
        instance = _instance_name(
            AUCTION_GAP_SERVICE_ID if role == "candidate_publisher" else role
        )
        sandbox = sandbox_of(role, instance=instance, runtime_root=PRODUCTION_ROOT)
        writable = sandbox["ReadWritePaths"]
        assert writable, f"{role} declares no writable path at all"
        for granted in writable:
            assert PRODUCTION_ROOT in granted.parents or granted == PRODUCTION_ROOT, (
                f"{role} may write {granted}, which is outside the runtime root"
            )
            assert replica_directory not in granted.parents
            assert granted != replica_directory
