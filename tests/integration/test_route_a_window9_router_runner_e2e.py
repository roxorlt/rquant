"""#263 acceptance: the router against three strategies that were stopped cleanly.

The ninth Route A window ran v0.33.9 (`7947eae`, authority sequence 7) on 2026-09-12.
The three `rquant-runtime-strategy@` units were stopped cleanly at 18:22-18:23 under the
runbook's R-29 blocking-stop discipline, `signal_router` was started at 18:33, and at
18:43:38 it exited 1 with

    sqlite3.OperationalError: unable to open database file
    ValueError: runner source schema is unavailable

`OnFailure=rquant-alert@%n.service` sent **one real push** (2 of 3 channels), `Restart=`
brought the unit back at 18:43:52, and the two strategies that had been idle rotated new
runners at 18:44:35 and 18:44:44 -- one iteration after the exit.

A clean SQLite close checkpoints a WAL database and removes `-wal`/`-shm`; a read-only
open of a WAL database then has to *create* the `-shm` wal-index, and this role mounts
`live/strategies` read-only. So the words the router said were about a file that was
perfectly intact and whose owner was simply not running -- which is what an absent
`runner.sqlite3` has meant since #232, what a runner still carrying our own previous
generation's identity has meant since #248, and what package O answered for the
strategy's own view of a stopped paper broker in #252. The eighth window missed this only
because the generation-6 runners had been SIGKILLed and still carried their sidecars.

This file is that window in package L's two-generation install world: the three strategies
create their runners by running, the runners are left exactly as a clean stop leaves them,
their directories are made read-only the way the unit makes them, and the router is started
through the wrapper's own argv inside its own sandbox. The negative half carries the same
weight -- a runner that is present *with* its sidecars and malformed still stops the
router, with the wording the window's journal carried.
"""

from __future__ import annotations

import gc
import sqlite3
from pathlib import Path

import pytest

from rquant.runtime_peer_artifacts import PeerArtifactUnavailableError
from tests.integration.test_route_a_all_roles_sandbox_e2e import (
    cold_chain,  # noqa: F401 -- the two-generation fixture, reused verbatim
    instance_of,
    run_role,
)
from tests.integration.test_route_a_legacy_binding_e2e import RouteAWorld
from tests.integration.test_route_a_window7_gaps_e2e import STRATEGY_ROLE
from tests.runtime_readonly_sandbox import tree_state

pytestmark = pytest.mark.integration

ROUTER_ROLE = "signal_router"

#: the sentence the window's journal carried, which ruling 29.2 keeps for real faults
SCHEMA_REFUSAL = "runner source schema is unavailable"


def runner_databases(route: RouteAWorld) -> list[Path]:
    return sorted((route.runtime_root / "live" / "strategies").glob("*/runner.sqlite3"))


def stopped_strategy_runners(route: RouteAWorld) -> list[Path]:
    """The three runners their owners created, left the way a clean stop leaves them.

    The strategies create `runner.sqlite3` while their step is being built (#232), which
    is why running each of them once is the whole setup. Dropping every connection then
    checkpoints and removes the sidecars, which is what `systemctl stop` does on the host.
    """

    for instance in instance_of(route, STRATEGY_ROLE):
        run = run_role(route, STRATEGY_ROLE, instance=instance)
        assert run.entered, run
    gc.collect()
    databases = runner_databases(route)
    assert len(databases) == 3, databases
    for path in databases:
        connection = sqlite3.connect(path)
        try:
            assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        finally:
            connection.close()
    gc.collect()
    for path in databases:
        assert not path.with_name(f"{path.name}-wal").exists(), path
        assert not path.with_name(f"{path.name}-shm").exists(), path
    return databases


def as_read_only(databases: list[Path]) -> None:
    """`live/strategies/<svc>/` the way the router's unit grants it: read-only."""

    for path in databases:
        path.parent.chmod(0o500)


def as_writable(databases: list[Path]) -> None:
    for path in databases:
        path.parent.chmod(0o700)


def test_the_router_waits_for_three_cleanly_stopped_strategy_runners(
    cold_chain: RouteAWorld,  # noqa: F811
) -> None:
    """#263: the exit and the push this replaces, with the wait it becomes instead."""

    databases = stopped_strategy_runners(cold_chain)
    before = {path.parent: tree_state(path.parent) for path in databases}
    as_read_only(databases)
    try:
        run = run_role(cold_chain, ROUTER_ROLE, instance=instance_of(cold_chain, ROUTER_ROLE)[0])
    finally:
        as_writable(databases)

    #: it reached its main loop instead of exiting 1 out of the step being built
    assert run.entered, run.traceback
    assert run.refusal is None, run.traceback
    #: and the iteration says which file it is waiting on, which is the heartbeat field
    #: `rquant-runtime-ready` and the operator both read
    assert run.waiting_for is not None, run
    assert run.waiting_for.endswith("runner.sqlite3"), run.waiting_for
    assert run.waiting_for in {str(path) for path in databases}, run.waiting_for
    assert SCHEMA_REFUSAL not in (run.last_error or ""), run
    assert "-wal/-shm" in (run.last_error or ""), run
    assert run.violations == [], run.violations
    #: and it really did leave the strategies' directories alone: no wal-index anywhere,
    #: which is the write `live/strategies` being read-only would have refused on the host
    assert {path.parent: tree_state(path.parent) for path in databases} == before


def test_the_next_round_routes_once_the_strategies_reopened_their_runners(
    cold_chain: RouteAWorld,  # noqa: F811
) -> None:
    """The wait ends by itself: the sidecars come back when the owner opens the file.

    Same world, same read-only directories, same router: only the strategies' own
    connections are added, which is what 18:44:35 and 18:44:44 were on the host.
    """

    databases = stopped_strategy_runners(cold_chain)
    as_read_only(databases)
    try:
        waiting = run_role(
            cold_chain,
            ROUTER_ROLE,
            instance=instance_of(cold_chain, ROUTER_ROLE)[0],
        )
    finally:
        as_writable(databases)
    assert waiting.entered, waiting.traceback
    assert waiting.waiting_for is not None, waiting

    #: the three strategies running again: each one's own connection is what keeps the
    #: wal-index beside its database
    owners = [sqlite3.connect(path) for path in databases]
    try:
        for owner, path in zip(owners, databases, strict=True):
            owner.execute("SELECT COUNT(*) FROM runner_signal").fetchone()
            assert path.with_name(f"{path.name}-shm").exists(), path
        as_read_only(databases)
        try:
            routed = run_role(
                cold_chain,
                ROUTER_ROLE,
                instance=instance_of(cold_chain, ROUTER_ROLE)[0],
            )
        finally:
            as_writable(databases)
    finally:
        for owner in owners:
            owner.close()

    assert routed.entered, routed.traceback
    assert routed.refusal is None, routed.traceback
    assert routed.waiting_for is None, routed
    assert routed.last_error is None, routed
    assert routed.violations == [], routed.violations
    #: the route spool the broker and the notifier read exists, which is the whole point
    #: of the router having got through its iteration
    assert (cold_chain.runtime_root / "live" / "signal-bus").is_dir()


def test_a_malformed_runner_that_has_its_sidecars_still_stops_the_router(
    cold_chain: RouteAWorld,  # noqa: F811
) -> None:
    """Ruling 29.2: present, openable and wrong is a fault, word for word as before.

    The owner is holding the database open, so the sidecars are there and the read-only
    open succeeds -- and then the identity table the strategy always creates is gone. That
    is not a peer to wait for under any reading, and the router must still refuse to start.
    """

    databases = stopped_strategy_runners(cold_chain)
    owner = sqlite3.connect(databases[0])
    try:
        owner.execute("DROP TABLE runner_source_identity")
        owner.commit()
        assert databases[0].with_name(f"{databases[0].name}-shm").exists()
        as_read_only(databases)
        try:
            run = run_role(
                cold_chain,
                ROUTER_ROLE,
                instance=instance_of(cold_chain, ROUTER_ROLE)[0],
            )
        finally:
            as_writable(databases)
    finally:
        owner.close()

    assert not run.entered, run
    assert run.waiting_for is None, run
    assert isinstance(run.refusal, ValueError), run.traceback
    assert not isinstance(run.refusal, PeerArtifactUnavailableError), run.traceback
    assert str(run.refusal) == SCHEMA_REFUSAL, run.traceback


def test_a_runner_whose_header_is_not_sqlites_still_stops_the_router(
    cold_chain: RouteAWorld,  # noqa: F811
) -> None:
    """The other half of the negative: no sidecars, read-only directory, and not a database.

    This is the shape the wait must never swallow. Everything about it looks like the
    window's state except the sixteen bytes that say what the file is, and those are the
    reason the judgement reads the header off disk instead of reading SQLite's message.
    """

    databases = stopped_strategy_runners(cold_chain)
    payload = bytearray(databases[0].read_bytes())
    payload[:16] = b"NotSQLite fmt 3\x00"
    databases[0].write_bytes(bytes(payload))
    as_read_only(databases)
    try:
        run = run_role(cold_chain, ROUTER_ROLE, instance=instance_of(cold_chain, ROUTER_ROLE)[0])
    finally:
        as_writable(databases)

    assert not run.entered, run
    assert run.waiting_for is None, run
    assert isinstance(run.refusal, ValueError), run.traceback
    assert not isinstance(run.refusal, PeerArtifactUnavailableError), run.traceback
