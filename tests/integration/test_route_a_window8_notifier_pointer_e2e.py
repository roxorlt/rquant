"""#260 acceptance: the signals pointer the notifier's own previous generation left.

The eighth Route A window installed the seventh bundle (`9eece6ad…`, producer_commit
`1025b12`) over the sixth (`1aebc325…`, `3cdfa22`) on 2026-09-12, and from 09:19
`notifier.admin.shadow.v1` was DEGRADED every two seconds with

    ServingSourceAuthorityIntegrityError: current pointer producer_commit does not
    match expected commit

This is #253's shape on a second reader. The signals serving authority belongs to the
notifier, which both publishes and reads it, and a release does not republish it: the
`current.json` on disk after a handover still carries the previous generation's commit.
Package O (PR #257) gave `serving.publisher.v1` the shared `producer_commit_lineage`
predicate for that very file; this role compared against its own commit and refused.

The failure was silent -- degraded, inside the main loop, no `OnFailure`, zero pushes --
and it produced no page projection at all until serving published a generation-7 pointer,
so across the weekend the window was red end to end.

This file is that window in package L's two-generation install world. The pointer is
written by the role that owns it, running under the previous generation's commit, and the
reader is started through the wrapper's own argv inside its own unit's sandbox. The
negative half carries the same weight: a commit no generation of ours ever ran is still
refused, word for word.

The side observation is here too. On the failing path the heartbeat reported
`replica_opened=null`, and that was wrong rather than absent: this role publishes its page
projection *before* it touches the serving authority on every return path, so the
iteration that failed had opened the 10 GB read-only replica and read it. The failed round
now summarizes the gate exactly as a successful one does (package Q MF-1, SF-7).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from rquant.runtime_service_entrypoint import RuntimeServiceKind
from rquant.runtime_serving_authority import ServingSourceAuthorityPublisher
from tests.integration.test_route_a_all_roles_sandbox_e2e import (
    cold_chain,  # noqa: F401 -- the two-generation fixture, reused verbatim
    credentials_root,  # noqa: F401 -- the notifier carries a `LoadCredentialEncrypted=`
    instance_of,
    run_role,
)
from tests.integration.test_route_a_generation_handover_e2e import (
    first_generation_manifest,
    manifests_of,
)
from tests.integration.test_route_a_legacy_binding_e2e import RouteAWorld
from tests.integration.test_route_a_live_chain_idle_e2e import FROZEN_NOW
from tests.integration.test_route_a_window7_gaps_e2e import (
    NOTIFIER_ROLE,
    STRATEGY_ROLE,
    _signals_result,
    write_first_generation_signals_pointer,
)

pytestmark = pytest.mark.integration

REFUSAL = "current pointer producer_commit does not match expected commit"
FOREIGN_COMMIT = "e" * 40


def notifier_projection_replica(route: RouteAWorld) -> Path:
    """The read-only replica this role polls every two seconds, with the tables it reads.

    Package L's world leaves this file absent, and #255's test writes a DuckDB with one
    unrelated table so the schema audit is the next thing that can fail. Here the audit has
    to *pass*: the page projection is published before the serving authority, so a
    projection that raises would stand in front of the pointer this file is about and the
    window's actual failure would never be reached.
    """

    import duckdb

    notifier = manifests_of(route, RuntimeServiceKind.NOTIFIER)[0]
    database = Path(str(notifier.settings["page_projection_database_path"]))
    database.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(database)) as connection:
        connection.execute(
            """
            CREATE TABLE screen_result (
                trade_date DATE, preset_name VARCHAR, ts_code VARCHAR, name VARCHAR,
                close DOUBLE, pct_chg DOUBLE, extra JSON, created_at TIMESTAMP
            );
            INSERT INTO screen_result VALUES
              ('2026-08-03', 'n-shape-pool1', '600000.SH', 'PF', 10.6, 6, '{}',
               '2026-08-03 10:05:00');
            CREATE TABLE minute_bar (
                ts_code VARCHAR, trade_time TIMESTAMP, freq VARCHAR, open DOUBLE,
                high DOUBLE, low DOUBLE, close DOUBLE, vol DOUBLE, amount DOUBLE,
                source VARCHAR, created_at TIMESTAMP
            );
            INSERT INTO minute_bar VALUES
              ('600000.SH', '2026-08-03 09:30:00', '1min', 10, 10, 10, 10,
               100, 1000, 'tushare', '2026-08-03 09:31:00');
            """
        )
        connection.execute("CHECKPOINT")
    database.chmod(0o600)
    return database


def run_notifier(route: RouteAWorld, credentials: dict[str, Path]) -> object:
    """The notifier, after the two roles that create the spool it opens (runbook C-3)."""

    for strategy in instance_of(route, STRATEGY_ROLE):
        run_role(route, STRATEGY_ROLE, instance=strategy)
    run_role(route, "signal_router", instance=instance_of(route, "signal_router")[0])
    instance = instance_of(route, NOTIFIER_ROLE)[0]
    return run_role(route, NOTIFIER_ROLE, instance=instance, credentials=credentials[instance])


def test_the_notifier_carries_the_previous_generations_signals_pointer(
    cold_chain: RouteAWorld,  # noqa: F811
    credentials_root: dict[str, Path],  # noqa: F811
) -> None:
    """#260: a generation-6 pointer read by a generation-7 notifier, in the real world.

    `write_first_generation_signals_pointer` publishes `current.json` under the commit the
    *first* installed generation carried, which is the state every release leaves behind.
    The role then has to reach its main loop, find the pointer readable, and publish a page
    projection -- none of which it did for the whole eighth window.
    """

    notifier_projection_replica(cold_chain)
    root, previous_commit = write_first_generation_signals_pointer(cold_chain)
    notifier = manifests_of(cold_chain, RuntimeServiceKind.NOTIFIER)[0]
    assert previous_commit != notifier.producer_commit
    assert previous_commit in (root / "current.json").read_text(encoding="utf-8")

    run = run_notifier(cold_chain, credentials_root)

    assert run.entered, run
    assert REFUSAL not in (run.last_error or ""), run
    assert run.violations == [], run.violations
    #: the pointer is still the one generation 6 wrote -- this role carries it and
    #: replaces it on its own next publish, it does not rewrite somebody's past
    assert previous_commit in (root / "current.json").read_text(encoding="utf-8")


def test_the_carried_iteration_reports_what_it_did_with_the_replica(
    cold_chain: RouteAWorld,  # noqa: F811
    credentials_root: dict[str, Path],  # noqa: F811
) -> None:
    """The heartbeat of the iteration that now gets through: `(True, bytes)`, not `null`."""

    notifier_projection_replica(cold_chain)
    write_first_generation_signals_pointer(cold_chain)

    run = run_notifier(cold_chain, credentials_root)

    assert run.entered, run
    assert run.replica_opened is True, run
    assert run.replica_read_bytes is None or run.replica_read_bytes >= 0, run


def test_a_signals_pointer_from_no_generation_of_ours_still_stops_the_notifier(
    cold_chain: RouteAWorld,  # noqa: F811
    credentials_root: dict[str, Path],  # noqa: F811
) -> None:
    """The negative half: only our own past is carried, and the refusal is unchanged."""

    notifier_projection_replica(cold_chain)
    notifier = manifests_of(cold_chain, RuntimeServiceKind.NOTIFIER)[0]
    root = Path(str(notifier.settings["serving_authority_root"]))
    ServingSourceAuthorityPublisher(
        root=root,
        producer_commit=FOREIGN_COMMIT,
        dataset_id="signals",
        payload_kind="signal_delivery",
        clock=lambda: FROZEN_NOW,
    ).publish(_signals_result())

    run = run_notifier(cold_chain, credentials_root)

    assert run.entered, run
    assert REFUSAL in (run.last_error or ""), run
    #: the loop records the failure and keeps going, which is what made the window silent;
    #: `run_role` stops it after the one iteration, so the final status is `stopped`
    assert run.exit_code == 0, run


def test_the_degraded_iteration_still_summarizes_the_replica_gate(
    cold_chain: RouteAWorld,  # noqa: F811
    credentials_root: dict[str, Path],  # noqa: F811
) -> None:
    """#260's side observation, on the one path that is still allowed to fail.

    A foreign pointer is refused after the page projection has already been published, so
    this iteration opened the replica and then raised -- which is exactly the shape the
    eighth window's every iteration had. `replica_opened=null` said "this role cannot say";
    the truth is `true`, and package Q's SF-7 already holds that a loader which raises
    part-way still counts as an open.
    """

    notifier_projection_replica(cold_chain)
    notifier = manifests_of(cold_chain, RuntimeServiceKind.NOTIFIER)[0]
    ServingSourceAuthorityPublisher(
        root=Path(str(notifier.settings["serving_authority_root"])),
        producer_commit=FOREIGN_COMMIT,
        dataset_id="signals",
        payload_kind="signal_delivery",
        clock=lambda: FROZEN_NOW,
    ).publish(_signals_result())

    run = run_notifier(cold_chain, credentials_root)

    assert REFUSAL in (run.last_error or ""), run
    assert run.replica_opened is True, run
    assert run.replica_read_bytes is None or run.replica_read_bytes >= 0, run


def test_the_first_generation_manifest_is_the_one_that_wrote_the_pointer(
    cold_chain: RouteAWorld,  # noqa: F811
) -> None:
    """What makes the positive half evidence rather than a fixture.

    The pointer above is published under the producer_commit of the manifest the *first*
    install left in the generation tree, and that tree is what `producer_commit_lineage`
    reads: the generation directory is named by `canonical_sha256` of its own basis and the
    basis records the sha256 of this service's manifest. Nothing here is asserted about a
    commit this world did not really install.
    """

    notifier = manifests_of(cold_chain, RuntimeServiceKind.NOTIFIER)[0]
    previous = first_generation_manifest(cold_chain, notifier.service_id)

    assert previous.service_id == notifier.service_id
    assert previous.producer_commit != notifier.producer_commit
    assert previous.producer_commit != FOREIGN_COMMIT
