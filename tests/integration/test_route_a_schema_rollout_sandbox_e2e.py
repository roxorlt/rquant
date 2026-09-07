"""#227 acceptance: two bundle generations, a read-only rollout root, real roles in loops.

The second Route A window installed the first deployment generation that had a predecessor,
so `install_runtime_deployment_profile` prepared a schema rollout for every channel whose
declaration fingerprint moved — which is every channel with both a producer and a consumer,
because a declaration fingerprint carries the producer commit. From then on every kind-backed
role reached `load_runtime_schema_service_bindings`, which walks `control/schema-rollouts`
and opens each plan's state store *before* it knows whether this service is in the plan.
`SchemaRolloutStore` had one open: `mkdir(parents=True)` plus an unconditional
`PRAGMA journal_mode = WAL`. A runtime unit runs `ProtectSystem=strict`,
`ProtectHome=read-only` and a `ReadWritePaths` that never lists `control/schema-rollouts`, so
the wal-index it wanted to create was refused and all eight started units went into a restart
loop behind `sqlite3.OperationalError: unable to open database file`.

The existing rollout e2e could not see any of this, because it runs on a runtime root the
test process owns and may write. So this file makes the root read-only and keeps everything
else real:

* two generations are really installed — `install_runtime_deployment_profile` twice over the
  real production profile, the second with no bootstrap reason, which is what makes its
  receipt carry a `previous_generation_hash` and prepare the rollout plans;
* `control/schema-rollouts` is then stripped of every write bit, directories included, which
  is what the unit sandbox does to it and what no other test does;
* the authority chain is really staged and published over that runtime root, and the
  wrapper's own `resolve_launch` derives both the argv and the allowlisted child environment;
* `runtime_service_main.run()` is what runs, with `os.environ` replaced by that environment
  and nothing else, and it enters the real service loop for one iteration.

The two roles are `serving_publisher` and `watchlist_quote_source` — two of the eight units
that flapped on the host, and the two the sandbox can serve without any write at all: the
first consumes five channels and produces none, the second produces a channel with no
consumer, so no plan names it. What the other six need is the subject of the last case in
this file, which pins the residual rather than papering over it.

Seams are package A's and package E's, unchanged: the module constants that name
`/etc/rquant`, `/var/lib/rquant` and the system interpreter, the `os.stat` hook that lets a
non-root test own a 0444 keyring, the two credential-sealing calls that need `systemd-creds`
under sudo, and the frozen `--control-root` prefix that no test can own.
"""

from __future__ import annotations

import os
import sqlite3
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

import rquant.runtime_service_main as service_main
from rquant.runtime_deployment_bundle import (
    RuntimeSchemaCompatibilityError,
    load_runtime_schema_rollout,
)
from rquant.schema_compatibility import (
    RolloutPhase,
    SchemaRolloutStateUnavailableError,
    SchemaRolloutStore,
)
from tests.integration.test_route_a_legacy_binding_e2e import (
    PRODUCTION_ROOT,
    RouteAWorld,
    _production_bundle,
    _StopAfterOneIteration,
)
from tests.unit.test_runtime_authority_publish import World

pytestmark = pytest.mark.integration

#: The generation the rollout moves away from. Any commit but the chain's own will do: a
#: declaration fingerprint is `semantic_fingerprint + producer_commit`, so a different commit
#: is exactly what makes every two-sided channel a changed channel and gives the second
#: install something to prepare a rollout for. This is the host's situation too — window one
#: installed `7d572c79…` at one commit and window two `bf2da6d8…` at another.
PREVIOUS_COMMIT = "1e2d3c4b5a69788796a5b4c3d2e1f00918273645"

#: Two of the eight units that flapped, and the two that need no write to be admitted.
READ_ONLY_ROLES = ("serving_publisher", "watchlist_quote_source")

#: One that does: it produces `runtime.intraday_feature.batch-envelope`, whose consumer is
#: `strategy_live`, so it is a producer participant and the plan opens in PREPARE waiting
#: for its acknowledgement.
PRODUCER_ROLE = "feature_live"

#: How SQLite refuses a WAL open it may not create a wal-index for. The host's journal
#: showed the first of these, eight times over, four frames below the role; a macOS VFS
#: says the second for the same cause, so the reverse case accepts either and pins the
#: exception class, which is the one thing both platforms agree on.
HOST_FAILURES = (
    "unable to open database file",
    "attempt to write a readonly database",
)


def _seal(root: Path) -> None:
    """Refuse creation inside the rollout root, the way the unit's read-only mount does.

    Directories only, and file modes left exactly as the installer wrote them — 0600 for
    `authority.json`, which the bundle loader insists on before it will read the document,
    and the umask default for the state database. A read-only bind mount does not rewrite
    modes either. What it takes away is the ability to *create* the `-wal`, `-shm` and
    `-journal` files beside the database, and that is a directory permission — the one #227
    tripped over, and the one that reproduces the host's `unable to open database file`
    rather than a different SQLite refusal about the file's own mode.
    """

    for path in sorted(root.rglob("*"), reverse=True):
        if path.is_dir():
            path.chmod(0o555)
    root.chmod(0o555)


def _unseal(root: Path) -> None:
    root.chmod(0o755)
    for path in sorted(root.rglob("*")):
        if path.is_dir():
            path.chmod(0o755)


class RolloutWorld(RouteAWorld):
    """A `RouteAWorld` whose runtime root holds two generations and a prepared rollout."""

    previous_receipt: Any = None

    @property
    def rollout_root(self) -> Path:
        return self.runtime_root / "control" / "schema-rollouts"

    def plan_ids(self) -> tuple[str, ...]:
        return tuple(self.receipt.schema_rollout_plan_ids)

    def state_path(self, plan_id: str) -> Path:
        return self.rollout_root / plan_id / "state.sqlite3"

    def phase(self, plan_id: str) -> RolloutPhase:
        """Through the real loader, so the trusted registry comes off the bundle it did."""

        _authority, store = load_runtime_schema_rollout(
            self.runtime_root, plan_id=plan_id, read_only=True
        )
        return store.get_state(plan_id).phase

    def launch(self, role: str) -> dict[str, Any]:
        return self.world.resolve(role, self.instance_of(role))

    def run_role_in_wrapper_environment(self, role: str) -> tuple[int, _StopAfterOneIteration]:
        """`runtime_service_main.run()` under exactly the environment the wrapper builds."""

        launch = self.launch(role)
        argv = list(launch["module_argv"])
        index = argv.index("--control-root") + 1
        argv[index] = str(self.runtime_root / Path(argv[index]).relative_to(PRODUCTION_ROOT))
        arguments = service_main.build_parser().parse_args(argv)
        stop = _StopAfterOneIteration()
        real_event = service_main.Event
        service_main.Event = lambda: stop  # type: ignore[assignment]
        try:
            with mock.patch.dict(os.environ, dict(launch["environment"]), clear=True):
                code = service_main.run(arguments)
        finally:
            service_main.Event = real_event  # type: ignore[assignment]
        return code, stop


@pytest.fixture
def rollout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[RolloutWorld]:
    """Two installed generations, a prepared rollout, and a rollout root nothing may write."""

    world = World(tmp_path / "root", monkeypatch).build()
    runtime_root = tmp_path / "host" / "data" / "runtime"

    _previous_inputs, _previous_profile, previous_receipt, _previous_sealed = _production_bundle(
        tmp_path / "previous",
        monkeypatch,
        producer_commit=PREVIOUS_COMMIT,
        runtime_root=runtime_root,
        schema_bootstrap_reason="#227 acceptance bootstrap",
    )
    inputs, profile, receipt, sealed = _production_bundle(
        tmp_path / "target",
        monkeypatch,
        producer_commit=world.commit,
        runtime_root=runtime_root,
        #: only the first install into an empty root may carry one, and the second install
        #: without one is precisely what prepares the rollouts
        schema_bootstrap_reason=None,
        #: one registry root cannot hold two commits' definitions (#225); production gives
        #: each commit its own `definitions-<commit>`
        definition_registry_root=runtime_root.parent / f"definitions-{world.commit[:7]}",
    )

    route = RolloutWorld(world, inputs.runtime_root)
    route.profile = profile
    route.receipt = receipt
    route.previous_receipt = previous_receipt
    route.sealed_credentials = sealed
    route.stage_and_publish()

    _seal(route.rollout_root)
    try:
        yield route
    finally:
        _unseal(route.rollout_root)


# ---------------------------------------------------------------------------------------
# The world is the one #227 happened in: assert that before asserting anything about roles
# ---------------------------------------------------------------------------------------


def test_the_second_generation_is_the_first_one_that_carries_a_rollout(
    rollout: RolloutWorld,
) -> None:
    """`previous_generation_hash` non-null, plans prepared, every plan still in PREPARE."""

    assert rollout.previous_receipt.previous_generation_hash is None
    assert rollout.previous_receipt.schema_rollout_plan_ids == ()
    assert rollout.receipt.previous_generation_hash == rollout.previous_receipt.generation_hash

    plan_ids = rollout.plan_ids()
    assert len(plan_ids) >= 10, plan_ids
    assert sorted(path.name for path in rollout.rollout_root.iterdir()) == sorted(plan_ids)
    for plan_id in plan_ids:
        assert rollout.state_path(plan_id).is_file()
        assert rollout.phase(plan_id) is RolloutPhase.PREPARE


def test_the_rollout_root_really_refuses_writes(rollout: RolloutWorld) -> None:
    """Otherwise the rest of this file proves nothing. Root would pass through the bits."""

    if os.geteuid() == 0:
        pytest.skip("running as root: the mode bits this case relies on are not enforced")

    plan_id = rollout.plan_ids()[0]
    with pytest.raises(PermissionError):
        (rollout.rollout_root / plan_id / "state.sqlite3-shm").write_bytes(b"")
    with pytest.raises(PermissionError):
        (rollout.rollout_root / "intruder").mkdir()

    #: and the store the installer left is readable without one, which is the whole fix
    assert not list(rollout.rollout_root.glob("*/state.sqlite3-wal"))
    assert not list(rollout.rollout_root.glob("*/state.sqlite3-shm"))
    connection = sqlite3.connect(rollout.state_path(plan_id))
    try:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
    finally:
        connection.close()


# ---------------------------------------------------------------------------------------
# The roles, through the wrapper, into their loops, with the rollout root read-only
# ---------------------------------------------------------------------------------------


@pytest.mark.parametrize("role", READ_ONLY_ROLES)
def test_a_kind_backed_role_reaches_its_loop_with_the_rollout_root_read_only(
    rollout: RolloutWorld,
    role: str,
) -> None:
    """One iteration, and none of the host's restart loop on the way there."""

    code, stop = rollout.run_role_in_wrapper_environment(role)

    assert code == 0
    assert stop.iterations == 1, f"{role} never entered its service loop"


def test_both_roles_come_up_over_the_same_two_generation_root(rollout: RolloutWorld) -> None:
    """The window's real question — not one role, the group — and the plan is untouched."""

    before = {plan_id: rollout.phase(plan_id) for plan_id in rollout.plan_ids()}

    for role in READ_ONLY_ROLES:
        code, stop = rollout.run_role_in_wrapper_environment(role)
        assert (code, stop.iterations) == (0, 1), role

    assert {plan_id: rollout.phase(plan_id) for plan_id in rollout.plan_ids()} == before
    assert not list(rollout.rollout_root.glob("*/state.sqlite3-*"))


# ---------------------------------------------------------------------------------------
# Reverse: put the WAL open back and the same two roles are back in the restart loop
# ---------------------------------------------------------------------------------------


def _restore_the_wal_open(monkeypatch: pytest.MonkeyPatch) -> None:
    """v0.33.0's `SchemaRolloutStore` open, verbatim: mkdir, and WAL on every connect."""

    def _pre_fix_init(
        self: SchemaRolloutStore,
        path: Path,
        *,
        production_consumer_registry: Any = None,
        read_only: bool = False,  # noqa: ARG001 - the pre-fix signature had no such door
    ) -> None:
        self.path = Path(path)
        self.read_only = False
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.production_consumer_registry = production_consumer_registry
        self._initialize()

    def _pre_fix_connect(self: SchemaRolloutStore) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        connection.execute("PRAGMA journal_mode = WAL")
        return connection

    monkeypatch.setattr(SchemaRolloutStore, "__init__", _pre_fix_init)
    monkeypatch.setattr(SchemaRolloutStore, "_connect", _pre_fix_connect)


@pytest.mark.parametrize("role", READ_ONLY_ROLES)
def test_restoring_the_wal_open_puts_the_role_back_in_the_host_failure(
    rollout: RolloutWorld,
    monkeypatch: pytest.MonkeyPatch,
    role: str,
) -> None:
    """The mutation the fix is for, run through the same world: the role does not start."""

    if os.geteuid() == 0:
        pytest.skip("running as root: the mode bits this case relies on are not enforced")

    _restore_the_wal_open(monkeypatch)

    with pytest.raises(sqlite3.OperationalError) as caught:
        rollout.run_role_in_wrapper_environment(role)

    assert any(failure in str(caught.value) for failure in HOST_FAILURES), caught.value


# ---------------------------------------------------------------------------------------
# The residual, pinned rather than hidden: a producer participant still needs a writer
# ---------------------------------------------------------------------------------------


def test_a_producer_participant_names_the_sandbox_instead_of_the_sqlite_error(
    rollout: RolloutWorld,
) -> None:
    """Admission reads read-only; a PREPARE acknowledgement is an append and cannot.

    No unit's `ReadWritePaths` covers `control/schema-rollouts`, so a rollout that is still
    waiting on startup acknowledgements cannot be completed by the services themselves. That
    is a sequencing decision for the installer and the rollout controller, not something this
    fix may paper over — so what changes here is only that the refusal says which plan, which
    path and which sandbox setting, instead of `unable to open database file`.
    """

    if os.geteuid() == 0:
        pytest.skip("running as root: the mode bits this case relies on are not enforced")

    with pytest.raises(RuntimeSchemaCompatibilityError) as caught:
        rollout.run_role_in_wrapper_environment(PRODUCER_ROLE)

    message = str(caught.value)
    assert "ReadWritePaths" in message
    assert "control/schema-rollouts" in message
    assert str(rollout.rollout_root) in message


def test_a_wal_store_left_by_the_installed_build_fails_closed_with_a_named_reason(
    rollout: RolloutWorld,
) -> None:
    """The store the second window actually installed is WAL, and this says what happens.

    A build without this fix wrote every rollout state in WAL, so the host's
    `control/schema-rollouts` holds WAL stores right now. Fixing the reader does not fix
    those: nothing can read a WAL database without creating a wal-index beside it. The role
    therefore still refuses — but it says which store, that it is WAL, and which sandbox
    setting is in the way, which is the sentence that tells an operator to have the
    installer reopen it rather than to go looking at SQLite.
    """

    plan_id = rollout.plan_ids()[0]
    path = rollout.state_path(plan_id)
    path.parent.chmod(0o755)
    try:
        connection = sqlite3.connect(path, isolation_level=None)
        try:
            connection.execute("PRAGMA journal_mode = WAL")
        finally:
            connection.close()
    finally:
        for leftover in path.parent.glob("state.sqlite3-*"):
            leftover.unlink()
        path.parent.chmod(0o555)

    with pytest.raises(SchemaRolloutStateUnavailableError) as caught:
        rollout.run_role_in_wrapper_environment(READ_ONLY_ROLES[0])

    message = str(caught.value)
    assert "WAL" in message
    assert "ReadWritePaths" in message
    assert str(path) in message


def test_the_admission_read_itself_fails_closed_on_an_unreadable_store(
    rollout: RolloutWorld,
) -> None:
    """Not degraded, not skipped: an unreadable plan stops the role, naming the path."""

    plan_id = rollout.plan_ids()[0]
    path = rollout.state_path(plan_id)
    path.parent.chmod(0o755)
    try:
        path.unlink()
    finally:
        path.parent.chmod(0o555)

    with pytest.raises(SchemaRolloutStateUnavailableError) as caught:
        rollout.run_role_in_wrapper_environment(READ_ONLY_ROLES[0])

    assert str(path) in str(caught.value)
