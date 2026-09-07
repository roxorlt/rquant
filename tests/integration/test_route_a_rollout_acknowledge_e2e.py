"""The 2026-09-07 ruling, end to end: sixteen units may write, and the installer signs PREPARE.

Package G's world is the world this file needs — two really installed bundle generations over
one runtime root, a published authority chain, and the wrapper's own launch resolution — so it
is imported whole. What is added here is the half #227 could not reach:

* the sixteen units the owner granted `control/schema-rollouts` are derived from the sixteen
  plans this install really prepares, not read off a list, and compared against the list the
  unit files are pinned to;
* `acknowledge` previews and then carries all sixteen plans from PREPARE to DUAL_WRITE, once,
  and refuses to go one phase further;
* the two producer participants of the plan with three producers start without the failure
  that made the host's first instances flap, once the rollout root is writable — which is
  exactly what the unit grant does on the host;
* the sentence package G gave the main-loop writers is still what a service gets when the
  rollout root is *not* writable, which is a reachable state: every granted entry is written
  `-/…`, so on a host where the rollout root does not exist the unit still has no write.

Everything the two-generation install needs is package G's fixture; nothing here re-implements
it, and nothing here writes to a root the sandbox would refuse without saying so.
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

import rquant.runtime_service_main as service_main
from rquant import runtime_deployment_bundle as bundle
from rquant.runtime_deployment_bundle import (
    acknowledge_runtime_schema_rollout_preparation,
    load_runtime_schema_rollout,
    load_runtime_schema_service_bindings,
)
from rquant.runtime_schema_registry import (
    RuntimeSchemaCompatibilityError,
    RuntimeSchemaDualWriteBinding,
)
from rquant.schema_compatibility import RolloutPhase, persisted_rollout_journal_layout
from tests.integration.test_route_a_legacy_binding_e2e import (
    PRODUCTION_ROOT,
    _StopAfterOneIteration,
)
from tests.integration.test_route_a_schema_rollout_sandbox_e2e import (
    READ_ONLY_ROLES,
    RolloutWorld,
    _seal,
    _unseal,
)
from tests.integration.test_route_a_schema_rollout_sandbox_e2e import (
    rollout as _package_g_rollout,
)
from tests.unit.test_runtime_systemd_schema_rollout_paths import PARTICIPANTS

pytestmark = pytest.mark.integration

#: Package G's two-generation fixture, whole. pytest finds a fixture by module attribute, and
#: an imported name that every case also takes as a parameter reads to the linter as a
#: redefinition, so it is imported privately and re-exported under the name the cases use.
rollout = _package_g_rollout

#: The plan the production profile gives three producers: three candidate publishers feeding
#: `runtime.strategy_candidate.snapshot`. Its first two instances are the ones that failed once
#: each on the host, because a producer's startup raises until every producer has acknowledged.
MULTI_PRODUCER_ROLE = "candidate_publisher"
#: The other three-producer plan, `runtime.strategy_signal.envelope`. Its roles do not reach a
#: service loop in this harness for reasons of their own (#215-shaped host conditions), so what
#: is asserted about them is only that the rollout is no longer what stops them.
SECOND_MULTI_PRODUCER_ROLE = "strategy_live"
#: A producer whose plan has one producer, so it never had the multi-producer failure — it is
#: here to show the acknowledgement did not take anything away from the roles that did work.
SINGLE_PRODUCER_ROLE = "auction_universe_publisher"
#: The main-loop writer package G wrapped: `market_minute_gateway.commit_payload`.
LOOP_WRITER_SERVICE = "market-minute.source.v1"


def _run_instance(world: RolloutWorld, role: str, instance: str) -> tuple[int, Any]:
    """`runtime_service_main.run()` for one named instance of a role, one loop iteration.

    `RolloutWorld.run_role_in_wrapper_environment` resolves the single instance of a role;
    the plan that matters here has three of them, so this takes the label explicitly.
    """

    launch = world.world.resolve(role, instance)
    argv = list(launch["module_argv"])
    index = argv.index("--control-root") + 1
    argv[index] = str(world.runtime_root / Path(argv[index]).relative_to(PRODUCTION_ROOT))
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


def _instances(world: RolloutWorld, role: str) -> list[str]:
    return list(world.world.instances(world.plan)[role])


def _digests(world: RolloutWorld) -> dict[str, str]:
    return {
        plan_id: hashlib.sha256(world.state_path(plan_id).read_bytes()).hexdigest()
        for plan_id in world.plan_ids()
    }


def _acknowledge(world: RolloutWorld, **kwargs: Any) -> tuple[Any, ...]:
    """Run the acknowledgement with the rollout root writable, the way the installer does."""

    _unseal(world.rollout_root)
    try:
        return acknowledge_runtime_schema_rollout_preparation(
            world.runtime_root, now=datetime.now(UTC), **kwargs
        )
    finally:
        _seal(world.rollout_root)


# ---------------------------------------------------------------------------------------
# Which sixteen units — derived from the plans, not read off a list
# ---------------------------------------------------------------------------------------


def test_the_granted_units_are_exactly_the_participants_of_the_prepared_plans(
    rollout: RolloutWorld,
) -> None:
    """The unit files carry a literal list; this is where that list comes from.

    A service is a participant if any plan names it as a producer — it appends a PREPARE and
    later a CUTOVER acknowledgement — or as a consumer, because at CONSUMER_ACK every consumer
    of the plan appends a capability receipt (the ones that need a serving generation through a
    binding, the rest directly). Both are appends to the plan's hash chain, and an append needs
    a journal created beside the database, which is a directory permission.
    """

    kinds = {
        manifest.service_id: manifest.service_kind.value for manifest in rollout.profile.manifests
    }
    unit_of_role = {role: unit for unit, (role, _kind) in PARTICIPANTS.items()}

    participants: set[str] = set()
    for plan_id in rollout.plan_ids():
        authority, _store = load_runtime_schema_rollout(
            rollout.runtime_root, plan_id=plan_id, read_only=True
        )
        participants.update(item.participant_id for item in authority.plan.producers)
        participants.update(item.service_id for item in authority.registry.consumers)

    granted = {unit_of_role[kinds[service_id]] for service_id in participants}

    assert len(rollout.plan_ids()) == 16
    assert granted == set(PARTICIPANTS)
    assert len(granted) == 16


# ---------------------------------------------------------------------------------------
# The preview, then the acknowledgement
# ---------------------------------------------------------------------------------------


def test_the_preview_lists_all_sixteen_plans_without_any_write_at_all(
    rollout: RolloutWorld,
) -> None:
    """Run against the sealed root on purpose: the preview must need no write to work."""

    if os.geteuid() == 0:
        pytest.skip("running as root: the mode bits this case relies on are not enforced")

    before = _digests(rollout)

    results = acknowledge_runtime_schema_rollout_preparation(
        rollout.runtime_root, now=datetime.now(UTC), dry_run=True
    )

    assert len(results) == 16
    assert {item.phase_before for item in results} == {RolloutPhase.PREPARE}
    assert {item.phase_after for item in results} == {RolloutPhase.DUAL_WRITE}
    assert all(item.acknowledged_producers for item in results)
    assert all(item.already_acknowledged_producers == () for item in results)
    assert _digests(rollout) == before
    assert not list(rollout.rollout_root.glob("*/state.sqlite3-*"))
    assert all(rollout.phase(plan_id) is RolloutPhase.PREPARE for plan_id in rollout.plan_ids())


def test_acknowledging_carries_all_sixteen_plans_to_dual_write(rollout: RolloutWorld) -> None:
    results = _acknowledge(rollout)

    assert len(results) == 16
    assert all(item.changed for item in results)
    assert {item.phase_after for item in results} == {RolloutPhase.DUAL_WRITE}
    assert all(rollout.phase(plan_id) is RolloutPhase.DUAL_WRITE for plan_id in rollout.plan_ids())
    assert {
        persisted_rollout_journal_layout(rollout.state_path(plan_id))
        for plan_id in rollout.plan_ids()
    } == {"rollback"}
    assert not list(rollout.rollout_root.glob("*/state.sqlite3-*"))


def test_a_second_acknowledgement_changes_nothing(rollout: RolloutWorld) -> None:
    _acknowledge(rollout)
    before = _digests(rollout)

    again = _acknowledge(rollout)

    assert len(again) == 16
    assert not any(item.changed for item in again)
    assert {item.phase_before for item in again} == {RolloutPhase.DUAL_WRITE}
    assert _digests(rollout) == before


def test_the_installer_refuses_to_carry_a_plan_past_dual_write(
    rollout: RolloutWorld,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The reverse case: raising the ceiling to CUTOVER stops, it does not proceed.

    CUTOVER is where a plan stops being provisional, and what earns it is the producers' own
    dual-write records plus the trusted consumers' receipts. An installer that could sign for
    it would make the whole protocol a formality, so the ceiling is checked in the installer
    as well as in the store, and the plans stay in PREPARE.
    """

    monkeypatch.setattr(
        bundle, "SCHEMA_ROLLOUT_INSTALLER_PHASE_CEILING", RolloutPhase.CUTOVER, raising=True
    )

    with pytest.raises(RuntimeSchemaCompatibilityError, match="no further than dual_write"):
        _acknowledge(rollout)

    assert all(rollout.phase(plan_id) is RolloutPhase.PREPARE for plan_id in rollout.plan_ids())


def test_a_wal_store_left_by_the_installed_build_is_converted_by_the_acknowledgement(
    rollout: RolloutWorld,
) -> None:
    """The production host's sixteen stores are WAL right now, and this is the way out.

    The recommended repair is one writer open per store; the acknowledgement is a writer open
    per store, so it performs the conversion as part of the step that has to happen anyway.
    """

    plan_id = rollout.plan_ids()[0]
    _unseal(rollout.rollout_root)
    try:
        connection = sqlite3.connect(rollout.state_path(plan_id), isolation_level=None)
        try:
            connection.execute("PRAGMA journal_mode = WAL")
        finally:
            connection.close()
        for leftover in rollout.state_path(plan_id).parent.glob("state.sqlite3-*"):
            leftover.unlink()
    finally:
        _seal(rollout.rollout_root)

    preview = acknowledge_runtime_schema_rollout_preparation(
        rollout.runtime_root, now=datetime.now(UTC), dry_run=True
    )
    previewed = next(item for item in preview if item.plan_id == plan_id)
    assert previewed.journal_mode_before == "wal"
    assert previewed.phase_before is None
    assert persisted_rollout_journal_layout(rollout.state_path(plan_id)) == "wal"

    applied = next(item for item in _acknowledge(rollout) if item.plan_id == plan_id)

    assert (applied.journal_mode_before, applied.journal_mode_after) == ("wal", "rollback")
    assert applied.phase_after is RolloutPhase.DUAL_WRITE
    assert not list(rollout.rollout_root.glob("*/state.sqlite3-*"))


# ---------------------------------------------------------------------------------------
# The roles: with the grant in place, the multi-producer plan no longer costs two failures
# ---------------------------------------------------------------------------------------


@pytest.mark.parametrize("ordinal", (0, 1))
def test_a_producer_of_the_three_producer_plan_no_longer_fails_first(
    rollout: RolloutWorld,
    ordinal: int,
) -> None:
    """The host's shape, both halves: the failure before, and its absence after.

    The rollout root is writable throughout, which is what the unit grant does on the host —
    so this is not the sandbox refusing, it is the protocol itself. Before the acknowledgement
    a producer records its own PREPARE and then raises, because two of its three peers have not
    started yet; every one of those failures relays an alert. After it, the same instance walks
    into its service loop.
    """

    instance = _instances(rollout, MULTI_PRODUCER_ROLE)[ordinal]
    _unseal(rollout.rollout_root)

    with pytest.raises(RuntimeSchemaCompatibilityError, match="waiting for every producer"):
        _run_instance(rollout, MULTI_PRODUCER_ROLE, instance)

    acknowledge_runtime_schema_rollout_preparation(rollout.runtime_root, now=datetime.now(UTC))
    code, stop = _run_instance(rollout, MULTI_PRODUCER_ROLE, instance)

    assert (code, stop.iterations) == (0, 1)


def test_the_second_three_producer_plan_stops_blocking_its_roles_too(
    rollout: RolloutWorld,
) -> None:
    """`strategy_live` does not reach a loop here for host reasons; the rollout is not one."""

    instance = _instances(rollout, SECOND_MULTI_PRODUCER_ROLE)[0]
    _unseal(rollout.rollout_root)

    with pytest.raises(RuntimeSchemaCompatibilityError, match="waiting for every producer"):
        _run_instance(rollout, SECOND_MULTI_PRODUCER_ROLE, instance)

    acknowledge_runtime_schema_rollout_preparation(rollout.runtime_root, now=datetime.now(UTC))

    with pytest.raises(Exception) as caught:  # noqa: PT011 - the point is which one it is not
        _run_instance(rollout, SECOND_MULTI_PRODUCER_ROLE, instance)

    assert not isinstance(caught.value, RuntimeSchemaCompatibilityError), caught.value
    assert "PREPARE ACK" not in str(caught.value)


def test_a_single_producer_role_still_comes_up_after_the_acknowledgement(
    rollout: RolloutWorld,
) -> None:
    """It was never blocked; the acknowledgement must not have taken anything from it.

    Its plan is at DUAL_WRITE afterwards, so its loop runs with a dual-write binding rather
    than with none — which is the phase this whole step exists to reach.
    """

    instance = _instances(rollout, SINGLE_PRODUCER_ROLE)[0]
    _unseal(rollout.rollout_root)
    acknowledge_runtime_schema_rollout_preparation(rollout.runtime_root, now=datetime.now(UTC))

    code, stop = _run_instance(rollout, SINGLE_PRODUCER_ROLE, instance)

    assert (code, stop.iterations) == (0, 1)


@pytest.mark.parametrize("role", READ_ONLY_ROLES)
def test_a_read_only_role_still_comes_up_over_the_acknowledged_root(
    rollout: RolloutWorld,
    role: str,
) -> None:
    """Package G's two roles, over sixteen plans the installer has just written into.

    The acknowledgement is a writer open per store; if it left any of them in a layout the
    sandboxed read-only admission cannot read, these two would be back in #227.
    """

    _acknowledge(rollout)

    code, stop = rollout.run_role_in_wrapper_environment(role)

    assert (code, stop.iterations) == (0, 1)


# ---------------------------------------------------------------------------------------
# The main-loop writers still say why, and the grant does not make that unreachable
# ---------------------------------------------------------------------------------------


def test_a_dual_write_commit_under_an_unwritable_rollout_root_still_names_the_sandbox(
    rollout: RolloutWorld,
) -> None:
    """Package G's sentence, over the real plan, at the phase that makes it reachable.

    The unit grant is written `-/home/…/control/schema-rollouts`, ignored when missing, so on
    a host where that directory does not exist the unit still has no write to it — and a plan
    at DUAL_WRITE is exactly when a producer's loop tries to append. The refusal has to say
    which service, which store and which sandbox setting, which is what tells an operator to
    look at the unit rather than at SQLite.
    """

    if os.geteuid() == 0:
        pytest.skip("running as root: the mode bits this case relies on are not enforced")

    _acknowledge(rollout)
    manifest = next(
        item for item in rollout.profile.manifests if item.service_id == LOOP_WRITER_SERVICE
    )
    bindings = load_runtime_schema_service_bindings(
        rollout.runtime_root,
        manifest=manifest,
        generation_id=rollout.receipt.generation_hash,
        observed_at=datetime.now(UTC),
    )
    binding = next(item for item in bindings if isinstance(item, RuntimeSchemaDualWriteBinding))
    prepared = binding.prepare_payload(
        {name: f"value-for-{name}" for name in binding.new_declaration.available_fields()},
        observed_at=datetime.now(UTC),
    )
    assert prepared is not None, "a plan at dual_write must hand back a prepared payload"

    with pytest.raises(RuntimeSchemaCompatibilityError) as caught:
        binding.commit_payload(prepared, operation_id="acceptance-dual-write")

    message = str(caught.value)
    assert LOOP_WRITER_SERVICE in message
    assert str(binding.store_path) in message
    assert "ReadWritePaths" in message
    assert "control/schema-rollouts" in message
