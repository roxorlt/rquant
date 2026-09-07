"""The installer carries a prepared schema rollout to DUAL_WRITE, and not one phase further.

`install_runtime_deployment_profile` prepares one rollout plan per changed channel whenever a
bundle generation has a predecessor, and every plan opens in PREPARE waiting for an
acknowledgement from each of its producers. Until 2026-09-07 no runtime unit could write
`control/schema-rollouts` at all, so no producer could ever record one (#227); with the grant
in place they can, but a plan with three producers still makes its first two instances fail
once each — `load_runtime_schema_service_bindings` raises `schema producer startup is waiting
for every producer PREPARE ACK` until the last producer has started — and every failure relays
an alert through `OnFailure=rquant-alert@%n.service`.

So the owner authorised the installer to do the PREPARE round on the producers' behalf. That
is safe for exactly one phase: every argument a PREPARE acknowledgement takes comes off the
frozen plan, and `SchemaRolloutStore.acknowledge` re-derives all of them from the frozen
registry before recording anything, so nothing is signed that the plan did not already say.
Leaving DUAL_WRITE is not like that — it takes the producers' own dual-write consistency
evidence — and CUTOVER takes the trusted consumers' receipts. This module is where that line
is held, and where the WAL-to-rollback-journal conversion the same writable open performs is
pinned.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from rquant import runtime_deployment_bundle as bundle
from rquant.runtime_deployment_bundle import (
    SCHEMA_ROLLOUT_INSTALLER_PHASE_CEILING,
    RuntimeSchemaDualWriteBinding,
    acknowledge_runtime_schema_rollout_preparation,
    changed_runtime_schema_channels,
    load_runtime_schema_rollout,
    load_runtime_schema_service_bindings,
    prepare_runtime_schema_rollout,
    strategy_live_producer_version,
)
from rquant.runtime_schema_registry import RuntimeSchemaCompatibilityError
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind
from rquant.schema_compatibility import (
    RolloutPhase,
    SchemaRolloutStateUnavailableError,
    SchemaRolloutStore,
    persisted_rollout_journal_layout,
)
from tests.unit.test_runtime_deployment_bundle import (
    COMMIT,
    _bundle_inputs,
    _manifest,
    install_runtime_deployment_bundle,
    isolated_root_credential_sealer,  # noqa: F401 - autouse fixture, imported to apply here
)

#: The commit the second generation is built at. A declaration fingerprint is
#: `semantic_fingerprint + producer_commit`, so a different commit is what makes every
#: two-sided channel a changed channel — the same reason the production host staged sixteen
#: plans for a release that changed no schema at all (#228).
NEXT_COMMIT = "b" * 40

#: The production profile's own window: `runtime_production_profile.py` defaults
#: `schema_rollout_stage_timeout_seconds` to 600, and `install_runtime_deployment_profile`
#: makes every plan's deadline `started_at + that`. Ten minutes is what the host really gives,
#: and pretending otherwise is what hid the expiry defect the first time round.
STAGE_TIMEOUT = timedelta(seconds=600)
#: The CLI stamps its own `datetime.now(UTC)`, so a fixture whose window closed in 2026 would
#: make every CLI case a deadline case. `STARTED_AT` is therefore anchored to the run's own
#: clock, and `EXPIRED` is the only thing that steps past the window.
STARTED_AT = datetime.now(UTC).replace(microsecond=0) - timedelta(seconds=30)
DEADLINE = STARTED_AT + STAGE_TIMEOUT
#: Inside the window, and after `started_at`: the ordinary case.
LATER = STARTED_AT + timedelta(seconds=120)
#: One second past the window: what an operator who took eleven minutes over the step has.
EXPIRED = DEADLINE + timedelta(seconds=1)


def _at_commit(manifest: Any, commit: str) -> Any:
    """The same manifest built at another commit, which is what a release really changes.

    A `strategy_live` manifest binds its own commit twice: once as `producer_commit`, once
    inside `settings["producer_version"]`, and `validate_strategy_live_completion_manifest`
    refuses the pair when they disagree.
    """

    update: dict[str, Any] = {"producer_commit": commit}
    if manifest.service_kind is RuntimeServiceKind.STRATEGY_LIVE:
        settings = dict(manifest.settings)
        settings["producer_version"] = strategy_live_producer_version(
            service_id=manifest.service_id,
            strategy_version=settings.get("strategy_version"),
            producer_commit=commit,
        )
        update["settings"] = settings
    return manifest.model_copy(update=update)


class Rollout:
    """Two installed generations over one runtime root, with every changed channel prepared."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.plan_ids: tuple[str, ...] = ()
        self.manifests: tuple[Any, ...] = ()
        self.generation_id = ""

    def state_path(self, plan_id: str) -> Path:
        return self.root / "control" / "schema-rollouts" / plan_id / "state.sqlite3"

    def phase(self, plan_id: str) -> RolloutPhase:
        _authority, store = load_runtime_schema_rollout(self.root, plan_id=plan_id, read_only=True)
        return store.get_state(plan_id).phase

    def revision(self, plan_id: str) -> int:
        _authority, store = load_runtime_schema_rollout(self.root, plan_id=plan_id, read_only=True)
        return store.get_state(plan_id).revision

    def revision_unverified(self, plan_id: str) -> int:
        """The revision straight off the row, for a store the read-only loader cannot open.

        A WAL store refuses a read-only open (#227), and the case that puts one there has to
        record what the revision was *before* the conversion in order to prove the conversion
        appended nothing.
        """

        connection = sqlite3.connect(self.state_path(plan_id))
        try:
            row = connection.execute(
                "SELECT revision FROM schema_rollout WHERE plan_id = ?", (plan_id,)
            ).fetchone()
        finally:
            connection.close()
        for leftover in self.state_path(plan_id).parent.glob("state.sqlite3-*"):
            leftover.unlink()
        return int(row[0])

    def producers(self, plan_id: str) -> tuple[str, ...]:
        authority, _store = load_runtime_schema_rollout(self.root, plan_id=plan_id, read_only=True)
        return tuple(sorted(item.participant_id for item in authority.plan.producers))

    def digest(self) -> dict[str, str]:
        """A byte-for-byte fingerprint of every store, so "wrote nothing" can be asserted."""

        return {
            plan_id: hashlib.sha256(self.state_path(plan_id).read_bytes()).hexdigest()
            for plan_id in self.plan_ids
        }

    def sidecars(self) -> list[str]:
        root = self.root / "control" / "schema-rollouts"
        return sorted(path.name for path in root.glob("*/state.sqlite3-*"))

    def make_wal(self, plan_id: str) -> None:
        """Put one store back in the layout every build before #227 left behind."""

        path = self.state_path(plan_id)
        connection = sqlite3.connect(path, isolation_level=None)
        try:
            connection.execute("PRAGMA journal_mode = WAL")
        finally:
            connection.close()
        for leftover in path.parent.glob("state.sqlite3-*"):
            leftover.unlink()


@pytest.fixture
def rollout(tmp_path: Path) -> Rollout:
    """A real two-generation install, with a real plan prepared for every changed channel."""

    root = tmp_path / "runtime"
    manifests, capabilities = _bundle_inputs(root)
    #: A second candidate publisher and the strategy that consumes both of them, so that
    #: `runtime.strategy_candidate.snapshot` carries two producers. That is the shape that
    #: makes a producer's first instance fail once on the host — two of the sixteen production
    #: plans carry three producers each — and the shape the acknowledgement exists to remove.
    extra = (
        _manifest(
            root,
            service_id="candidate-auction-gap",
            kind=RuntimeServiceKind.CANDIDATE_PUBLISHER,
            plane=RuntimeServicePlane.LIVE,
        ),
        _manifest(
            root,
            service_id="strategy-live-main",
            kind=RuntimeServiceKind.STRATEGY_LIVE,
            plane=RuntimeServicePlane.LIVE,
        ),
    )
    manifests = (*manifests, *extra)
    capabilities = {**capabilities, **{item.service_id: {} for item in extra}}

    first = install_runtime_deployment_bundle(
        root,
        producer_commit=COMMIT,
        manifests=manifests,
        capability_env=capabilities,
        schema_bootstrap_reason="acknowledge fixture bootstrap",
    )
    candidate_manifests = tuple(_at_commit(manifest, NEXT_COMMIT) for manifest in manifests)
    second = install_runtime_deployment_bundle(
        root,
        producer_commit=NEXT_COMMIT,
        manifests=candidate_manifests,
        capability_env=capabilities,
    )

    world = Rollout(root)
    world.manifests = candidate_manifests
    world.generation_id = second.generation_hash
    plan_ids: list[str] = []
    for channel_id in changed_runtime_schema_channels(
        root,
        previous_generation_id=first.generation_hash,
        target_generation_id=second.generation_hash,
    ):
        try:
            authority = prepare_runtime_schema_rollout(
                root,
                previous_generation_id=first.generation_hash,
                target_generation_id=second.generation_hash,
                channel_id=channel_id,
                started_at=STARTED_AT,
                deadline=DEADLINE,
                consumer_ack_max_age_seconds=3600,
            )
        except (RuntimeSchemaCompatibilityError, ValidationError) as exc:
            #: A rollout is only meaningful for a channel that has both a producer and a
            #: consumer. On the host the profile's `schema_rollout_policies` is what selects
            #: those; here the same selection falls out of the refusal, so the fixture stays a
            #: real install rather than a hand-written list of channel names.
            assert "no production consumers" in str(exc) or "producer registry cannot be" in str(
                exc
            ), channel_id
            continue
        plan_ids.append(authority.plan.plan_id)
    world.plan_ids = tuple(sorted(plan_ids))
    return world


# ---------------------------------------------------------------------------------------
# The world: prepared plans, every one of them in PREPARE, at least one with two producers
# ---------------------------------------------------------------------------------------


def test_the_fixture_is_the_shape_the_host_installed(rollout: Rollout) -> None:
    assert rollout.plan_ids
    assert all(rollout.phase(plan_id) is RolloutPhase.PREPARE for plan_id in rollout.plan_ids)
    assert any(len(rollout.producers(plan_id)) > 1 for plan_id in rollout.plan_ids)
    assert all(
        persisted_rollout_journal_layout(rollout.state_path(plan_id)) == "rollback"
        for plan_id in rollout.plan_ids
    )


# ---------------------------------------------------------------------------------------
# Applying: every producer acknowledged, every plan at DUAL_WRITE, nothing beyond it
# ---------------------------------------------------------------------------------------


def test_every_prepared_plan_is_carried_to_dual_write(rollout: Rollout) -> None:
    results = acknowledge_runtime_schema_rollout_preparation(rollout.root, now=LATER)

    assert {item.plan_id for item in results} == set(rollout.plan_ids)
    for item in results:
        assert item.skipped_reason is None, item
        assert item.phase_before is RolloutPhase.PREPARE
        assert item.phase_after is RolloutPhase.DUAL_WRITE
        assert item.advanced is True
        assert item.changed is True
        assert item.acknowledged_producers == rollout.producers(item.plan_id)
        assert item.already_acknowledged_producers == ()
    assert all(rollout.phase(plan_id) is RolloutPhase.DUAL_WRITE for plan_id in rollout.plan_ids)


def test_no_plan_is_ever_carried_past_dual_write(rollout: Rollout) -> None:
    """The authorisation is one phase wide, and this is the whole of it."""

    acknowledge_runtime_schema_rollout_preparation(rollout.root, now=LATER)
    acknowledge_runtime_schema_rollout_preparation(rollout.root, now=LATER + timedelta(minutes=1))

    beyond = {RolloutPhase.CONSUMER_ACK, RolloutPhase.CUTOVER, RolloutPhase.RETIRE}
    for plan_id in rollout.plan_ids:
        assert rollout.phase(plan_id) not in beyond
    assert SCHEMA_ROLLOUT_INSTALLER_PHASE_CEILING is RolloutPhase.DUAL_WRITE


def test_raising_the_ceiling_past_dual_write_is_refused_before_the_plan_moves(
    rollout: Rollout,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The mutation this whole package must not survive: an installer signing CUTOVER.

    Advancing straight to CUTOVER would skip the phase the producers pay for in real
    dual-write records. `SchemaRolloutStore` would refuse the jump on its own — phases advance
    consecutively, and leaving DUAL_WRITE needs consistency evidence — but a refusal that only
    lives in the store is a refusal an installer could route around by walking the phases one
    at a time. So the ceiling is checked here too, and the plan stays where it was.
    """

    monkeypatch.setattr(
        bundle, "SCHEMA_ROLLOUT_INSTALLER_PHASE_CEILING", RolloutPhase.CUTOVER, raising=True
    )

    with pytest.raises(RuntimeSchemaCompatibilityError, match="no further than dual_write"):
        acknowledge_runtime_schema_rollout_preparation(rollout.root, now=LATER)

    assert all(rollout.phase(plan_id) is RolloutPhase.PREPARE for plan_id in rollout.plan_ids)


def test_a_preview_under_a_raised_ceiling_refuses_instead_of_previewing_it(
    rollout: Rollout,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A preview is what an operator reads before deciding, so it must refuse the same way.

    Without this the dry run of a build whose ceiling had been raised would answer
    "would advance to cutover" — a plan for something the apply path then refuses, which is
    the worst of both: it reads like an authorisation and is not one.
    """

    monkeypatch.setattr(
        bundle, "SCHEMA_ROLLOUT_INSTALLER_PHASE_CEILING", RolloutPhase.CUTOVER, raising=True
    )

    with pytest.raises(RuntimeSchemaCompatibilityError, match="no further than dual_write"):
        acknowledge_runtime_schema_rollout_preparation(rollout.root, now=LATER, dry_run=True)


def test_a_second_run_records_nothing_and_says_so(rollout: Rollout) -> None:
    """Idempotent by observation, not by replay: the second run finds nothing left to do."""

    acknowledge_runtime_schema_rollout_preparation(rollout.root, now=LATER)
    revisions = {plan_id: rollout.revision(plan_id) for plan_id in rollout.plan_ids}
    digests = rollout.digest()

    again = acknowledge_runtime_schema_rollout_preparation(
        rollout.root, now=LATER + timedelta(minutes=1)
    )

    for item in again:
        assert item.changed is False
        assert item.advanced is False
        assert item.acknowledged_producers == ()
        assert item.phase_before is RolloutPhase.DUAL_WRITE
        assert item.phase_after is RolloutPhase.DUAL_WRITE
        assert item.skipped_reason == "past_prepare"
        assert item.detail == "plan is in dual_write; nothing here may act on it"
    assert {plan_id: rollout.revision(plan_id) for plan_id in rollout.plan_ids} == revisions
    assert rollout.digest() == digests


def test_a_producer_that_already_acknowledged_is_reported_apart(rollout: Rollout) -> None:
    """A half-done PREPARE round — one producer started, the rest did not — is completed."""

    plan_id = next(plan_id for plan_id in rollout.plan_ids if len(rollout.producers(plan_id)) > 1)
    authority, store = load_runtime_schema_rollout(rollout.root, plan_id=plan_id)
    first_producer = min(authority.plan.producers, key=lambda item: item.participant_id)
    store.acknowledge(
        plan_id=plan_id,
        expected_revision=store.get_state(plan_id).revision,
        phase=RolloutPhase.PREPARE,
        participant_id=first_producer.participant_id,
        participant_fingerprint=first_producer.contract_fingerprint,
        declaration_fingerprint=authority.plan.new_declaration_fingerprint,
        now=STARTED_AT + timedelta(minutes=1),
        operation_id=f"service-prepare:probe:{first_producer.participant_id}",
    )

    results = acknowledge_runtime_schema_rollout_preparation(rollout.root, now=LATER)
    item = next(result for result in results if result.plan_id == plan_id)

    assert item.already_acknowledged_producers == (first_producer.participant_id,)
    assert first_producer.participant_id not in item.acknowledged_producers
    assert set(item.acknowledged_producers) | {first_producer.participant_id} == set(
        rollout.producers(plan_id)
    )
    assert item.phase_after is RolloutPhase.DUAL_WRITE


def test_the_acknowledgements_are_in_the_plans_own_hash_chain(rollout: Rollout) -> None:
    """Not a side file: the same append-only chain a producer would have written into."""

    acknowledge_runtime_schema_rollout_preparation(rollout.root, now=LATER)
    plan_id = rollout.plan_ids[0]
    _authority, store = load_runtime_schema_rollout(rollout.root, plan_id=plan_id, read_only=True)

    payloads = [json.loads(receipt.payload_json) for receipt in store.receipts(plan_id)]
    operations = [receipt.operation_id for receipt in store.receipts(plan_id)]

    assert [payload["action"] for payload in payloads[-1:]] == ["advance"]
    assert {
        payload["participant_id"] for payload in payloads if payload["action"] == "participant_ack"
    } == set(rollout.producers(plan_id))
    assert any(operation.startswith("installer-prepare:") for operation in operations)
    assert f"installer-dual-write:{plan_id}" in operations


def test_a_producer_starting_after_the_acknowledgement_no_longer_waits(
    rollout: Rollout,
) -> None:
    """The reason for the whole thing: no first-instance failure, no alert relay.

    Before the acknowledgement a producer of a multi-producer plan raises on startup because
    the other producers have not acknowledged yet. After it, the same admission call returns a
    dual-write binding.
    """

    plan_id = next(plan_id for plan_id in rollout.plan_ids if len(rollout.producers(plan_id)) > 1)
    producer_id = rollout.producers(plan_id)[0]
    manifest = next(item for item in rollout.manifests if item.service_id == producer_id)

    with pytest.raises(RuntimeSchemaCompatibilityError, match="waiting for every producer"):
        load_runtime_schema_service_bindings(
            rollout.root,
            manifest=manifest,
            generation_id=rollout.generation_id,
            observed_at=LATER,
        )

    acknowledge_runtime_schema_rollout_preparation(rollout.root, now=LATER)
    bindings = load_runtime_schema_service_bindings(
        rollout.root,
        manifest=manifest,
        generation_id=rollout.generation_id,
        observed_at=LATER + timedelta(minutes=1),
    )

    assert any(
        isinstance(binding, RuntimeSchemaDualWriteBinding) and binding.plan.plan_id == plan_id
        for binding in bindings
    )


# ---------------------------------------------------------------------------------------
# The journal conversion the writable open performs, and that the preview does not
# ---------------------------------------------------------------------------------------


def test_applying_converts_a_store_an_older_build_left_in_wal(rollout: Rollout) -> None:
    """The production host's sixteen stores are WAL right now; this is what fixes them."""

    plan_id = rollout.plan_ids[0]
    rollout.make_wal(plan_id)
    with pytest.raises(SchemaRolloutStateUnavailableError, match="WAL"):
        rollout.phase(plan_id)

    results = acknowledge_runtime_schema_rollout_preparation(rollout.root, now=LATER)
    item = next(result for result in results if result.plan_id == plan_id)

    assert item.journal_mode_before == "wal"
    assert item.journal_mode_after == "rollback"
    assert persisted_rollout_journal_layout(rollout.state_path(plan_id)) == "rollback"
    assert rollout.sidecars() == []
    #: and the read-only admission open a unit does can now read it
    assert rollout.phase(plan_id) is RolloutPhase.DUAL_WRITE


def test_an_untouched_store_reports_the_same_layout_before_and_after(rollout: Rollout) -> None:
    """The conversion is reported, not assumed — a store already in rollback stays there."""

    results = acknowledge_runtime_schema_rollout_preparation(rollout.root, now=LATER)

    assert {item.journal_mode_before for item in results} == {"rollback"}
    assert {item.journal_mode_after for item in results} == {"rollback"}
    assert rollout.sidecars() == []


# ---------------------------------------------------------------------------------------
# The preview
# ---------------------------------------------------------------------------------------


def test_the_dry_run_lists_every_plan_and_writes_nothing(rollout: Rollout) -> None:
    digests = rollout.digest()

    results = acknowledge_runtime_schema_rollout_preparation(rollout.root, now=LATER, dry_run=True)

    assert {item.plan_id for item in results} == set(rollout.plan_ids)
    for item in results:
        assert item.phase_before is RolloutPhase.PREPARE
        assert item.phase_after is RolloutPhase.DUAL_WRITE
        assert item.advanced is True
        assert item.acknowledged_producers == rollout.producers(item.plan_id)
    assert rollout.digest() == digests
    assert rollout.sidecars() == []
    assert all(rollout.phase(plan_id) is RolloutPhase.PREPARE for plan_id in rollout.plan_ids)


def test_the_dry_run_refuses_to_convert_a_wal_store_just_to_preview_it(
    rollout: Rollout,
) -> None:
    """Converting is the change being previewed; a preview that performs it is not one."""

    plan_id = rollout.plan_ids[0]
    rollout.make_wal(plan_id)

    results = acknowledge_runtime_schema_rollout_preparation(rollout.root, now=LATER, dry_run=True)
    item = next(result for result in results if result.plan_id == plan_id)

    assert item.journal_mode_before == "wal"
    assert item.journal_mode_after == "wal"
    assert item.phase_before is None
    assert item.advanced is False
    assert item.skipped_reason == "state_unreadable"
    assert item.detail is not None
    assert "WAL" in item.detail
    assert persisted_rollout_journal_layout(rollout.state_path(plan_id)) == "wal"
    assert rollout.sidecars() == []


def test_the_dry_run_of_an_already_acknowledged_plan_finds_nothing_to_do(
    rollout: Rollout,
) -> None:
    acknowledge_runtime_schema_rollout_preparation(rollout.root, now=LATER)

    results = acknowledge_runtime_schema_rollout_preparation(
        rollout.root, now=LATER + timedelta(minutes=1), dry_run=True
    )

    assert all(item.changed is False for item in results)
    assert all(item.phase_before is RolloutPhase.DUAL_WRITE for item in results)


# ---------------------------------------------------------------------------------------
# Fail closed
# ---------------------------------------------------------------------------------------


def _swing_current_to_the_other_generation(rollout: Rollout) -> None:
    current = rollout.root / "current"
    generations = sorted(
        path.name for path in (rollout.root / "generations").iterdir() if path.is_dir()
    )
    other = next(name for name in generations if name != rollout.generation_id)
    current.unlink()
    current.symlink_to(f"generations/{other}")


def test_a_plan_that_targets_another_generation_keeps_its_phase(rollout: Rollout) -> None:
    """Generation-bound like the rollout controller: `current` decides which plans advance."""

    revisions = {plan_id: rollout.revision(plan_id) for plan_id in rollout.plan_ids}
    _swing_current_to_the_other_generation(rollout)

    results = acknowledge_runtime_schema_rollout_preparation(rollout.root, now=LATER)

    assert {item.skipped_reason for item in results} == {"not_current_generation"}
    assert all(item.advanced is False for item in results)
    assert all(item.acknowledged_producers == () for item in results)
    assert all(rollout.phase(plan_id) is RolloutPhase.PREPARE for plan_id in rollout.plan_ids)
    assert {plan_id: rollout.revision(plan_id) for plan_id in rollout.plan_ids} == revisions


def test_a_plan_that_targets_another_generation_is_still_converted(rollout: Rollout) -> None:
    """The other half of #227: a unit opens every plan's store before it knows the generation.

    `load_runtime_schema_service_bindings` walks `control/schema-rollouts` and opens each
    plan's state store, and only then reads its authority to see whether the plan is for this
    generation. So one store left in WAL by a previous generation stops every kind-backed
    role, exactly the way #227 did — leaving last generation's plans unconverted would put the
    failure straight back. Converting them changes nothing but the journal header.
    """

    for plan_id in rollout.plan_ids:
        rollout.make_wal(plan_id)
    revisions = {plan_id: rollout.revision_unverified(plan_id) for plan_id in rollout.plan_ids}
    _swing_current_to_the_other_generation(rollout)

    results = acknowledge_runtime_schema_rollout_preparation(rollout.root, now=LATER)

    assert {item.skipped_reason for item in results} == {"not_current_generation"}
    assert all(item.journal_mode_before == "wal" for item in results)
    assert all(item.journal_mode_after == "rollback" for item in results)
    assert all(item.converted for item in results)
    for plan_id in rollout.plan_ids:
        assert persisted_rollout_journal_layout(rollout.state_path(plan_id)) == "rollback"
        #: converted, and nothing else: no event on the chain, no phase moved
        assert rollout.revision(plan_id) == revisions[plan_id]
        assert rollout.phase(plan_id) is RolloutPhase.PREPARE
    assert rollout.sidecars() == []


def test_a_runtime_root_without_a_current_generation_is_refused(tmp_path: Path) -> None:
    root = tmp_path / "runtime"
    (root / "control" / "schema-rollouts").mkdir(parents=True)

    with pytest.raises(RuntimeSchemaCompatibilityError, match="no current generation"):
        acknowledge_runtime_schema_rollout_preparation(root, now=LATER)


def test_a_runtime_root_with_no_rollouts_at_all_is_an_empty_result(rollout: Rollout) -> None:
    """The first window's shape: a bootstrap install prepares no plan, and this says so."""

    for plan_id in rollout.plan_ids:
        for path in sorted(
            (rollout.root / "control" / "schema-rollouts" / plan_id).iterdir(), reverse=True
        ):
            path.unlink()
        (rollout.root / "control" / "schema-rollouts" / plan_id).rmdir()

    assert acknowledge_runtime_schema_rollout_preparation(rollout.root, now=LATER) == ()


def test_the_read_only_store_is_what_the_preview_opens(rollout: Rollout) -> None:
    """A preview that opened a writer would convert, lock, and be indistinguishable here."""

    opened: list[bool] = []
    real = SchemaRolloutStore.__init__

    def observe(self: SchemaRolloutStore, *args: Any, **kwargs: Any) -> None:
        opened.append(bool(kwargs.get("read_only", False)))
        real(self, *args, **kwargs)

    SchemaRolloutStore.__init__ = observe  # type: ignore[method-assign]
    try:
        acknowledge_runtime_schema_rollout_preparation(rollout.root, now=LATER, dry_run=True)
    finally:
        SchemaRolloutStore.__init__ = real  # type: ignore[method-assign]

    assert opened
    assert all(opened), "the preview opened a writable rollout store"


# ---------------------------------------------------------------------------------------
# The command an operator runs, between installing the bundle and starting the units
# ---------------------------------------------------------------------------------------


def _run_cli(
    argv: list[str],
    capsys: pytest.CaptureFixture[str],
    *,
    expected_code: int = 0,
) -> dict[str, Any]:
    from rquant.cli import build_parser, cmd_runtime_schema_rollout

    arguments = build_parser().parse_args(argv)
    assert cmd_runtime_schema_rollout(arguments) == expected_code
    return json.loads(capsys.readouterr().out)


def test_the_command_previews_then_applies(
    rollout: Rollout, capsys: pytest.CaptureFixture[str]
) -> None:
    root = str(rollout.root)

    preview = _run_cli(
        ["runtime-schema-rollout", "acknowledge", "--runtime-root", root, "--dry-run"], capsys
    )
    assert preview["status"] == "dry_run"
    assert preview["plans"] == len(rollout.plan_ids)
    assert preview["changed"] == len(rollout.plan_ids)
    assert all(rollout.phase(plan_id) is RolloutPhase.PREPARE for plan_id in rollout.plan_ids)

    applied = _run_cli(["runtime-schema-rollout", "acknowledge", "--runtime-root", root], capsys)
    assert applied["status"] == "applied"
    assert applied["changed"] == len(rollout.plan_ids)
    assert {item["phase_after"] for item in applied["acknowledgements"]} == {"dual_write"}
    assert all(rollout.phase(plan_id) is RolloutPhase.DUAL_WRITE for plan_id in rollout.plan_ids)

    repeated = _run_cli(["runtime-schema-rollout", "acknowledge", "--runtime-root", root], capsys)
    assert repeated["changed"] == 0


def test_the_command_reports_the_journal_conversion_per_plan(
    rollout: Rollout, capsys: pytest.CaptureFixture[str]
) -> None:
    """An operator has to be able to see that the sixteen WAL stores really converted."""

    plan_id = rollout.plan_ids[0]
    rollout.make_wal(plan_id)

    applied = _run_cli(
        ["runtime-schema-rollout", "acknowledge", "--runtime-root", str(rollout.root)], capsys
    )
    converted = next(item for item in applied["acknowledgements"] if item["plan_id"] == plan_id)

    assert converted["journal_mode_before"] == "wal"
    assert converted["journal_mode_after"] == "rollback"


# ---------------------------------------------------------------------------------------
# The ten-minute window the production profile really gives (MF1)
# ---------------------------------------------------------------------------------------


def test_the_fixture_uses_the_window_the_production_profile_gives(rollout: Rollout) -> None:
    """Otherwise none of the cases below mean anything.

    `runtime_production_profile.schema_rollout_stage_timeout_seconds` defaults to 600, and
    `install_runtime_deployment_profile` makes each plan's deadline `started_at + that`. A
    fixture with a ten-year deadline hides every expiry path, which is how the first round of
    this package shipped a command that could not run on the host at all.
    """

    for plan_id in rollout.plan_ids:
        authority, _store = load_runtime_schema_rollout(
            rollout.root, plan_id=plan_id, read_only=True
        )
        assert authority.plan.deadline - authority.plan.started_at == STAGE_TIMEOUT


def test_an_expired_plan_is_reopened_once_and_then_carried(rollout: Rollout) -> None:
    """The host's normal case: installing a bundle and acknowledging it takes over ten minutes.

    A plan whose window has closed can be neither acknowledged nor advanced — `_validate_time`
    refuses both — so without this it is a dead end. The installer restarts the plan's own
    window once, on the plan's own terms: `now` plus exactly `deadline - started_at`.
    """

    results = acknowledge_runtime_schema_rollout_preparation(rollout.root, now=EXPIRED)

    for item in results:
        assert item.skipped_reason is None, item
        assert item.deadline_reopened is True
        assert item.phase_after is RolloutPhase.DUAL_WRITE
    assert all(rollout.phase(plan_id) is RolloutPhase.DUAL_WRITE for plan_id in rollout.plan_ids)
    for plan_id in rollout.plan_ids:
        _authority, store = load_runtime_schema_rollout(
            rollout.root, plan_id=plan_id, read_only=True
        )
        reopened = store.deadline_reopened_until(plan_id)
        assert reopened == EXPIRED + STAGE_TIMEOUT
        assert store.effective_deadline(plan_id) == reopened


def test_the_reopen_is_on_the_hash_chain_and_says_who_did_it(rollout: Rollout) -> None:
    """Auditable like every acknowledgement around it, and identifiable as the installer's."""

    acknowledge_runtime_schema_rollout_preparation(rollout.root, now=EXPIRED)
    plan_id = rollout.plan_ids[0]
    _authority, store = load_runtime_schema_rollout(rollout.root, plan_id=plan_id, read_only=True)

    receipts = store.receipts(plan_id)
    reopens = [item for item in receipts if item.event_type == "deadline_reopen"]

    assert len(reopens) == 1
    assert reopens[0].operation_id == f"installer-deadline-reopen:{plan_id}"
    payload = json.loads(reopens[0].payload_json)
    assert payload["action"] == "deadline_reopen"
    assert payload["window_seconds"] == int(STAGE_TIMEOUT.total_seconds())
    assert payload["resulting_phase"] == RolloutPhase.PREPARE.value
    #: it is the first event, before any acknowledgement — the clock is restarted, then used
    assert receipts.index(reopens[0]) < min(
        index for index, item in enumerate(receipts) if item.event_type == "participant_ack"
    )


def test_the_window_is_restarted_not_removed(rollout: Rollout) -> None:
    """A reopen buys exactly one more of the profile's own windows, not an open lease."""

    plan_id = rollout.plan_ids[0]
    _authority, store = load_runtime_schema_rollout(rollout.root, plan_id=plan_id)
    store.reopen_deadline(
        plan_id=plan_id,
        expected_revision=store.get_state(plan_id).revision,
        now=EXPIRED,
        operation_id=f"installer-deadline-reopen:{plan_id}",
    )

    assert store.effective_deadline(plan_id) == EXPIRED + STAGE_TIMEOUT
    with pytest.raises(ValueError, match="deadline has expired"):
        store.advance(
            plan_id=plan_id,
            expected_revision=store.get_state(plan_id).revision,
            target_phase=RolloutPhase.DUAL_WRITE,
            now=EXPIRED + STAGE_TIMEOUT + timedelta(seconds=1),
            operation_id="past-the-reopened-window",
        )


def test_the_store_itself_allows_only_one_reopen(rollout: Rollout) -> None:
    """Once is enforced where it cannot be routed around, not in the caller.

    Still in PREPARE, so the phase rule is not what refuses: a second reopen of the same plan
    is refused on its own terms, which is what keeps this from becoming an open-ended lease.
    """

    plan_id = rollout.plan_ids[0]
    _authority, store = load_runtime_schema_rollout(rollout.root, plan_id=plan_id)
    store.reopen_deadline(
        plan_id=plan_id,
        expected_revision=store.get_state(plan_id).revision,
        now=EXPIRED,
        operation_id=f"installer-deadline-reopen:{plan_id}",
    )
    assert store.get_state(plan_id).phase is RolloutPhase.PREPARE

    with pytest.raises(ValueError, match="already been reopened once"):
        store.reopen_deadline(
            plan_id=plan_id,
            expected_revision=store.get_state(plan_id).revision,
            now=EXPIRED + STAGE_TIMEOUT + timedelta(seconds=1),
            operation_id=f"installer-deadline-reopen:{plan_id}:again",
        )


def test_a_plan_whose_reopen_is_spent_is_reported_not_raised(rollout: Rollout) -> None:
    """The whole report is printed, every plan gets a line, and the exit code says look."""

    plan_id = rollout.plan_ids[0]
    _authority, store = load_runtime_schema_rollout(rollout.root, plan_id=plan_id)
    store.reopen_deadline(
        plan_id=plan_id,
        expected_revision=store.get_state(plan_id).revision,
        now=EXPIRED,
        operation_id=f"installer-deadline-reopen:{plan_id}",
    )
    beyond = EXPIRED + STAGE_TIMEOUT + timedelta(seconds=1)

    results = acknowledge_runtime_schema_rollout_preparation(rollout.root, now=beyond)
    spent = next(item for item in results if item.plan_id == plan_id)

    assert len(results) == len(rollout.plan_ids), "every plan is still reported"
    assert spent.skipped_reason == "deadline_expired"
    assert spent.detail is not None and "reopen is already spent" in spent.detail
    assert spent.advanced is False
    assert rollout.phase(plan_id) is RolloutPhase.PREPARE
    #: and the plans whose one reopen is still available are carried, in the same run
    others = [item for item in results if item.plan_id != plan_id]
    assert others and all(item.phase_after is RolloutPhase.DUAL_WRITE for item in others)


def test_the_preview_reaches_the_same_verdict_about_an_expired_plan(rollout: Rollout) -> None:
    """A preview that promised what the apply cannot do is what MF1 was."""

    plan_id = rollout.plan_ids[0]
    _authority, store = load_runtime_schema_rollout(rollout.root, plan_id=plan_id)
    store.reopen_deadline(
        plan_id=plan_id,
        expected_revision=store.get_state(plan_id).revision,
        now=EXPIRED,
        operation_id=f"installer-deadline-reopen:{plan_id}",
    )
    beyond = EXPIRED + STAGE_TIMEOUT + timedelta(seconds=1)

    preview = acknowledge_runtime_schema_rollout_preparation(rollout.root, now=beyond, dry_run=True)
    applied = acknowledge_runtime_schema_rollout_preparation(rollout.root, now=beyond)

    assert [item.skipped_reason for item in preview] == [item.skipped_reason for item in applied]
    assert [item.deadline_reopened for item in preview] == [
        item.deadline_reopened for item in applied
    ]
    assert [item.advanced for item in preview] == [item.advanced for item in applied]


def test_the_preview_of_an_expired_plan_announces_the_reopen(rollout: Rollout) -> None:
    """And it does not perform it: the plan is untouched afterwards."""

    digests = rollout.digest()

    preview = acknowledge_runtime_schema_rollout_preparation(
        rollout.root, now=EXPIRED, dry_run=True
    )

    assert all(item.deadline_reopened for item in preview)
    assert all(item.advanced for item in preview)
    assert rollout.digest() == digests
    for plan_id in rollout.plan_ids:
        _authority, store = load_runtime_schema_rollout(
            rollout.root, plan_id=plan_id, read_only=True
        )
        assert store.deadline_reopened_until(plan_id) is None


def test_the_installer_may_not_reopen_a_plan_that_has_left_prepare(rollout: Rollout) -> None:
    """The ruling stops at PREPARE: a later phase waits on evidence, not on a clock."""

    acknowledge_runtime_schema_rollout_preparation(rollout.root, now=LATER)
    plan_id = rollout.plan_ids[0]
    _authority, store = load_runtime_schema_rollout(rollout.root, plan_id=plan_id)
    assert store.get_state(plan_id).phase is RolloutPhase.DUAL_WRITE

    with pytest.raises(ValueError, match="only a preparing rollout"):
        store.reopen_deadline(
            plan_id=plan_id,
            expected_revision=store.get_state(plan_id).revision,
            now=EXPIRED,
            operation_id=f"installer-deadline-reopen:{plan_id}",
        )


# ---------------------------------------------------------------------------------------
# The door the whole "DUAL_WRITE is a safe ceiling" argument rests on
# ---------------------------------------------------------------------------------------


def test_leaving_dual_write_still_needs_the_producers_own_evidence(rollout: Rollout) -> None:
    """Why stopping at DUAL_WRITE gives nothing away, stated as a failing transition.

    The installer signs the PREPARE round because every argument that round takes comes off
    the frozen plan. The next step is not like that: `_validate_phase_exit` will not let a
    plan out of DUAL_WRITE until the store holds dual-write consistency evidence, which only
    a running producer writes. That door is what makes the ceiling safe, so it is asserted
    here rather than assumed.
    """

    acknowledge_runtime_schema_rollout_preparation(rollout.root, now=LATER)
    plan_id = rollout.plan_ids[0]
    _authority, store = load_runtime_schema_rollout(rollout.root, plan_id=plan_id)

    with pytest.raises(ValueError, match="dual_write lacks consistency evidence"):
        store.advance(
            plan_id=plan_id,
            expected_revision=store.get_state(plan_id).revision,
            target_phase=RolloutPhase.CONSUMER_ACK,
            now=LATER + timedelta(seconds=1),
            operation_id="advance-without-evidence",
        )

    assert rollout.phase(plan_id) is RolloutPhase.DUAL_WRITE


def test_the_command_exits_two_when_a_plan_is_out_of_reopens(
    rollout: Rollout, capsys: pytest.CaptureFixture[str]
) -> None:
    """The exit code is the only part of the output a shell reads, so it has to be right.

    A plan whose window closed and whose one reopen is spent cannot be carried by this
    command at all — a person has to decide. The report is still complete and the other plans
    are still carried; what says "look at this" is the exit code.
    """

    plan_id = rollout.plan_ids[0]
    _authority, store = load_runtime_schema_rollout(rollout.root, plan_id=plan_id)
    store.reopen_deadline(
        plan_id=plan_id,
        expected_revision=store.get_state(plan_id).revision,
        now=EXPIRED,
        operation_id=f"installer-deadline-reopen:{plan_id}",
    )
    beyond = EXPIRED + STAGE_TIMEOUT + timedelta(seconds=1)

    class _Clock:
        @staticmethod
        def now(tz: Any = None) -> datetime:  # noqa: ARG004 - the CLI always passes UTC
            return beyond

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr("rquant.cli.datetime", _Clock)
        report = _run_cli(
            ["runtime-schema-rollout", "acknowledge", "--runtime-root", str(rollout.root)],
            capsys,
            expected_code=2,
        )

    assert report["deadline_expired"] == 1
    assert report["plans"] == len(rollout.plan_ids), "every plan is still reported"
    assert len(report["acknowledgements"]) == len(rollout.plan_ids)
    spent = next(item for item in report["acknowledgements"] if item["plan_id"] == plan_id)
    assert spent["skipped_reason"] == "deadline_expired"
    #: the rest of the run still happened
    others = [item for item in report["acknowledgements"] if item["plan_id"] != plan_id]
    assert others and all(item["phase_after"] == "dual_write" for item in others)


def test_a_reopen_signed_with_someone_elses_operation_id_is_refused(rollout: Rollout) -> None:
    """The prefix is what makes a reopen identifiable in `receipts()` afterwards.

    Enforced in the store, not in the caller: a rule the caller could choose to skip would
    not survive the first other caller, and then the chain would carry a deadline change
    nobody could attribute.
    """

    plan_id = rollout.plan_ids[0]
    _authority, store = load_runtime_schema_rollout(rollout.root, plan_id=plan_id)
    revision = store.get_state(plan_id).revision

    with pytest.raises(ValueError, match="installer-deadline-reopen"):
        store.reopen_deadline(
            plan_id=plan_id,
            expected_revision=revision,
            now=EXPIRED,
            operation_id=f"service-prepare:{plan_id}",
        )

    assert store.get_state(plan_id).revision == revision
    assert store.deadline_reopened_until(plan_id) is None


def test_reopening_a_window_that_is_still_open_is_refused(rollout: Rollout) -> None:
    """Spending the single use early would hand the plan more than one window."""

    plan_id = rollout.plan_ids[0]
    _authority, store = load_runtime_schema_rollout(rollout.root, plan_id=plan_id)
    revision = store.get_state(plan_id).revision

    with pytest.raises(ValueError, match="has not expired"):
        store.reopen_deadline(
            plan_id=plan_id,
            expected_revision=revision,
            now=LATER,
            operation_id=f"installer-deadline-reopen:{plan_id}",
        )

    assert store.get_state(plan_id).revision == revision
    assert store.deadline_reopened_until(plan_id) is None
    assert store.effective_deadline(plan_id) == DEADLINE


def test_a_reopen_moves_the_later_phases_windows_too(rollout: Rollout) -> None:
    """Stated because it is a consequence an operator has to know, not a side effect.

    `_validate_time` gates every later mutation of the plan, so restarting the clock moves
    the whole remaining rollout — the producers' dual-write records and the consumers'
    receipts — later by the same one window.
    """

    acknowledge_runtime_schema_rollout_preparation(rollout.root, now=EXPIRED)
    plan_id = rollout.plan_ids[0]
    _authority, store = load_runtime_schema_rollout(rollout.root, plan_id=plan_id)
    assert store.get_state(plan_id).phase is RolloutPhase.DUAL_WRITE

    #: past the plan's own deadline, inside the reopened one: a DUAL_WRITE write is accepted
    #: as far as its own rules take it, and is not stopped by the clock
    with pytest.raises(ValueError, match="dual_write lacks consistency evidence"):
        store.advance(
            plan_id=plan_id,
            expected_revision=store.get_state(plan_id).revision,
            target_phase=RolloutPhase.CONSUMER_ACK,
            now=EXPIRED + timedelta(seconds=60),
            operation_id="inside-the-reopened-window",
        )

    #: past the reopened one, the clock is what stops it again
    with pytest.raises(ValueError, match="deadline has expired"):
        store.advance(
            plan_id=plan_id,
            expected_revision=store.get_state(plan_id).revision,
            target_phase=RolloutPhase.CONSUMER_ACK,
            now=EXPIRED + STAGE_TIMEOUT + timedelta(seconds=1),
            operation_id="past-the-reopened-window",
        )
