"""Native WF capability uses the original prepared family and installed authorities."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from rquant.experiment_platform_commands import ExperimentCommandWriter, RegisterExperimentFamily
from rquant.runtime_contracts import canonical_sha256
from rquant.strategy_promotion_contracts import StrategyPromotionContext
from rquant.strategy_promotion_walk_forward import StrategyPromotionWalkForwardBackend
from tests.support.strategy_promotion_native_fixture import (
    build_native_promotion_fixture,
    install_native_owned_research,
)


@pytest.fixture(scope="module")
def native_owner(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SimpleNamespace]:
    monkeypatch = pytest.MonkeyPatch()
    root = tmp_path_factory.mktemp("native-wf-capability")
    fixture = build_native_promotion_fixture(root, monkeypatch)
    actor = fixture.seed.runtime.owner_id
    command = RegisterExperimentFamily(
        command_id=str(uuid4()),
        requested_at=fixture.clock(),
        actor_id=actor,
        request=fixture.request(),
    )
    writer = ExperimentCommandWriter(
        store=fixture.platform,
        commands=fixture.commands,
        prepare=fixture.preparer(),
        enabled=True,
        owners=frozenset({actor}),
    )
    fixture.platform.begin_request(
        owner=actor,
        request_id=UUID(command.command_id),
        body_hash=canonical_sha256(command),
        request=command.request,
        registered_at=fixture.clock(),
    )
    fixture.now += timedelta(seconds=6)
    marker = writer.freeze(command)
    registered = writer.submit(command, marker)
    lease = fixture.jobs.acquire_scheduler_lease(
        owner_id="native-capability-preflight", lease_seconds=60, now=fixture.clock()
    )
    try:
        for entry in fixture.commands.spool.pending(limit=64):
            receipt = fixture.jobs.apply_command(
                entry.envelope,
                lease=lease,
                now=fixture.clock(),
                submission_authority=lambda envelope, observed: (
                    fixture.commands.validate_prepared_experiment_submission(
                        envelope, observed_at=observed
                    )
                ),
            )
            assert receipt.status == "applied"
            fixture.commands.spool.ack(entry, receipt)
    finally:
        fixture.jobs.release_scheduler_lease(lease, now=fixture.clock())
    family = fixture.platform.get_request(actor, UUID(command.command_id))
    assert family is not None and family.state == "ready"
    receipts = tuple(
        fixture.platform.preparation(actor, family.family_id, index)
        for index in range(len(family.actual_configurations))
    )
    owner = install_native_owned_research(fixture, preparations=receipts)
    try:
        yield SimpleNamespace(
            fixture=fixture,
            owner=owner,
            actor=actor,
            family=family,
            command=command,
            registered=registered,
        )
    finally:
        monkeypatch.undo()


def context(value: SimpleNamespace) -> StrategyPromotionContext:
    return value.owner.page.context(
        actor_id=value.actor,
        source_kind="builtin",
        strategy_id=value.fixture.selection().target.strategy_id,
        head=value.fixture.selection().target.head,
    )


def test_exact_native_prepared_family_exposes_its_installed_wf(
    native_owner: SimpleNamespace,
) -> None:
    selected = context(native_owner)
    assert len(selected.candidates) == 1
    chosen = selected.candidates[0]
    assert chosen.target == native_owner.fixture.selection().target
    assert chosen.selection.family_id == native_owner.family.family_id
    assert chosen.parent_count == len(native_owner.family.actual_configurations)
    assert chosen.is_current and selected.can_evaluate
    assert selected.can_run_walk_forward


@pytest.mark.parametrize(
    "fault",
    [
        "not_installed",
        "reader_missing",
        "source_reader_missing",
        "writer_disabled",
        "not_operator",
        "writer_owner",
    ],
)
def test_native_wf_capability_refuses_missing_or_disabled_authority(
    native_owner: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    value, source = native_owner, native_owner.owner.source
    wf = source.walk_forward
    if fault == "not_installed":
        monkeypatch.setattr(source, "walk_forward", None)
    elif fault == "reader_missing":
        monkeypatch.setattr(wf, "native", replace(wf.native, results=None))
    elif fault == "source_reader_missing":
        monkeypatch.setattr(source, "native_results", None)
    elif fault == "writer_disabled":
        monkeypatch.setattr(wf.native.writer, "enabled", False)
    elif fault == "not_operator":
        monkeypatch.setattr(value.owner.page, "operator_users", frozenset())
    else:
        monkeypatch.setattr(wf.native.writer, "owners", frozenset())
    selected = context(value)
    assert len(selected.candidates) == 1
    assert not selected.can_run_walk_forward


def test_native_wf_cannot_use_a_different_original_metadata_owner(
    native_owner: SimpleNamespace,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.strategy_authoring import StrategyAuthoringStore

    other = StrategyAuthoringStore(
        tmp_path / "other.sqlite",
        definition_root=native_owner.fixture.definitions.root,
        producer_commit=native_owner.fixture.seed.runtime.producer_commit,
        clock=native_owner.fixture.clock,
    )
    other.initialize()
    wf = native_owner.owner.source.walk_forward
    binding = replace(wf.native, store=other, expected_identity=other.identity())
    replacement = StrategyPromotionWalkForwardBackend(registry=wf.registry, native=binding)
    monkeypatch.setattr(native_owner.owner.source, "walk_forward", replacement)
    assert not context(native_owner).can_run_walk_forward


def test_changed_native_head_is_refused_before_wf_capability(native_owner: SimpleNamespace) -> None:
    target = native_owner.fixture.selection().target
    wrong = target.head.model_copy(update={"record_hash": "0" * 64})
    with pytest.raises(PermissionError, match="exact builtin private owner"):
        native_owner.owner.page.context(
            actor_id=native_owner.actor,
            source_kind="builtin",
            strategy_id=target.strategy_id,
            head=wrong,
        )


def test_current_viewer_can_read_but_cannot_run_native_wf(native_owner: SimpleNamespace) -> None:
    from rquant.collaboration_roles import RoleEntry, RoleState

    path = native_owner.owner.roles_path
    original = path.read_bytes()
    changed = RoleState.create(
        revision=2,
        users=(
            RoleEntry(username=native_owner.actor, role="viewer"),
            RoleEntry(username="synthetic-admin", role="admin"),
        ),
    )
    path.write_text(changed.model_dump_json())
    try:
        selected = context(native_owner)
        assert len(selected.candidates) == 1
        assert not selected.can_evaluate
        assert not selected.can_run_walk_forward
    finally:
        path.write_bytes(original)


def test_original_template_without_wf_remains_unavailable(tmp_path: Path) -> None:
    from tests.support.strategy_promotion_fixture import build_original_variant_fixture

    fixture = build_original_variant_fixture(tmp_path)
    try:
        parent = fixture.record.request.template
        selected = fixture.backend.context(
            actor_id="alice",
            source_kind="template",
            strategy_id=parent.strategy_id,
            head=parent.head,
        )
        assert len(selected.candidates) == 4
        assert fixture.backend.domain.source.walk_forward is None
        assert not selected.can_run_walk_forward
    finally:
        fixture.close()
