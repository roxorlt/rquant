from __future__ import annotations

import os
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest

from rquant.experiment_registry import DateRange, PromotionStage
from rquant.strategy_authoring import StrategyAuthoringStore
from rquant.strategy_authoring_commands import SaveStrategyTemplate
from rquant.strategy_authoring_source import StrategySourceCatalog
from rquant.strategy_promotion import StrategyPromotionBackend, evaluate_review
from rquant.strategy_promotion_commands import (
    ApprovePromotion,
    PreparePromotionApproval,
    RequestPromotionReview,
)
from rquant.strategy_promotion_contracts import (
    BoundValidationPromotionEvidence,
    PromotionEvidenceBundle,
    PromotionEvidenceSelection,
    SealedPromotionResult,
    StrategyPromotionReview,
    StrategyPromotionState,
    StrategyPromotionTarget,
)
from rquant.strategy_promotion_evidence import StrategyPromotionEvidenceSource
from rquant.strategy_template import StrategyTemplate

NOW = datetime(2026, 10, 6, 8, tzinfo=UTC)


def saved_target(tmp_path: Path) -> tuple[StrategyAuthoringStore, StrategyPromotionTarget]:
    store = StrategyAuthoringStore(
        tmp_path / "authoring.sqlite",
        definition_root=tmp_path / "definitions",
        producer_commit="0" * 40,
        clock=lambda: NOW,
    )
    store.initialize()
    draft = SaveStrategyTemplate(
        command_id=str(uuid4()),
        requested_at=NOW,
        generation_id="test-gen",
        name="研究策略",
        rules=StrategyTemplate.model_validate(
            {
                "entry": {"kind": "conditions", "conditions": [{"key": "not_st"}]},
                "weight_rule": {"max_positions": 10},
                "rebalance_rule": {"kind": "daily"},
            }
        ),
    )
    saved = store.save(
        draft,
        owner_id="alice",
        catalog=StrategySourceCatalog(
            owner_id="alice", generation_id="test-gen", pools=(), signals=()
        ),
    )
    target = StrategyPromotionTarget(
        source_kind="template",
        owner_id="alice",
        strategy_id=saved.strategy_id,
        name="研究策略",
        head=saved.head,
        parameter_fingerprint=store.definition_registry(saved.strategy_id)
        .read_strategy_spec(saved.head.registration_fingerprint)
        .spec.parameter_fingerprint,
        cost_fingerprint="e" * 64,
    )
    return store, target


def validation_bundle(
    target: StrategyPromotionTarget, *, count: int = 30, full_costs: bool = True
) -> PromotionEvidenceBundle:
    reference = SealedPromotionResult(
        job_id=uuid4(),
        spec_hash="1" * 64,
        manifest_hash="2" * 64,
        result_hash="3" * 64,
        input_hash="4" * 64,
        content_hash="5" * 64,
        available_at=NOW,
    )
    validation = BoundValidationPromotionEvidence(
        target=target,
        parent_family="original-parent",
        parent_manifest_hash="6" * 64,
        parent_count=2,
        experiment_id="7" * 64,
        reference=reference,
        train_window=DateRange(start_date=date(2025, 1, 1), end_date=date(2025, 2, 28)),
        window=DateRange(start_date=date(2025, 3, 1), end_date=date(2025, 6, 30)),
        full_dates_hash="8" * 64,
        returns_hash="9" * 64,
        trades_hash="a" * 64,
        closed_trades=count,
        net_return=Decimal(".2"),
        max_drawdown=Decimal(".1"),
        win_rate=Decimal(".6"),
        sharpe=Decimal("1.1"),
        full_costs=full_costs,
    )
    return PromotionEvidenceBundle(target=target, validation=validation, observed_at=NOW)


def review_for(
    store: StrategyAuthoringStore,
    target: StrategyPromotionTarget,
    bundle: PromotionEvidenceBundle,
    *,
    revision: int = 0,
) -> tuple[RequestPromotionReview, StrategyPromotionReview]:
    request = RequestPromotionReview(
        command_id=str(uuid4()),
        requested_at=NOW,
        generation_id="test-gen",
        target=target,
        expected_revision=revision,
        selection=PromotionEvidenceSelection(family_id="original-parent", experiment_id="7" * 64),
    )
    state = store.promotion_state(target)
    return request, evaluate_review(
        request, state=state, bundle=bundle, actor_id="alice", metadata_identity=store.identity()
    )


@pytest.mark.parametrize("count,eligible", [(29, False), (30, True)])
def test_comparable_reads_validation_and_does_not_require_outer_or_formal_p(
    tmp_path: Path, count: int, eligible: bool
) -> None:
    store, target = saved_target(tmp_path)
    request, review = review_for(store, target, validation_bundle(target, count=count))
    assert review.eligible is eligible and review.to_stage is PromotionStage.COMPARABLE
    assert review.from_stage is PromotionStage.EXPLORATORY
    assert store.promotion_state(target).revision == 0
    stored = store.record_promotion_review(request, review)
    assert stored == review and store.lookup_promotion_command(request, actor_id="alice") == review
    assert store.promotion_state(target).stage is PromotionStage.EXPLORATORY


def test_review_and_recovery_preserve_exact_original_body_and_actor(tmp_path: Path) -> None:
    store, target = saved_target(tmp_path)
    request, review = review_for(store, target, validation_bundle(target))
    store.record_promotion_review(request, review)
    with pytest.raises(PermissionError):
        store.lookup_promotion_command(request, actor_id="bob")
    changed = request.model_copy(update={"generation_id": "other"})
    with pytest.raises(Exception, match="original|body"):
        store.lookup_promotion_command(changed, actor_id="alice")


def test_missing_costs_deny_comparable_and_builtin_without_installation_fails(
    tmp_path: Path,
) -> None:
    store, target = saved_target(tmp_path)
    _, review = review_for(store, target, validation_bundle(target, full_costs=False))
    assert not review.eligible
    with pytest.raises(PermissionError, match="builtin|installed"):
        store.promotion_state(
            target.model_copy(update={"source_kind": "builtin", "strategy_id": "nshape"})
        )


def test_private_metadata_rejects_invented_fixed_parameters(tmp_path: Path) -> None:
    store, target = saved_target(tmp_path)
    with pytest.raises(Exception, match="parameter|definition"):
        store.promotion_state(target.model_copy(update={"parameter_fingerprint": "f" * 64}))


def test_old_save_uuid_is_checked_before_manual_tables_or_evidence(tmp_path: Path) -> None:
    from rquant.strategy_authoring import StrategyAuthoringConflict

    store, target = saved_target(tmp_path)
    request, _ = review_for(store, target, validation_bundle(target))
    with store._connection() as connection:
        old_id = connection.execute("SELECT command_id FROM command_refs LIMIT 1").fetchone()[0]
    with pytest.raises(StrategyAuthoringConflict, match="UUID|kind"):
        store.lookup_promotion_command(
            request.model_copy(update={"command_id": old_id}), actor_id="alice"
        )


def backend_fixture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[
    StrategyPromotionBackend,
    StrategyPromotionTarget,
    list[datetime],
    list[PromotionEvidenceBundle],
    list[PromotionEvidenceSelection],
    Path,
]:
    from rquant.collaboration_commands import PageControlRoleAuthority
    from rquant.collaboration_roles import RoleEntry, RoleState
    from rquant.strategy_promotion_evidence import StrategyPromotionEvidenceSource

    store, target = saved_target(tmp_path)
    directory = tmp_path / "private"
    directory.mkdir(mode=0o700)
    role_path = directory / "roles.json"
    roles_state = RoleState.create(
        revision=1,
        users=(RoleEntry(username="alice", role="admin"), RoleEntry(username="root", role="admin")),
    )
    role_path.write_text(roles_state.model_dump_json())
    os.chmod(role_path, 0o600)
    clock = [NOW]
    roles = PageControlRoleAuthority(mode="enforced", roles_path=role_path, clock=lambda: clock[0])
    bundle = [validation_bundle(target)]
    reads = []
    # Domain owner tests substitute only sealed evidence transport; role and SQLite are real.
    source = object.__new__(StrategyPromotionEvidenceSource)
    source.walk_forward = None

    def read_selection(
        self: StrategyPromotionEvidenceSource,
        target_arg: StrategyPromotionTarget,
        selection: PromotionEvidenceSelection,
        **kwargs: object,
    ) -> PromotionEvidenceBundle:
        reads.append(selection)
        return bundle[0]

    monkeypatch.setattr(
        StrategyPromotionEvidenceSource, "read_selection", read_selection, raising=False
    )
    backend = StrategyPromotionBackend(store, roles=roles, source=source, enabled=True)
    return backend, target, clock, bundle, reads, role_path


def issue_confirmation(
    backend: StrategyPromotionBackend, target: StrategyPromotionTarget
) -> tuple[RequestPromotionReview, PreparePromotionApproval, ApprovePromotion]:
    from rquant.strategy_promotion_commands import ApprovePromotion, PreparePromotionApproval

    request = RequestPromotionReview(
        command_id=str(uuid4()),
        requested_at=NOW,
        generation_id="test-gen",
        target=target,
        expected_revision=0,
        selection=PromotionEvidenceSelection(family_id="original-parent", experiment_id="7" * 64),
    )
    review = backend.review(request, actor_id="alice")
    prepare = PreparePromotionApproval(
        command_id=str(uuid4()),
        requested_at=NOW,
        generation_id="test-gen",
        target=target,
        review_id=review.review_id,
    )
    prepared = backend.prepare(prepare, actor_id="alice")
    approve = ApprovePromotion(
        command_id=str(uuid4()),
        requested_at=NOW,
        generation_id="test-gen",
        target=target,
        preparation=prepared,
        entered_name=target.name,
    )
    return request, prepare, approve


def test_manual_two_step_approval_and_old_uuid_survive_new_head_and_expiry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    backend, target, clock, _, reads, _ = backend_fixture(tmp_path, monkeypatch)
    review, prepare, request = issue_confirmation(backend, target)
    assert backend.store.promotion_state(target).revision == 0
    effect = uuid4()
    approval = backend.approve(request, actor_id="alice", effect_id=effect)
    assert approval.after.stage is PromotionStage.COMPARABLE and approval.after.revision == 1
    saved = backend.store.get_current(target.strategy_id, owner_id="alice")
    draft = SaveStrategyTemplate(
        command_id=str(uuid4()),
        requested_at=NOW,
        generation_id="test-gen",
        name="新名称",
        rules=saved.rules,
        strategy_id=target.strategy_id,
        expected_head=target.head,
        change_note="更新名称",
    )
    backend.store.save(
        draft,
        owner_id="alice",
        catalog=StrategySourceCatalog(
            owner_id="alice", generation_id="test-gen", pools=(), signals=()
        ),
    )
    clock[0] = NOW + timedelta(days=1)
    count = len(reads)
    assert backend.approve(request, actor_id="alice", effect_id=effect) == approval
    assert backend.review(review, actor_id="alice") == request.preparation.review
    assert backend.prepare(prepare, actor_id="alice") == request.preparation
    assert len(reads) == count


@pytest.mark.parametrize("fault", ["expiry", "name", "evidence", "role", "restart"])
def test_stale_confirmations_cannot_change_stage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    from rquant.collaboration_commands import PageControlRoleAuthority
    from rquant.collaboration_roles import RoleEntry, RoleState

    backend, target, clock, bundle, _, path = backend_fixture(tmp_path, monkeypatch)
    _, _, request = issue_confirmation(backend, target)
    if fault == "expiry":
        clock[0] = NOW + timedelta(seconds=120)
    if fault == "name":
        request = request.model_copy(update={"entered_name": "另一个策略"})
    if fault == "evidence":
        bundle[0] = validation_bundle(target, count=31)
    if fault == "role":
        path.write_text(
            RoleState.create(
                revision=2,
                users=(
                    RoleEntry(username="alice", role="researcher"),
                    RoleEntry(username="root", role="admin"),
                ),
            ).model_dump_json()
        )
    if fault == "restart":
        backend.roles = PageControlRoleAuthority(
            mode="enforced", roles_path=path, clock=lambda: clock[0]
        )
    with pytest.raises((PermissionError, ValueError), match="role|confirmation|changed|enabled"):
        backend.approve(request, actor_id="alice", effect_id=uuid4())
    assert backend.store.promotion_state(target).revision == 0


def test_two_valid_confirmations_have_one_actual_stage_cas(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    backend, target, _, _, _, _ = backend_fixture(tmp_path, monkeypatch)
    _, _, first = issue_confirmation(backend, target)
    _, _, second = issue_confirmation(backend, target)
    backend.approve(first, actor_id="alice", effect_id=uuid4())
    with pytest.raises(PermissionError, match="role authority") as caught:
        backend.approve(second, actor_id="alice", effect_id=uuid4())
    assert "revision" in str(caught.value.__cause__)
    assert backend.store.promotion_state(target).revision == 1


def test_preparation_persistence_requires_its_exact_original_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.strategy_authoring import StrategyAuthoringIntegrityError

    backend, target, _, _, _, _ = backend_fixture(tmp_path, monkeypatch)
    _, prepare, approval = issue_confirmation(backend, target)
    with pytest.raises(StrategyAuthoringIntegrityError, match="request|preparation"):
        backend.store.record_promotion_preparation(
            prepare.model_copy(update={"command_id": str(uuid4())}), approval.preparation
        )


def test_later_stage_cannot_switch_the_actual_human_approved_candidate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    backend, target, _, _, reads, _ = backend_fixture(tmp_path, monkeypatch)
    _, _, approval = issue_confirmation(backend, target)
    backend.approve(approval, actor_id="alice", effect_id=uuid4())
    before = len(reads)
    request = RequestPromotionReview(
        command_id=str(uuid4()),
        requested_at=NOW,
        generation_id="test-gen",
        target=target,
        expected_revision=1,
        selection=PromotionEvidenceSelection(family_id="another-parent", experiment_id="e" * 64),
    )
    with pytest.raises((PermissionError, ValueError), match="candidate|role authority"):
        backend.review(request, actor_id="alice")
    assert backend.store.promotion_state(target).revision == 1 and len(reads) == before


def test_private_projection_has_bounded_original_facts_and_no_future_approval(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.strategy_promotion_projection import (
        StrategyPromotionProjectionReader,
        validate_strategy_promotion_projections,
    )

    backend, target, _, _, _, _ = backend_fixture(tmp_path, monkeypatch)
    _, _, request = issue_confirmation(backend, target)
    backend.approve(request, actor_id="alice", effect_id=uuid4())
    reader = StrategyPromotionProjectionReader(backend.store)
    snapshot = reader.snapshot(NOW)
    assert snapshot.states[0].state.stage is PromotionStage.COMPARABLE
    assert snapshot.states[0].state.target.owner_id == "alice"
    assert all(row.owner_id == "alice" for row in snapshot.reviews)
    payloads = reader(NOW)
    assert {p.table_name for p in payloads} == {
        "strategy_manual_state",
        "strategy_manual_review",
        "strategy_manual_window",
    }
    assert all(row["owner_id"] == "alice" for p in payloads for row in p.rows)
    validate_strategy_promotion_projections({p.table_name: p for p in payloads})
    with pytest.raises(ValueError, match="partial"):
        validate_strategy_promotion_projections({p.table_name: p for p in payloads[:2]})
    changed = payloads[0].model_copy(update={"rows": ({**payloads[0].rows[0], "owner_id": "bob"},)})
    with pytest.raises(ValueError, match="owner"):
        validate_strategy_promotion_projections({p.table_name: p for p in (changed, *payloads[1:])})
    with pytest.raises(ValueError, match="future"):
        reader.snapshot(NOW - timedelta(seconds=1))


@pytest.mark.parametrize(
    "fault", ["ok", "sharpe", "p", "outer_zero", "missing_fold", "three_positive"]
)
def test_paper_gate_uses_versioned_strict_original_thresholds(tmp_path: Path, fault: str) -> None:
    from rquant.strategy_promotion_contracts import (
        BoundOuterPromotionEvidence,
        BoundWalkForwardFold,
    )
    from tests.unit.test_strategy_promotion_evidence import original_family

    store, target = saved_target(tmp_path)
    _, specs, receipt = original_family(tmp_path)
    base = validation_bundle(target).validation.model_copy(
        update={
            "parent_family": receipt.manifest.hypothesis_family,
            "parent_manifest_hash": receipt.manifest.manifest_id,
            "parent_count": 2,
            "experiment_id": specs[0].experiment_id,
            "sharpe": Decimal(".8"),
        }
    )
    outer = BoundOuterPromotionEvidence(
        target=target,
        parent_experiment_id=specs[0].experiment_id,
        outer_experiment_id="e" * 64,
        grant_hash="f" * 64,
        reference=base.reference,
        window=DateRange(start_date=date(2025, 7, 1), end_date=date(2025, 8, 31)),
        net_return=Decimal(".01"),
    )
    folds = tuple(
        BoundWalkForwardFold(
            target=target,
            index=index,
            train_dates=(date(2025, 1, 1),),
            test_dates=(date(2025, 3, index),),
            reference=base.reference,
            net_return=Decimal(".01") if index <= 4 else Decimal(0),
        )
        for index in range(1, 7)
    )
    p = Decimal(".049")
    if fault == "sharpe":
        base = base.model_copy(update={"sharpe": Decimal(".799")})
    if fault == "p":
        p = Decimal(".05")
    if fault == "outer_zero":
        outer = outer.model_copy(update={"net_return": Decimal(0)})
    if fault == "missing_fold":
        folds = folds[:-1]
    if fault == "three_positive":
        folds = tuple(
            f.model_copy(update={"net_return": Decimal(0)}) if f.index == 4 else f for f in folds
        )
    bundle = PromotionEvidenceBundle(
        target=target,
        validation=base,
        family_receipt=receipt,
        adjusted_p=p,
        adjustment_hash="a" * 64,
        outer=outer,
        folds=folds,
        observed_at=NOW,
    )
    state = StrategyPromotionState(
        target=target, stage=PromotionStage.COMPARABLE, revision=1, latest_approval_hash="b" * 64
    )
    request = RequestPromotionReview(
        command_id=str(uuid4()),
        requested_at=NOW,
        generation_id="test-gen",
        target=target,
        expected_revision=1,
        selection=PromotionEvidenceSelection(
            family_id=base.parent_family, experiment_id=base.experiment_id
        ),
    )
    review = evaluate_review(
        request, state=state, bundle=bundle, actor_id="alice", metadata_identity=store.identity()
    )
    assert review.eligible is (fault == "ok")
