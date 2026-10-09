from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4, uuid5

import pytest

from rquant.experiment_registry import (
    DateRange,
    ExperimentIdentityConflictError,
    ExperimentRegistry,
)
from rquant.strategy_authoring import StrategyAuthoringStore
from rquant.strategy_promotion_commands import RunStrategyWalkForward
from rquant.strategy_promotion_contracts import PromotionEvidenceSelection
from rquant.strategy_promotion_walk_forward import (
    StrategyPromotionWalkForwardPlan,
    build_walk_forward_plan,
)
from rquant.topn_walk_forward import build_expanding_folds
from tests.unit.test_strategy_promotion import NOW, saved_target, validation_bundle


def test_native_reference_plan_retains_original_sixfold_ids_and_registry(tmp_path: Path) -> None:
    from rquant import strategy_promotion_walk_forward as wf
    from rquant.minute_backtest_formal import MinuteExperimentProtocol
    from rquant.strategy_promotion_contracts import NativeMinuteSelection

    assert hasattr(wf, "build_native_walk_forward_plan"), "native fixed sixfold plan is missing"
    _, _, _, original = plan_for(tmp_path)
    target = original.request.target.model_copy(update={"source_kind": "builtin", "strategy_id": "n_shape"})
    request = original.request.model_copy(update={"target": target})
    parent = original.parent.model_copy(update={"target": target})
    selection = NativeMinuteSelection(target=target, source_key="synthetic.original-minute",
        source_version=1, profile_hash="f" * 64)
    protocol = MinuteExperimentProtocol(train_range=parent.train_window, validation_range=parent.window,
        frozen_outer_test_range=DateRange(start_date=date(2026, 3, 1), end_date=date(2026, 3, 2)))
    plan = wf.build_native_walk_forward_plan(request, metadata_identity=original.metadata_identity,
        parent=parent, dates=original.dates, calendar_source_identity=original.calendar_source_identity,
        selection=selection, protocol=protocol)
    assert plan.complete_for_promotion and len(plan.folds) == 6
    assert len(plan.model_dump_json().encode()) < 128 * 1024
    for fold in plan.folds:
        assert fold.job_id == uuid5(UUID(request.command_id), f"strategy-fixed-wf:{fold.index}")
        assert fold.configuration.selection == selection
        assert (fold.configuration.start_date, fold.configuration.end_date) == (fold.train_dates[0], fold.test_dates[-1])
        assert not any(protocol.frozen_outer_test_range.start_date <= d <= protocol.frozen_outer_test_range.end_date
            for d in fold.train_dates + fold.test_dates)
    family_request = plan.family_request()
    assert family_request.walk_forward_command_id == UUID(request.command_id)
    assert family_request.walk_forward_plan_hash == plan.fingerprint
    registry = ExperimentRegistry(tmp_path / "native-wf.sqlite", managed_trust_root=tmp_path)
    from rquant.experiment_platform import ExperimentPlatformStore, experiment_family_job
    from rquant.runtime_contracts import canonical_sha256

    platform = ExperimentPlatformStore(registry, activate_private_schema=True)
    with pytest.raises(ValueError, match="reference plan"):
        platform.begin_request(owner=target.owner_id, request_id=UUID(request.command_id),
            body_hash=canonical_sha256(family_request), request=family_request, registered_at=NOW)
    assert registry.record_walk_forward_plan(plan, actor_id=target.owner_id) == plan
    assert registry.walk_forward_plan(request, actor_id=target.owner_id) == plan
    assert registry.walk_forward_plan_by_id(UUID(request.command_id), actor_id=target.owner_id) == plan
    record = platform.begin_request(owner=target.owner_id, request_id=UUID(request.command_id),
        body_hash=canonical_sha256(family_request), request=family_request, registered_at=NOW)
    assert tuple(experiment_family_job(record, i) for i in range(len(plan.folds))) == tuple(
        fold.job_id for fold in plan.folds)
    with pytest.raises(ValueError, match="reference plan"):
        platform.begin_request(owner=target.owner_id, request_id=uuid4(),
            body_hash=canonical_sha256(family_request), request=family_request, registered_at=NOW)
    with pytest.raises(PermissionError):
        registry.walk_forward_plan(request, actor_id="other")
    with pytest.raises(ExperimentIdentityConflictError):
        registry.walk_forward_plan(request.model_copy(update={"generation_id": "changed"}), actor_id=target.owner_id)
    with pytest.raises(ExperimentIdentityConflictError):
        registry.record_walk_forward_plan(plan.model_copy(update={"calendar_source_identity": "2" * 64}), actor_id=target.owner_id)


def plan_for(
    tmp_path: Path, *, days: int = 18
) -> tuple[
    StrategyAuthoringStore,
    RunStrategyWalkForward,
    tuple[date, ...],
    StrategyPromotionWalkForwardPlan,
]:
    store, target = saved_target(tmp_path)
    request = RunStrategyWalkForward(
        command_id=str(uuid4()),
        requested_at=NOW,
        generation_id="original-generation",
        target=target,
        fold_count=6,
        selection=PromotionEvidenceSelection(family_id="original-parent", experiment_id="7" * 64),
    )
    calendar = tuple(date(2025, 1, 1) + timedelta(days=i) for i in range(days))
    protocol_train = DateRange(start_date=calendar[0], end_date=calendar[5])
    protocol_validation = DateRange(start_date=calendar[6], end_date=calendar[-1])
    bound = validation_bundle(target).validation.model_copy(
        update={"train_window": protocol_train, "window": protocol_validation}
    )
    plan = build_walk_forward_plan(
        request,
        metadata_identity=store.identity(),
        parent=bound,
        dates=calendar,
        calendar_source_identity="1" * 64,
        initial_cash=Decimal("100000"),
    )
    return store, request, calendar, plan


def test_folds_use_original_builder_past_only_fixed_version_and_child_uuid(tmp_path: Path) -> None:
    store, request, calendar, plan = plan_for(tmp_path)
    expected = build_expanding_folds(list(calendar), fold_count=6, min_train_dates=6)
    assert len(plan.folds) == 6
    for fold, original in zip(plan.folds, expected, strict=True):
        assert fold.train_dates == tuple(original.train_dates)
        assert fold.test_dates == tuple(original.test_dates)
        assert fold.train_dates[-1] < fold.test_dates[0]
        assert fold.request.head == fold.request.expected_head == request.target.head
        assert fold.request.initial_cash == Decimal("100000")
        assert fold.request.start_date == calendar[0]
        assert fold.request.end_date == fold.test_dates[-1]
    again = build_walk_forward_plan(
        request,
        metadata_identity=store.identity(),
        parent=plan.parent,
        dates=calendar,
        calendar_source_identity="1" * 64,
        initial_cash=Decimal("100000"),
    )
    assert again == plan and again.fingerprint == plan.fingerprint
    changed = request.model_copy(update={"command_id": str(uuid4())})
    other = build_walk_forward_plan(
        changed,
        metadata_identity=store.identity(),
        parent=plan.parent,
        dates=calendar,
        calendar_source_identity="1" * 64,
        initial_cash=Decimal("100000"),
    )
    assert set(f.request.command_id for f in plan.folds).isdisjoint(
        f.request.command_id for f in other.folds
    )


def test_short_actual_calendar_does_not_invent_six_folds(tmp_path: Path) -> None:
    _, _, _, plan = plan_for(tmp_path, days=9)
    assert len(plan.folds) == 3
    assert not plan.complete_for_promotion


def test_outer_dates_unsorted_dates_and_parent_mismatch_are_rejected(tmp_path: Path) -> None:
    store, request, calendar, plan = plan_for(tmp_path)
    for dates in (tuple(reversed(calendar)), calendar + (date(2025, 7, 1),)):
        with pytest.raises(ValueError, match="date|interval|calendar"):
            build_walk_forward_plan(
                request,
                metadata_identity=store.identity(),
                parent=plan.parent,
                dates=dates,
                calendar_source_identity="1" * 64,
                initial_cash=Decimal("100000"),
            )
    changed = request.model_copy(
        update={"target": request.target.model_copy(update={"owner_id": "bob"})}
    )
    with pytest.raises(ValueError, match="parent|target"):
        build_walk_forward_plan(
            changed,
            metadata_identity=store.identity(),
            parent=plan.parent,
            dates=calendar,
            calendar_source_identity="1" * 64,
            initial_cash=Decimal("100000"),
        )


def test_original_registry_stores_one_immutable_plan_and_full_body_before_children(
    tmp_path: Path,
) -> None:
    _, request, _, plan = plan_for(tmp_path)
    registry = ExperimentRegistry(tmp_path / "registry.sqlite", managed_trust_root=tmp_path)
    assert registry.walk_forward_plan(request, actor_id="alice") is None
    assert registry.record_walk_forward_plan(plan, actor_id="alice") == plan
    assert registry.walk_forward_plan(request, actor_id="alice") == plan
    assert registry.record_walk_forward_plan(plan, actor_id="alice") == plan
    with pytest.raises(PermissionError):
        registry.walk_forward_plan(request, actor_id="bob")
    with pytest.raises(ExperimentIdentityConflictError, match="body|original|conflict"):
        registry.walk_forward_plan(request.model_copy(update={"fold_count": 5}), actor_id="alice")
    with pytest.raises(ExperimentIdentityConflictError, match="immutable|conflict"):
        registry.record_walk_forward_plan(
            plan.model_copy(
                update={
                    "metadata_identity": plan.metadata_identity.model_copy(
                        update={"instance_id": uuid4().hex}
                    )
                }
            ),
            actor_id="alice",
        )


def original_wf_submission_fixture(tmp_path: Path):
    from rquant.lab_artifact_preview import ArtifactPreviewReader
    from rquant.runtime_contracts import canonical_sha256
    from rquant.strategy_promotion_contracts import StrategyPromotionTarget
    from rquant.strategy_promotion_walk_forward import StrategyPromotionWalkForwardBackend
    from rquant.strategy_template_adapter import build_strategy_template_adapter_catalog
    from rquant.strategy_template_artifact import StrategyTemplateSealedResultReader
    from tests.unit.test_strategy_template_submission import run_service

    store, _, runs, original, _ = run_service(tmp_path)
    catalog = build_strategy_template_adapter_catalog(
        store,
        expected_identity=store.identity(),
        selected_keys=((original.strategy_id, original.head.version),),
    )
    version = catalog.versions[0]
    source = runs.preparer.source_provider("alice", original.generation_id, version)
    dates = tuple(day.trade_date for day in source.portfolio.template.days)
    target = StrategyPromotionTarget(
        source_kind="template",
        owner_id="alice",
        strategy_id=original.strategy_id,
        name=store.get_current(original.strategy_id, owner_id="alice").name,
        head=original.head,
        parameter_fingerprint=version.definition.spec.parameter_fingerprint,
        cost_fingerprint=canonical_sha256(source.portfolio.template.execution_cost_spec),
    )
    parent = validation_bundle(target).validation.model_copy(
        update={
            "train_window": DateRange(start_date=dates[0], end_date=dates[0]),
            "window": DateRange(start_date=dates[1], end_date=dates[-1]),
        }
    )
    request = RunStrategyWalkForward(
        command_id=str(uuid4()),
        requested_at=original.requested_at,
        generation_id=original.generation_id,
        target=target,
        fold_count=6,
        selection=PromotionEvidenceSelection(
            family_id=parent.parent_family, experiment_id=parent.experiment_id
        ),
    )
    plan = build_walk_forward_plan(
        request,
        metadata_identity=store.identity(),
        parent=parent,
        dates=dates,
        calendar_source_identity=source.portfolio.template.calendar.source_identity,
        initial_cash=original.initial_cash,
    )
    previews = ArtifactPreviewReader(
        reader=runs.facade.reader, artifact_root=tmp_path / "artifacts"
    )
    results = StrategyTemplateSealedResultReader(
        reader=runs.facade.reader, artifact_reader=previews
    )
    backend = StrategyPromotionWalkForwardBackend(
        registry=runs.preparer.experiments, runs=runs, results=results
    )
    return store, request, plan, target, runs, backend


def test_wf_uses_original_preparer_spool_and_restores_without_new_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, request, plan, target, runs, backend = original_wf_submission_fixture(tmp_path)
    import os

    from rquant.collaboration_commands import PageControlRoleAuthority
    from rquant.collaboration_roles import RoleEntry, RoleState
    from rquant.strategy_promotion import StrategyPromotionBackend
    from rquant.strategy_promotion_evidence import StrategyPromotionEvidenceSource

    private = tmp_path / "roles"
    private.mkdir(mode=0o700)
    role_path = private / "roles.json"
    role_path.write_text(
        RoleState.create(
            revision=1,
            users=(
                RoleEntry(username="alice", role="researcher"),
                RoleEntry(username="admin", role="admin"),
            ),
        ).model_dump_json()
    )
    os.chmod(role_path, 0o600)
    roles = PageControlRoleAuthority(mode="enforced", roles_path=role_path, clock=store.clock)
    promotion_source = object.__new__(StrategyPromotionEvidenceSource)
    promotion_source.walk_forward = backend
    planned = []

    def actual_parent_transport(
        self: StrategyPromotionEvidenceSource, requested: RunStrategyWalkForward, **kwargs: object
    ) -> object:
        planned.append(requested)
        assert requested == request
        return plan

    monkeypatch.setattr(
        StrategyPromotionEvidenceSource, "plan_walk_forward", actual_parent_transport, raising=False
    )
    domain = StrategyPromotionBackend(store, roles=roles, source=promotion_source, enabled=True)
    original_prepare = runs.preparer.prepare

    def prepare_after_plan(*args: object, **kwargs: object) -> object:
        assert backend.lookup(request, actor_id="alice") == plan
        return original_prepare(*args, **kwargs)

    monkeypatch.setattr(runs.preparer, "prepare", prepare_after_plan)
    submitted = domain.run_walk_forward(request, actor_id="alice")
    assert submitted.plan_hash == plan.fingerprint
    assert len(submitted.receipts) == len(plan.folds) == 2 and not plan.complete_for_promotion
    entries = runs.facade.spool.pending()
    assert {entry.envelope.command.job_id for entry in entries} == {
        receipt.job_id for receipt in submitted.receipts
    }

    def forbidden_prepare(*args: object, **kwargs: object) -> object:
        raise AssertionError("old UUID must use its original accepted source")

    monkeypatch.setattr(runs.preparer, "prepare", forbidden_prepare)
    role_path.write_text(
        RoleState.create(
            revision=2,
            users=(
                RoleEntry(username="alice", role="viewer"),
                RoleEntry(username="admin", role="admin"),
            ),
        ).model_dump_json()
    )
    assert domain.run_walk_forward(request, actor_id="alice") == submitted and planned == [request]
    assert len(runs.facade.spool.pending()) == 2
    # Pending jobs remain pending; no sealed success is supplied by this offline bridge.
    assert backend.read_folds(submitted.command_id, target=target, as_of=NOW) == ()


def test_original_page_control_recovers_partial_wf_from_exact_child_admissions(tmp_path: Path) -> None:
    """Real preparer/spool, synthetic Lab transport; no worker success is supplied."""
    import os
    from datetime import timedelta
    from rquant.collaboration_commands import PageControlRoleAuthority
    from rquant.collaboration_roles import RoleEntry, RoleState
    from rquant.page_control import PageControlConsumer, PageControlOutbox, PageControlService
    from rquant.strategy_promotion import StrategyPromotionBackend, StrategyPromotionPageControlBackend
    from rquant.strategy_promotion_evidence import StrategyPromotionEvidenceSource

    store, request, plan, target, runs, wf = original_wf_submission_fixture(tmp_path)
    roles_root = tmp_path / "private-roles"
    roles_root.mkdir(mode=0o700)
    roles_path = roles_root / "roles.json"
    roles_path.write_text(RoleState.create(revision=1, users=(RoleEntry(username="alice", role="researcher"), RoleEntry(username="admin", role="admin"))).model_dump_json())
    roles_path.chmod(0o600)
    clock = [NOW]
    roles = PageControlRoleAuthority(mode="enforced", roles_path=roles_path, clock=lambda: clock[0])
    source = object.__new__(StrategyPromotionEvidenceSource)
    source.walk_forward = wf
    domain = StrategyPromotionBackend(store, roles=roles, source=source, enabled=True)
    installed = StrategyPromotionPageControlBackend(domain, operator_users=frozenset({"alice"}))
    outbox = PageControlOutbox(roles_root / "page-control.sqlite")
    outbox.path.chmod(0o600)
    service = PageControlService(outbox=outbox, collaboration=roles, consumer=PageControlConsumer(outbox=outbox, data_dir=tmp_path / "data", log_dir=tmp_path / "logs", clock=lambda: clock[0], strategy_promotion_backend=installed))
    owned = installed.compile(request, actor_id="alice", expected_identity=store.identity())
    service._enqueue_strategy_promotion(owned, actor_id="alice")
    claim = outbox.claim_records(limit=1, owner_id="lost-process", lease_seconds=1, now=clock[0], target_command_id=request.command_id)[0]
    outbox.begin_effect(owned, owner_id=claim.owner_id, claim_token=claim.claim_token, now=clock[0])
    wf.registry.record_walk_forward_plan(plan, actor_id="alice")
    first_command = runs.compile(plan.folds[0].request, owner_id="alice", expected_identity=store.identity())
    first_receipt = runs.submit(first_command)
    assert len(runs.facade.spool.pending()) == 1
    clock[0] += timedelta(seconds=2)
    resumed = service._resume_trusted_strategy_promotion(request, authenticated_actor_id="alice")
    assert resumed.status.value == "succeeded"
    submission = wf.completed_submission(plan, actor_id="alice")
    assert submission is not None and submission.receipts[0] == first_receipt
    assert len(runs.facade.spool.pending()) == len(plan.folds) == 2
    assert installed.domain.store.identity() == plan.metadata_identity
    assert service._resume_trusted_strategy_promotion(request, authenticated_actor_id="alice") == resumed
    assert len(runs.facade.spool.pending()) == 2
    from rquant.strategy_authoring_admission import StrategyAuthoringAdmission
    admission = StrategyAuthoringAdmission(service, source_catalog_provider=lambda *_: None)
    exact = admission.promotion_lookup(request, authenticated_actor_id="alice")
    assert exact is not None and exact.receipt == resumed
    assert len(exact.receipt.result["receipts"]) == len(plan.folds) == 2
    assert exact.receipt.result["plan_hash"] == plan.fingerprint
