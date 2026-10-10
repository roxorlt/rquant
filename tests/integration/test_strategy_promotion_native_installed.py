from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from rquant import experiment_platform as platform
from rquant.runtime_contracts import canonical_sha256
from rquant.experiment_registry import IncompleteHypothesisFamilyError, ExperimentIdentityConflictError
from uuid import UUID
from rquant.experiment_platform_commands import ExperimentCommandWriter, RegisterExperimentFamily
from tests.support.strategy_promotion_native_fixture import build_native_promotion_fixture
from tests.unit.test_minute_backtest_producer import NOW


def test_native_family_uses_actual_publication_time_and_original_spool(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert hasattr(platform, "NativeMinuteExperimentRequest"), "native original family request is missing"
    fixture = build_native_promotion_fixture(tmp_path, monkeypatch)
    request = fixture.request()
    command = RegisterExperimentFamily(command_id=str(uuid4()), requested_at=fixture.now,
        actor_id=fixture.seed.runtime.owner_id, request=request)
    record = fixture.platform.begin_request(owner=command.actor_id, request_id=UUID(command.command_id),
        body_hash=canonical_sha256(command),
        request=request, registered_at=fixture.now)
    original_time = record.registered_at
    fixture.now = NOW
    prepare = fixture.preparer()
    ready = prepare(record)
    assert ready.state == "ready" and ready.registered_at == original_time
    receipt = fixture.platform.preparation(record.owner, record.family_id, 0)
    prepared = receipt.prepared
    assert prepared.kind == "minute_runtime_replay"
    assert prepared.frozen.native_registration.logical_id == "n_shape"
    assert prepared.registration.logical_id == "minute_runtime_replay"
    assert prepared.formal_plan.preregistered_at == NOW > original_time
    manifest = fixture.platform.registry.get_hypothesis_family(record.family_id)
    assert manifest.preregistered_at == original_time and len(manifest.experiment_ids) == 1
    attempt = fixture.platform.registry.get_attempt(manifest.experiment_ids[0])
    assert attempt.registered_at == NOW
    assert prepare(ready) == ready and len(fixture.source_reads) == 1
    writer = ExperimentCommandWriter(store=fixture.platform, commands=fixture.commands,
        prepare=prepare, enabled=True, owners=frozenset({record.owner}))
    effect = writer.freeze(command)
    result = writer.submit(command, effect)
    pending = fixture.commands.spool.pending()
    assert len(pending) == 1 and str(pending[0].envelope.command.job_id) == result["job_ids"][0]
    assert pending[0].envelope.command.spec.strategy_execution.strategy_id == "minute_runtime_replay"
    assert writer.submit(command, effect) == result and len(fixture.commands.spool.pending()) == 1
    assert len(fixture.source_reads) == 1


def test_native_preparation_refuses_future_publication_and_wrong_owner(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert hasattr(platform, "NativeMinuteExperimentRequest"), "native original family request is missing"
    fixture = build_native_promotion_fixture(tmp_path, monkeypatch)
    record = fixture.platform.begin_request(owner=fixture.seed.runtime.owner_id,
        request_id=uuid4(), body_hash="b" * 64, request=fixture.request(), registered_at=fixture.now)
    with pytest.raises((ValueError, PermissionError), match="(publication|visible|time)"):
        fixture.preparer()(record)
    with pytest.raises(IncompleteHypothesisFamilyError):
        fixture.platform.registry.get_hypothesis_family(record.family_id)
    assert not fixture.commands.spool.pending()
    fixture.now = NOW
    fixture.seed = fixture.seed.model_copy(update={"runtime": fixture.seed.runtime.model_copy(
        update={"owner_id": "another-owner"})})
    with pytest.raises((ValueError, PermissionError), match="(owner|source|profile)"):
        fixture.preparer()(record)


def test_native_wf_uses_original_spool_lookup_and_same_plan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant import strategy_promotion_walk_forward as wf

    assert hasattr(wf, "NativeStrategyPromotionWalkForwardBinding"), "native original WF binding is missing"
    from rquant.strategy_authoring import StrategyAuthoringStore
    from rquant.strategy_promotion_commands import RunStrategyWalkForward
    from rquant.strategy_promotion_contracts import PromotionEvidenceSelection
    from tests.unit.test_strategy_promotion import validation_bundle

    fixture = build_native_promotion_fixture(tmp_path, monkeypatch)
    store = StrategyAuthoringStore(tmp_path / "promotion.sqlite", definition_root=tmp_path / "promotion-definitions",
        producer_commit=fixture.seed.runtime.producer_commit, clock=fixture.clock)
    store.initialize()
    selection = fixture.selection()
    parent = validation_bundle(selection.target).validation.model_copy(update={
        "train_window": fixture.request().protocol.train_range,
        "window": fixture.request().protocol.validation_range})
    request = RunStrategyWalkForward(command_id=str(uuid4()), requested_at=fixture.now,
        generation_id="synthetic-generation", target=selection.target, fold_count=1,
        selection=PromotionEvidenceSelection(family_id=parent.parent_family, experiment_id=parent.experiment_id))
    plan = wf.build_native_walk_forward_plan(request, metadata_identity=store.identity(), parent=parent,
        selection=selection, dates=(fixture.seed.runtime.start_date, fixture.seed.runtime.end_date),
        calendar_source_identity=fixture.seed.runtime.market_calendar.content_sha256, protocol=fixture.request().protocol)
    writer = ExperimentCommandWriter(store=fixture.platform, commands=fixture.commands, prepare=fixture.preparer(),
        enabled=True, owners=frozenset({request.target.owner_id}))
    binding = wf.NativeStrategyPromotionWalkForwardBinding(store=store, expected_identity=store.identity(), writer=writer)
    backend = wf.StrategyPromotionWalkForwardBackend(registry=fixture.platform.registry, native=binding)
    assert backend.completed_submission(plan, actor_id=request.target.owner_id) is None
    # Only the plan/preparation/admission chain is proved here. No sealed parent/fold is supplied.
    fixture.now = NOW
    accepted = backend.submit(plan, actor_id=request.target.owner_id)
    assert len(accepted.receipts) == 1 and accepted.receipts[0].job_id == plan.folds[0].job_id
    assert backend.lookup(request, actor_id=request.target.owner_id) == plan
    monkeypatch.setattr(writer.prepare, "native_phase_provider", lambda *_: (_ for _ in ()).throw(AssertionError("new source")))
    assert backend.completed_submission(plan, actor_id=request.target.owner_id) == accepted
    assert backend.submit(plan, actor_id=request.target.owner_id) == accepted
    assert len(fixture.commands.spool.pending()) == 1 and len(fixture.source_reads) == 1
    with pytest.raises(PermissionError):
        backend.completed_submission(plan, actor_id="other")
    with pytest.raises(ExperimentIdentityConflictError):
        backend.lookup(request.model_copy(update={"generation_id": "different"}), actor_id=request.target.owner_id)


@pytest.mark.parametrize("native_id", ("n_shape", "auction_gap", "growth_board_surge"))
def test_native_original_forward_peers_publish_nav_and_viewer_only_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, native_id: str, request: pytest.FixtureRequest,
) -> None:
    from rquant.collaboration_roles import RoleEntry, RoleState
    from rquant.paper_research_runtime import NativeMinuteForwardViewSource
    from tests.support.strategy_promotion_native_fixture import native_forward_source_fixture

    fixture = native_forward_source_fixture(tmp_path, monkeypatch, native_id=native_id)
    request.addfinalizer(fixture.close_owner)
    import gc
    gc.collect()
    source, runtime, now = fixture.source, fixture.runtime, fixture.clock[0]
    from rquant.strategy_live_service import publish_native_forward_close
    assert publish_native_forward_close(source, observed_at=now, completion_receipt_id=None) is None
    assert source.views.nav_series() == ()
    with pytest.raises(ValueError, match="runner/route receipt"):
        publish_native_forward_close(source, observed_at=now, completion_receipt_id="f" * 64)
    valuation = runtime.daily_valuation(now)
    assert valuation.status == "complete" and valuation.as_of == fixture.close
    assert valuation.observed_at == now and valuation.account.as_of_time == fixture.close
    assert valuation.market_pointer == fixture.raw.current(valuation.market_pointer.channel)
    nav = source.record_close(observed_at=now, published_at=now)
    view = source.read(as_of=now)
    assert nav.status == "complete" and view.status == "complete"
    assert view.frame.reconciliation.is_consistent
    assert view.frame.account.as_of_time == now and nav.account.as_of_time == fixture.close
    assert view.frame.account.snapshot_id != nav.account.snapshot_id
    assert view.frame.account.model_dump(exclude={"as_of_time", "snapshot_id"}) == nav.account.model_dump(exclude={"as_of_time", "snapshot_id"})
    assert view.frame.ledger_revision == nav.ledger_revision
    assert view.complete_comparison_dates() == (fixture.close.date(),)
    assert source.record_close(observed_at=now, published_at=now) == nav
    from rquant.paper_research_submission import PaperResearchRunBackend, PaperResearchRunPreparer
    from rquant.paper_research_commands import RunPaperPortfolioResearch
    from rquant.lab_job_center import LabCommandSubmissionFacade
    from rquant.lab_jobs import LabJobReader
    from rquant.lab_job_protocol import LabCommandSpool
    from rquant.research_catalog import ResearchCatalog
    from rquant.storage.duckdb import DuckDBStore
    private_inputs = tmp_path / "native-band-inputs"
    private_inputs.mkdir(mode=0o700)
    preparer = PaperResearchRunPreparer(sources=(source,),
        metadata_store_factory=lambda: DuckDBStore(tmp_path / "forward-metadata.duckdb"),
        research_catalog=ResearchCatalog(tmp_path / "forward-catalog.sqlite"),
        input_root=private_inputs, lake_root=tmp_path / "forward-lake",
        code_sha=runtime.producer_commit, clock=lambda: fixture.clock[0])
    assert preparer.source_for(fixture.configuration.binding.account_id, fixture.configuration.target.owner_id) is source
    with pytest.raises(PermissionError):
        preparer.source_for(fixture.configuration.binding.account_id, "another-owner")
    backend = PaperResearchRunBackend(preparer=preparer, facade=LabCommandSubmissionFacade(
        reader=LabJobReader(tmp_path / "minute/jobs.sqlite"),
        spool=LabCommandSpool(tmp_path / "minute/commands"), clock=lambda: fixture.clock[0]))
    band_request = RunPaperPortfolioResearch(command_id=str(uuid4()), requested_at=now,
        generation_id="d" * 64, account_id=fixture.configuration.binding.account_id,
        configuration_fingerprint=fixture.configuration.fingerprint, task_name="paper_backtest_band", backtest_job_id=uuid4())
    fixture.owner.domain.source.native_results = None
    with pytest.raises(PermissionError) as missing_seal:
        backend.compile(band_request, owner_id=fixture.configuration.target.owner_id,
            expected_identity=fixture.configuration.metadata_identity)
    assert isinstance(missing_seal.value.__cause__, ValueError)
    assert "原生分钟封存" in str(missing_seal.value.__cause__)
    assert not tuple(private_inputs.iterdir()) and not backend.facade.spool.pending()
    # Empty original accounts here prove the installed observation path, not a
    # synthetic profitable trade or the 20-day promotion threshold.
    fixture.roles_path.write_text(RoleState.create(revision=3, users=(
        RoleEntry(username=fixture.configuration.target.owner_id, role="viewer"),
        RoleEntry(username="root", role="admin"),
    )).model_dump_json())
    assert source.read(as_of=now) == view
    assert NativeMinuteForwardViewSource(runtime).read(as_of=now) == view
    assert backend.lookup(band_request, owner_id=fixture.configuration.target.owner_id,
        expected_identity=fixture.configuration.metadata_identity) is None
    with pytest.raises(PermissionError):
        backend.compile(band_request, owner_id=fixture.configuration.target.owner_id,
            expected_identity=fixture.configuration.metadata_identity)
    with pytest.raises(PermissionError):
        source.record_close(observed_at=now, published_at=now)


@pytest.mark.parametrize("native_id", ("n_shape", "auction_gap", "growth_board_surge"))
def test_native_daily_producer_uses_original_signals_broker_and_complete_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest,
    native_id: str,
) -> None:
    from tests.support.strategy_promotion_native_fixture import (
        native_forward_source_fixture, drive_native_forward_session,
    )

    fixture = native_forward_source_fixture(tmp_path, monkeypatch, native_id=native_id,
        completion_authority=True)
    request.addfinalizer(fixture.close_owner)
    facts = drive_native_forward_session(fixture)
    assert facts.receipt.completion_attestation is not None
    assert facts.receipt.source_id == fixture.runtime.manifest.service_id
    assert facts.receipt.high_watermark == fixture.runtime.runner.signal_high_watermark() > 0
    assert facts.signals and facts.fills and facts.orders
    assert facts.nav.status == "complete" and facts.nav.published_at == fixture.clock[0]
    assert facts.nav.close_at == fixture.close
    assert facts.nav.account.holdings and facts.nav.account.cash < fixture.runtime.broker.initial_cash
    assert facts.frame.reconciliation.is_consistent
    assert facts.nav.ledger_revision == facts.frame.ledger_revision
    assert not facts.pending_queue
    assert fixture.source.views.nav_series() == (facts.nav,)
    first = facts.nav
    fixture.strategy_step()
    fixture.broker_step()
    assert fixture.source.views.nav_series() == (first,)


def test_native_forward_rejects_a_manifest_with_another_frozen_candidate_age(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tests.support.strategy_promotion_native_fixture import native_forward_source_fixture

    with pytest.raises(ValueError, match="profile"):
        fixture = native_forward_source_fixture(tmp_path, monkeypatch, candidate_max_age_seconds=1)
        fixture.close_owner()


def test_native_owned_research_installs_original_authorities_without_sealed_substitution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.experiment_platform_commands import RegisterExperimentFamily
    from rquant.experiment_registry import PromotionStage
    from rquant.minute_backtest_artifact import MinuteSealedReplayReader
    from rquant.strategy_promotion_commands import RequestPromotionReview
    from rquant.strategy_promotion_contracts import PromotionEvidenceSelection
    from tests.support.strategy_promotion_native_fixture import (
        build_native_promotion_fixture, install_native_owned_research,
    )

    fixture = build_native_promotion_fixture(tmp_path, monkeypatch)
    request = fixture.request()
    register = RegisterExperimentFamily(command_id=str(uuid4()), requested_at=fixture.now,
        actor_id=fixture.seed.runtime.owner_id, request=request)
    original = fixture.platform.begin_request(owner=register.actor_id, request_id=UUID(register.command_id),
        body_hash=canonical_sha256(register), request=request, registered_at=fixture.now)
    fixture.now = NOW
    fixture.preparer()(original)
    preparation = fixture.platform.preparation(original.owner, original.family_id, 0)
    installed = install_native_owned_research(fixture, preparations=(preparation,))
    target = fixture.selection().target
    installed.domain._builtin(target)
    assert installed.domain.source.platform is fixture.platform
    assert installed.domain.source.projection.jobs is fixture.commands.reader
    assert type(installed.domain.source.native_results) is MinuteSealedReplayReader
    assert installed.domain.source.native_results.submission_facade is fixture.commands
    assert installed.domain.source.walk_forward.native.writer.store is fixture.platform
    manifest = fixture.platform.registry.get_hypothesis_family(original.family_id)
    review = installed.domain.review(RequestPromotionReview(command_id=str(uuid4()), requested_at=fixture.now,
        generation_id="synthetic-original-native", target=target, expected_revision=0,
        selection=PromotionEvidenceSelection(family_id=manifest.hypothesis_family,
            experiment_id=manifest.experiment_ids[0])), actor_id=target.owner_id)
    assert review.from_stage is PromotionStage.EXPLORATORY
    assert not review.eligible
    assert not installed.domain.store.promotion_approvals(owner_id=target.owner_id)
    assert not fixture.commands.spool.pending()


def test_three_native_complete_parent_sources_use_original_preparer_and_restore_uuid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import os
    from rquant.minute_backtest_formal import PreparedMinuteRequest
    from tests.support import strategy_promotion_native_fixture as sources

    assert hasattr(sources, "build_native_complete_promotion_fixture"), "complete native original source owner is missing"
    material_root = Path(os.environ.get("RQUANT_NATIVE_MATERIAL_ROOT", str(tmp_path)))
    fixture = sources.build_native_complete_promotion_fixture(material_root, monkeypatch)
    request = fixture.request()
    command = RegisterExperimentFamily(command_id=str(uuid4()), requested_at=fixture.now,
        actor_id=fixture.seed.runtime.owner_id, request=request)
    record = fixture.platform.begin_request(owner=command.actor_id, request_id=UUID(command.command_id),
        body_hash=canonical_sha256(command), request=request, registered_at=fixture.now)
    fixture.now += timedelta(seconds=1)
    ready = fixture.preparer()(record)
    family = fixture.platform.registry.get_hypothesis_family(ready.family_id)
    assert len(family.experiment_ids) == len(ready.actual_configurations) == 3
    receipts = []
    for index, seed in enumerate(fixture.seeds):
        receipt = fixture.platform.preparation(record.owner, record.family_id, index)
        prepared = receipt.prepared
        assert isinstance(prepared, PreparedMinuteRequest)
        assert prepared.frozen.native_registration == seed.native_registration
        assert prepared.frozen.runtime.execution_profile == seed.runtime.execution_profile
        assert prepared.frozen.runtime.start_date == request.protocol.train_range.start_date
        assert prepared.frozen.runtime.end_date == request.protocol.validation_range.end_date
        expected = tuple(d for d in seed.runtime.market_calendar.open_dates
            if prepared.frozen.runtime.start_date <= d <= prepared.frozen.runtime.end_date)
        assert len(expected) == 84
        assert prepared.frozen.runtime.daily_trade_dates == expected
        assert prepared.frozen.provenance.published_at == fixture.now > ready.registered_at
        assert prepared.formal_plan.spec.experiment_id in family.experiment_ids
        attempt = fixture.platform.registry.get_attempt(prepared.formal_plan.spec.experiment_id)
        assert attempt.spec == prepared.formal_plan.spec
        assert attempt.registered_at == prepared.formal_plan.preregistered_at == fixture.now
        receipts.append(receipt)
    installed = sources.install_native_owned_research(fixture, preparations=tuple(receipts))
    assert len(installed.native_reader.catalog.entries) == 3
    assert len({s.runtime.execution_profile.paper_policy.account_id for s in fixture.seeds}) == 1
    assert len({s.runtime.source_key for s in fixture.seeds}) == 3
    effect = installed.writer.freeze(command)
    accepted = installed.writer.submit(command, effect)
    assert installed.writer.submit(command, effect) == accepted
    assert len(accepted["job_ids"]) == len(record.actual_configurations) == 3
    for configuration in ready.actual_configurations:
        installed.domain._builtin(configuration.selection.target)
    assert len(fixture.source_reads) == 3
    assert len(fixture.commands.spool.pending()) == 3
    assert not installed.store.promotion_approvals(owner_id=fixture.seed.runtime.owner_id)
    (fixture.root / "complete-parent-preparation-facts.json").write_text(__import__("json").dumps({
        "family": ready.model_dump(mode="json"),
        "command": command.model_dump(mode="json"),
        "accepted": accepted,
        "parent_manifest": family.model_dump(mode="json"),
        "source_reads": len(fixture.source_reads),
        "actual_python": __import__("sys").version,
        "sealed_results": False,
        "synthetic": True,
    }, indent=2, ensure_ascii=False))


def test_native_retained_parent_restores_original_catalog_without_new_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import json
    import os
    from tests.support.strategy_promotion_native_fixture import (
        restore_native_complete_promotion_fixture, install_native_owned_research,
    )

    retained = os.environ.get("RQUANT_NATIVE_RESTORE_ROOT")
    if retained is None:
        pytest.skip("requires retained original complete parent material")
    root = Path(retained)
    facts = json.loads((root / "complete-parent-preparation-facts.json").read_text())
    command = RegisterExperimentFamily.model_validate(facts["command"])
    fixture = restore_native_complete_promotion_fixture(root, monkeypatch)
    record = fixture.platform.get_request(command.actor_id, UUID(command.command_id))
    receipts = tuple(fixture.platform.preparation(record.owner, record.family_id, i)
        for i in range(len(record.actual_configurations)))
    installed = install_native_owned_research(fixture, preparations=receipts)
    before_files = tuple(sorted(root.glob("phase-sources/*/source-seed.json")))
    restored = installed.writer.submit(command, installed.writer.freeze(command))
    assert restored == facts["accepted"]
    assert len(installed.native_reader.catalog.entries) == len(record.actual_configurations) == 3
    assert tuple(sorted(root.glob("phase-sources/*/source-seed.json"))) == before_files
    assert not fixture.source_reads
    assert len(fixture.commands.spool.pending()) == 3


def test_native_retained_parent_preflights_original_worker_and_complete_reader(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import json
    import os
    from tests.support import strategy_promotion_native_fixture as native_fixture

    material = os.environ.get("RQUANT_NATIVE_RESTORE_ROOT")
    if material is None:
        pytest.skip("retained original N=3 preparations are required for this reuse gate")
    fixture = native_fixture.restore_native_complete_promotion_fixture(Path(material), monkeypatch)
    facts = json.loads((Path(material) / "complete-parent-preparation-facts.json").read_text())
    command = RegisterExperimentFamily.model_validate(facts["command"])
    record = fixture.platform.get_request(command.actor_id, UUID(command.command_id))
    assert record is not None
    preparations = tuple(fixture.platform.preparation(record.owner, record.family_id, index)
        for index in range(len(record.actual_configurations)))
    owner = native_fixture.install_native_owned_research(fixture, preparations=preparations)
    assert owner.domain.source.walk_forward.native.results is owner.native_reader
    foundation = native_fixture.build_native_original_worker_foundation(fixture, owner=owner)
    try:
        assert foundation.commands is fixture.commands
        assert foundation.scheduler.scheduling_control._store is fixture.jobs
        assert foundation.original_worker_claim_spool()._expected_scheduling_barrier_identity == foundation.scheduler.scheduling_control.identity
        assert foundation.scheduler.adapter_registry.closed_descriptor() == foundation.adapter_registry.closed_descriptor()
        assert foundation.runtime_manifest.registry.configuration_json
        pending = fixture.commands.spool.pending()
        assert len(pending) == 3
        for entry in pending:
            fixture.commands.validate_prepared_experiment_submission(entry.envelope, observed_at=fixture.clock())
            definitions = foundation.adapter_registry.plan(entry.envelope.command.spec)
            assert definitions and all(definition.work_plan is not None for definition in definitions)
            assert foundation.reader.get_job(entry.envelope.command.job_id) is None
        assert fixture.source_reads == []
        assert foundation.catalog == owner.native_reader.catalog
    finally:
        foundation.close()


def test_native_original_worker_sets_private_tmp_before_original_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import json
    import os
    import tempfile
    import rquant.lab_worker as original_worker
    from tests.support import strategy_promotion_native_fixture as native_fixture

    material = os.environ.get("RQUANT_NATIVE_RESTORE_ROOT")
    if material is None:
        pytest.skip("retained original N=3 preparations are required for this reuse gate")
    fixture = native_fixture.restore_native_complete_promotion_fixture(Path(material), monkeypatch)
    facts = json.loads((Path(material) / "complete-parent-preparation-facts.json").read_text())
    command = RegisterExperimentFamily.model_validate(facts["command"])
    record = fixture.platform.get_request(command.actor_id, UUID(command.command_id))
    preparations = tuple(fixture.platform.preparation(record.owner, record.family_id, index)
        for index in range(len(record.actual_configurations)))
    owner = native_fixture.install_native_owned_research(fixture, preparations=preparations)
    foundation = native_fixture.build_native_original_worker_foundation(fixture, owner=owner)
    job_id = UUID(facts["accepted"]["job_ids"][0])
    spec = preparations[0].prepared.submission(job_id=job_id).command.spec
    before = tuple((job.job_id, job.status, job.version) for job in (
        foundation.reader.get_job(UUID(value)) for value in facts["accepted"]["job_ids"]))
    real = tmp_path / "original-temporary-directory"
    real.mkdir(mode=0o700)
    alias = tmp_path / "mac-temporary-alias"
    alias.symlink_to(real, target_is_directory=True)
    monkeypatch.setattr(tempfile, "tempdir", str(alias))
    previous_env = os.environ.get("TMPDIR")
    actual_mkdtemp = tempfile.mkdtemp

    def private_test_directory(*args: object, **kwargs: object) -> str:
        if kwargs.get("prefix") == "rqcn-" and kwargs.get("dir") == "/private/tmp":
            kwargs["dir"] = str(tmp_path)
        return actual_mkdtemp(*args, **kwargs)

    monkeypatch.setattr(tempfile, "mkdtemp", private_test_directory)
    plans = []

    def original_source_plan() -> None:
        # Read the actual complete 84-day receipt. No scheduler/job state is changed.
        plans.extend(foundation.adapter_registry.plan(spec))

    class SourcePlanComplete(Exception):
        pass

    def stop_before_worker(**kwargs: object) -> None:
        assert plans and all(plan.adapter_version == "2" for plan in plans)
        raise SourcePlanComplete("original full source plan verified; physical worker not run")

    monkeypatch.setattr(foundation.scheduler, "run_once", original_source_plan)
    monkeypatch.setattr(original_worker, "LabWorker", stop_before_worker)
    try:
        with pytest.raises(SourcePlanComplete):
            native_fixture.seal_native_original_jobs(foundation, (job_id,))
        assert tempfile.tempdir == str(alias) and os.environ.get("TMPDIR") == previous_env
        assert len(plans) == 1
        assert all(plan.work_plan.work_units == preparations[0].prepared.frozen.formal_work.work_units for plan in plans)
        after = tuple((job.job_id, job.status, job.version) for job in (
            foundation.reader.get_job(UUID(value)) for value in facts["accepted"]["job_ids"]))
        assert after == before
        cleanup = tuple(tmp_path.glob("rqcn-*"))
        assert cleanup == ()
    finally:
        foundation.close()


def test_native_independent_replay_uses_original_full_source_and_new_normal_ids(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import json
    import os
    import sqlite3
    from tests.support import strategy_promotion_native_fixture as sources

    original_root = os.environ.get("RQUANT_NATIVE_RESTORE_ROOT")
    if original_root is None:
        pytest.skip("retained original N=3 preparations are required for this reuse gate")
    original_root = Path(original_root)
    facts = json.loads((original_root / "complete-parent-preparation-facts.json").read_text())
    previous = RegisterExperimentFamily.model_validate(facts["command"])
    old = sources.restore_native_complete_promotion_fixture(original_root, monkeypatch)
    old_record = old.platform.get_request(previous.actor_id, UUID(previous.command_id))
    original_jobs = tuple(old.commands.reader.get_job(UUID(value)) for value in facts["accepted"]["job_ids"])
    assert all(job.status.value == "failed" and not job.recoverable for job in original_jobs)
    material = Path(os.environ.get("RQUANT_NATIVE_MATERIAL_ROOT", str(tmp_path / "independent-replay")))
    if material.exists():
        # The complete preparation is durable even if a later assertion interrupts the test.
        with sqlite3.connect((material / "trust/experiments.sqlite").as_uri() + "?mode=ro", uri=True) as connection:
            rows = connection.execute("SELECT payload_json FROM experiment_family_request LIMIT 2").fetchall()
        assert len(rows) == 1
        record = platform.ExperimentFamilyRecord.model_validate_json(rows[0][0])
        command = RegisterExperimentFamily(command_id=str(record.request_id), requested_at=record.registered_at,
            actor_id=record.owner, request=record.request)
        assert record.body_hash == canonical_sha256(command)
        (material / "complete-parent-preparation-facts.json").write_text(json.dumps({
            "family": record.model_dump(mode="json"), "command": command.model_dump(mode="json"),
            "sealed_results": False, "synthetic": True,
        }, indent=2, ensure_ascii=False))
        fixture = sources.restore_native_complete_promotion_fixture(material, monkeypatch)
        fixture.phase_source_builder = lambda read, base: (_ for _ in ()).throw(AssertionError("restoration republished source"))
    else:
        fixture = sources.build_native_independent_replay_fixture(material, original_root=original_root, monkeypatch=monkeypatch)
        command = RegisterExperimentFamily(command_id=str(uuid4()), requested_at=fixture.now,
            actor_id=previous.actor_id, request=previous.request)
        record = fixture.platform.begin_request(owner=command.actor_id, request_id=UUID(command.command_id),
            body_hash=canonical_sha256(command), request=command.request, registered_at=fixture.now)
        fixture.now += timedelta(seconds=1)
    ready = fixture.preparer()(record)
    preparations = tuple(fixture.platform.preparation(record.owner, record.family_id, index)
        for index in range(len(ready.actual_configurations)))
    installed = sources.install_native_owned_research(fixture, preparations=preparations)
    effect = installed.writer.freeze(command)
    accepted = installed.writer.submit(command, effect)
    assert installed.writer.submit(command, effect) == accepted
    assert command.command_id != previous.command_id and ready.family_id != old_record.family_id
    assert set(accepted["job_ids"]).isdisjoint(facts["accepted"]["job_ids"])
    assert len(fixture.commands.spool.pending()) == len(preparations) == 3
    identities = []
    for index, fresh in enumerate(preparations):
        prior = old.platform.preparation(old_record.owner, old_record.family_id, index)
        before = prior.prepared.published.receipt.seed
        after = fresh.prepared.published.receipt.seed
        assert fresh.configuration == prior.configuration
        normalized = after.model_dump(mode="python")
        normalized["runtime"]["source_key"] = before.runtime.source_key
        assert type(before).model_validate(normalized) == before
        assert after.origin_materials == before.origin_materials and after.derivations == before.derivations
        assert len(fresh.prepared.frozen.runtime.daily_trade_dates) == 84
        identities.append({"native_id": after.native_registration.logical_id,
            "original_job_id": facts["accepted"]["job_ids"][index], "replay_job_id": accepted["job_ids"][index],
            "original_seed_hash": before.seed_hash, "replay_seed_hash": after.seed_hash,
            "full_raw_material_unchanged": True, "profile_hash": after.runtime.execution_profile.profile_hash})
    assert tuple(old.commands.reader.get_job(job.job_id) for job in original_jobs) == original_jobs
    family = fixture.platform.registry.get_hypothesis_family(ready.family_id)
    (material / "complete-parent-preparation-facts.json").write_text(json.dumps({
        "family": ready.model_dump(mode="json"), "command": command.model_dump(mode="json"),
        "accepted": accepted, "parent_manifest": family.model_dump(mode="json"),
        "source_reads": len(fixture.source_reads), "actual_python": __import__("sys").version,
        "sealed_results": False, "synthetic": True,
    }, indent=2, ensure_ascii=False))
    (material / "independent-replay-link.json").write_text(json.dumps({
        "kind": "independent normal installation and attempt; not original UUID recovery",
        "original_root": str(original_root), "original_command": previous.command_id,
        "replay_command": command.command_id, "original_family": old_record.family_id,
        "replay_family": ready.family_id, "links": identities,
        "original_failed_jobs_unchanged": True, "sealed_results": False,
    }, indent=2, ensure_ascii=False))
