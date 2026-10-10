from __future__ import annotations

from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

import pytest

from rquant.lab_jobs import LabJobReader, LabJobStore, SchedulerLeaseFencedError
from rquant.lab_shard_protocol import LabClaimSpool
from tests.unit.test_lab_jobs import _spec, _submit

if TYPE_CHECKING:
    from rquant.lab_scheduling_control import LabSchedulingBarrierPort, LabSchedulingCommandEnvelope

NOW = datetime(2026, 10, 6, 0, 0, tzinfo=UTC)


def store_and_port(tmp_path: Path) -> tuple[LabJobStore, LabSchedulingBarrierPort]:
    from rquant.lab_scheduling_control import LabSchedulingBarrierPort

    store = LabJobStore(tmp_path / "jobs.sqlite3")
    store.initialize()
    spool = LabClaimSpool(tmp_path / "claims")
    return store, LabSchedulingBarrierPort(spool.root, store=store)


def command(store: LabJobStore, *, paused: bool, expected_version: int, request_id: UUID | None = None) -> LabSchedulingCommandEnvelope:
    from rquant.lab_scheduling_control import LabSchedulingCommandEnvelope, PauseSchedulingCommand, ResumeSchedulingCommand

    action = PauseSchedulingCommand if paused else ResumeSchedulingCommand
    return LabSchedulingCommandEnvelope(request_id=request_id or uuid4(), command=action(expected_version=expected_version, queue_identity=store.scheduling_identity(), accepted_at=NOW))


def test_tsc_08_legacy_default_no_silent_migration(tmp_path: Path) -> None:
    store, _port = store_and_port(tmp_path)
    assert store.scheduling_state() is None
    with closing(store._connect()) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 16
        assert connection.execute("SELECT 1 FROM sqlite_schema WHERE name='lab_scheduler_control'").fetchone() is None


def test_tsc_08_explicit_quiescent_migration_preserves_original_receipt(tmp_path: Path) -> None:
    store, port = store_and_port(tmp_path)
    lease = store.acquire_scheduler_lease(owner_id="scheduler", lease_seconds=120, now=NOW)
    legacy_spec = _spec()
    submitted = _submit(spec=type(legacy_spec).model_validate(legacy_spec.model_dump() | {"deadline": NOW + timedelta(days=1)}))
    receipt = store.apply_command(submitted, lease=lease, now=NOW)
    before = LabJobReader(store.path).get_job(submitted.command.job_id).model_dump_json()
    migrated = store.enable_scheduling_control(lease=lease, barrier_port=port, now=NOW)
    assert migrated.desired_version == migrated.applied_version == 0
    assert migrated.desired_paused is migrated.applied_paused is False
    with closing(store._connect()) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 17
    reopened = LabJobStore(store.path)
    reopened.initialize()
    assert reopened.scheduling_state() == migrated
    assert LabJobReader(store.path).get_job(submitted.command.job_id).model_dump_json() == before
    assert reopened.apply_command(submitted, lease=lease, now=NOW + timedelta(seconds=1)) == receipt


def test_tsc_08_global_cas_old_uuid_cannot_rollback_resume(tmp_path: Path) -> None:
    store, port = store_and_port(tmp_path)
    lease = store.acquire_scheduler_lease(owner_id="scheduler", lease_seconds=120, now=NOW)
    store.enable_scheduling_control(lease=lease, barrier_port=port, now=NOW)
    pause = command(store, paused=True, expected_version=0)
    paused = port.apply_command(pause, lease=lease, now=NOW + timedelta(seconds=1))
    assert paused.status == "applied" and paused.desired_version == 1
    assert store.scheduling_state().desired_paused is True
    resume = command(store, paused=False, expected_version=1)
    resumed = port.apply_command(resume, lease=lease, now=NOW + timedelta(seconds=2))
    assert resumed.status == "applied" and resumed.desired_version == 2
    assert port.apply_command(pause, lease=lease, now=NOW + timedelta(seconds=3)) == paused
    assert store.scheduling_state().desired_version == 2
    assert store.scheduling_state().desired_paused is False
    assert port.read_barrier().state == "open"
    stale = command(store, paused=True, expected_version=1)
    assert port.apply_command(stale, lease=lease, now=NOW + timedelta(seconds=4)).status == "rejected"
    assert store.scheduling_state().desired_version == 2


def test_tsc_08_fenced_scheduler_and_unknown_or_corrupt_marker_closed(tmp_path: Path) -> None:
    store, port = store_and_port(tmp_path)
    old = store.acquire_scheduler_lease(owner_id="scheduler-a", lease_seconds=2, now=NOW)
    store.enable_scheduling_control(lease=old, barrier_port=port, now=NOW)
    newer = store.acquire_scheduler_lease(owner_id="scheduler-b", lease_seconds=120, now=NOW + timedelta(seconds=3))
    with pytest.raises(SchedulerLeaseFencedError):
        port.apply_command(command(store, paused=True, expected_version=0), lease=old, now=NOW + timedelta(seconds=4))
    assert store.scheduling_state().desired_version == 0
    port.apply_command(command(store, paused=True, expected_version=0), lease=newer, now=NOW + timedelta(seconds=4))
    marker = port.marker_path
    marker.write_bytes(b'{"state":"open"}')
    with pytest.raises(ValueError):
        port.read_barrier()
    before = marker.read_bytes()
    with pytest.raises(ValueError):
        port.reconcile(lease=newer, now=NOW + timedelta(seconds=5))
    assert marker.read_bytes() == before
    assert store.scheduling_state().desired_paused is True


def test_tsc_02_scheduler_envelope_scope_and_request_content(tmp_path: Path) -> None:
    from rquant.lab_scheduling_control import LabSchedulingCommandEnvelope

    store, port = store_and_port(tmp_path)
    lease = store.acquire_scheduler_lease(owner_id="scheduler", lease_seconds=120, now=NOW)
    store.enable_scheduling_control(lease=lease, barrier_port=port, now=NOW)
    request = command(store, paused=True, expected_version=0)
    assert request.schema_version == 2
    assert request.command.target_scope == "scheduler"
    for changes in ({"job_id": str(uuid4())}, {"schema_version": 1}, {"content_hash": "a" * 64}):
        with pytest.raises(ValueError):
            LabSchedulingCommandEnvelope.model_validate(request.model_dump() | changes)
    first = port.apply_command(request, lease=lease, now=NOW + timedelta(seconds=1))
    altered = command(store, paused=False, expected_version=0, request_id=request.request_id)
    with pytest.raises(ValueError, match="different|conflict"):
        port.apply_command(altered, lease=lease, now=NOW + timedelta(seconds=2))
    assert store.scheduling_receipt(request.request_id) == first


def test_tsc_08_pause_is_applied_only_after_original_drain_closes(tmp_path: Path) -> None:
    store, port = store_and_port(tmp_path)
    lease = store.acquire_scheduler_lease(owner_id="scheduler", lease_seconds=120, now=NOW)
    store.enable_scheduling_control(lease=lease, barrier_port=port, now=NOW)
    port.apply_command(command(store, paused=True, expected_version=0), lease=lease, now=NOW + timedelta(seconds=1))
    state = port.reconcile(lease=lease, now=NOW + timedelta(seconds=2))
    assert state.applied_paused is True
    assert state.applied_version == state.desired_version == 1
    assert state.draining_count == 0
    assert state.applied_at == NOW + timedelta(seconds=2)


def test_tsc_08_scheduler_scope_roundtrips_original_spool_and_ack(tmp_path: Path) -> None:
    from rquant.lab_job_protocol import LabCommandSpool
    from rquant.strict_json import canonical_model_json_bytes

    store, port = store_and_port(tmp_path)
    lease = store.acquire_scheduler_lease(owner_id="scheduler", lease_seconds=120, now=NOW)
    store.enable_scheduling_control(lease=lease, barrier_port=port, now=NOW)
    spool = LabCommandSpool(tmp_path / "commands")
    base = _spec()
    legacy = _submit(spec=type(base).model_validate(base.model_dump() | {"deadline": NOW + timedelta(days=1)}))
    old_entry = spool.publish(legacy)
    before = old_entry.path.read_bytes()
    pause = command(store, paused=True, expected_version=0)
    entry = spool.publish(pause)
    assert entry.envelope == pause
    assert spool.find(pause.request_id) == entry
    assert set(item.envelope.schema_version for item in spool.pending()) == {1, 2}
    result = port.apply_command(pause, lease=lease, now=NOW + timedelta(seconds=1))
    acknowledged = spool.ack(entry, result)
    assert acknowledged.receipt == result
    assert spool.find(pause.request_id) == acknowledged
    assert spool.publish(pause) == acknowledged
    assert old_entry.path.read_bytes() == before == canonical_model_json_bytes(legacy)
    with pytest.raises(Exception, match="conflict|content|different"):
        spool.publish(command(store, paused=False, expected_version=0, request_id=pause.request_id))


def test_tsc_08_original_scheduler_consumes_global_commands_with_legacy_job(tmp_path: Path) -> None:
    from rquant.lab_job_protocol import LabCommandSpool
    from rquant.lab_scheduler import LabScheduler

    store, port = store_and_port(tmp_path)
    spool = LabCommandSpool(tmp_path / "commands")
    scheduler = LabScheduler(store=store, spool=spool, owner_id="scheduler", lease_seconds=120, heartbeat_seconds=10, poll_interval_ms=10, scheduling_control=port, clock=lambda: NOW)
    scheduler.run_once()
    assert store.scheduling_identity().schema_version == 17
    pause = command(store, paused=True, expected_version=0)
    spool.publish(pause)
    tick = scheduler.run_once()
    assert tick.applied == 1
    assert spool.find(pause.request_id).receipt == store.scheduling_receipt(pause.request_id)
    state = store.scheduling_state()
    assert state.desired_paused and state.applied_paused
    base = _spec()
    legacy = _submit(spec=type(base).model_validate(base.model_dump() | {"deadline": NOW + timedelta(days=1)}))
    spool.publish(legacy)
    scheduler.run_once()
    assert LabJobReader(store.path).get_job(legacy.command.job_id) is not None
    assert store.scheduling_state().desired_version == 1
    scheduler.release()


def test_tsc_08_facade_original_request_lookup_precedes_new_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant.lab_job_center import LabCommandSubmissionFacade
    from rquant.lab_job_protocol import LabCommandSpool

    store, port = store_and_port(tmp_path)
    lease = store.acquire_scheduler_lease(owner_id="scheduler", lease_seconds=120, now=NOW)
    store.enable_scheduling_control(lease=lease, barrier_port=port, now=NOW)
    reader = LabJobReader(store.path)
    spool = LabCommandSpool(tmp_path / "commands")
    facade = LabCommandSubmissionFacade(reader=reader, spool=spool, clock=lambda: NOW)
    request = command(store, paused=True, expected_version=0)
    first = facade.submit_scheduling_control(request)
    assert first.target_scope == "scheduler" and first.status == "pending"

    def replaced_state() -> None:
        pytest.fail("original UUID retry must lookup before current scheduling state")

    monkeypatch.setattr(reader, "scheduling_state", replaced_state)
    assert facade.submit_scheduling_control(request) == first


def test_tsc_08_original_reader_highwater_records_actual_schema17(tmp_path: Path) -> None:
    store, port = store_and_port(tmp_path)
    lease = store.acquire_scheduler_lease(owner_id="scheduler", lease_seconds=120, now=NOW)
    store.enable_scheduling_control(lease=lease, barrier_port=port, now=NOW)
    generations: list[int] = []

    class Observer:
        def observe(self, *, database_generation: tuple[int, int], schema_generation: int, mutation_epoch: int, chain_generation: int, chain_head_hash: str, receipt_kind: str, receipt_hash: str) -> None:
            generations.append(schema_generation)

    reader = LabJobReader(store.path, highwater_observer=Observer())
    reader.audit_incremental()
    assert generations == [17]


def test_tsc_08_restarted_scheduler_reopens_exact_capability_before_dispatch(tmp_path: Path) -> None:
    from rquant.lab_job_protocol import LabCommandSpool
    from rquant.lab_scheduler import LabScheduler
    from rquant.lab_scheduling_control import LabSchedulingBarrierPort
    from tests.unit.test_lab_jobs import _v1_definitions

    store, port = store_and_port(tmp_path)
    lease = store.acquire_scheduler_lease(owner_id="scheduler-a", lease_seconds=120, now=NOW)
    store.enable_scheduling_control(lease=lease, barrier_port=port, now=NOW)
    base = _spec()
    request = _submit(spec=type(base).model_validate(base.model_dump() | {"deadline": NOW + timedelta(days=1)}))
    store.apply_command(request, lease=lease, now=NOW)
    store.plan_job(request.command.job_id, _v1_definitions(1), lease=lease, now=NOW)
    store.release_scheduler_lease(lease, now=NOW)
    reopened = LabJobStore(store.path)
    reopened.initialize()
    next_port = LabSchedulingBarrierPort(port.root, store=reopened)
    scheduler = LabScheduler(store=reopened, spool=LabCommandSpool(tmp_path / "commands"), owner_id="scheduler-b", lease_seconds=120, heartbeat_seconds=10, poll_interval_ms=10, scheduling_control=next_port, clock=lambda: NOW + timedelta(seconds=1))
    try:
        scheduler.run_once()
        claim = reopened.claim_next_shard(worker_id="worker", shard_lease_seconds=30, lease=scheduler.lease, now=NOW + timedelta(seconds=1))
        assert claim is not None
        assert claim.job_id == request.command.job_id
    finally:
        scheduler.release()


def test_tsc_02_original_job_uuid_conflicts_with_scheduler_scope(tmp_path: Path) -> None:
    from rquant.lab_job_center import LabCommandSubmissionFacade, CommandSubmissionConflict
    from rquant.lab_job_protocol import LabCommandSpool

    store, port = store_and_port(tmp_path)
    lease = store.acquire_scheduler_lease(owner_id="scheduler", lease_seconds=120, now=NOW)
    store.enable_scheduling_control(lease=lease, barrier_port=port, now=NOW)
    spool = LabCommandSpool(tmp_path / "commands")
    pause = command(store, paused=True, expected_version=0)
    spool.publish(pause)
    facade = LabCommandSubmissionFacade(reader=LabJobReader(store.path), spool=spool, clock=lambda: NOW)
    base = _spec()
    original = _submit(spec=type(base).model_validate(base.model_dump() | {"deadline": NOW + timedelta(days=1)}))
    original = type(original).model_validate(original.model_dump() | {"request_id": pause.request_id, "content_hash": ""})
    result = facade._existing(original)
    assert isinstance(result, CommandSubmissionConflict)
    assert result.reason == "interaction_content_conflict"
    assert spool.find(pause.request_id).envelope == pause


def test_tsc_08_expired_schema16_admission_is_not_quiescent_without_actual_cleanup(tmp_path: Path) -> None:
    from tests.unit.test_lab_jobs import _v1_definitions

    store, port = store_and_port(tmp_path)
    lease = store.acquire_scheduler_lease(owner_id="scheduler", lease_seconds=120, now=NOW)
    base = _spec()
    request = _submit(spec=type(base).model_validate(base.model_dump() | {"deadline": NOW + timedelta(days=1)}))
    store.apply_command(request, lease=lease, now=NOW)
    store.plan_job(request.command.job_id, _v1_definitions(1), lease=lease, now=NOW)
    claim = store.claim_next_shard(worker_id="worker", shard_lease_seconds=2, lease=lease, now=NOW)
    assert claim is not None
    spool = LabClaimSpool(port.root)
    spool.consume(spool.publish(claim))
    spool.admit_execution(claim)
    store.recover_expired_jobs(lease, now=NOW + timedelta(seconds=3))
    with pytest.raises(ValueError, match="quiescent|admission|cleanup"):
        store.enable_scheduling_control(lease=lease, barrier_port=port, now=NOW + timedelta(seconds=4))
    with closing(store._connect()) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 16
    assert not port.marker_path.exists()


def test_tsc_08_physical_report_and_finalizer_intents_block_before_schema_change(tmp_path: Path) -> None:
    from rquant.lab_scheduling_control import LabSchedulingBarrierPort, LabSchedulingMaintenanceScope
    from rquant.lab_shard_protocol import LabReportSpool
    from rquant.lab_artifact_protocol import LabArtifactCommitSpool
    from rquant.lab_artifacts import LabJobArtifactStore

    store, initial = store_and_port(tmp_path)
    reports = LabReportSpool(tmp_path / "reports")
    commits = LabArtifactCommitSpool(tmp_path / "commits")
    artifacts = LabJobArtifactStore(tmp_path / "final")
    try:
        scope = LabSchedulingMaintenanceScope(report_root=reports.root, artifact_commit_root=commits.root, final_artifact_root=artifacts.root)
        port = LabSchedulingBarrierPort(initial.root, store=store, maintenance_scope=scope)
        lease = store.acquire_scheduler_lease(owner_id="scheduler", lease_seconds=120, now=NOW)
        for parent in (reports.pending_dir, commits.pending_dir, artifacts.seal_intents_root, artifacts.candidates_root):
            entry = parent / "unsettled.json"
            entry.write_bytes(b"{}")
            with pytest.raises(ValueError, match="quiescent|pending|finalizer"):
                store.enable_scheduling_control(lease=lease, barrier_port=port, now=NOW)
            with closing(store._connect()) as connection:
                assert connection.execute("PRAGMA user_version").fetchone()[0] == 16
            assert entry.read_bytes() == b"{}" and not port.marker_path.exists()
            entry.unlink()
        migrated = store.enable_scheduling_control(lease=lease, barrier_port=port, now=NOW)
        assert migrated.queue_identity.schema_version == 17
    finally:
        artifacts.close()


def test_tsc_09_same_store_without_control_profile_does_not_inherit_dispatch_capability(tmp_path: Path) -> None:
    from rquant.lab_scheduler import LabScheduler
    from rquant.lab_job_protocol import LabCommandSpool
    from tests.unit.test_lab_jobs import _v1_definitions

    store, port = store_and_port(tmp_path)
    lease = store.acquire_scheduler_lease(owner_id="scheduler", lease_seconds=120, now=NOW)
    store.enable_scheduling_control(lease=lease, barrier_port=port, now=NOW)
    base = _spec()
    request = _submit(spec=type(base).model_validate(base.model_dump() | {"deadline": NOW + timedelta(days=1)}))
    store.apply_command(request, lease=lease, now=NOW)
    store.plan_job(request.command.job_id, _v1_definitions(1), lease=lease, now=NOW)
    scheduler = LabScheduler(store=store, spool=LabCommandSpool(tmp_path / "commands"), owner_id="scheduler", lease_seconds=120, heartbeat_seconds=10, poll_interval_ms=10)
    assert scheduler.scheduling_control is None
    assert store.claim_next_shard(worker_id="worker", shard_lease_seconds=30, lease=lease, now=NOW) is None


def test_tsc_08_global_4096_original_receipts_keep_lookup_first(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store, port = store_and_port(tmp_path)
    lease = store.acquire_scheduler_lease(owner_id="scheduler", lease_seconds=120, now=NOW)
    store.enable_scheduling_control(lease=lease, barrier_port=port, now=NOW)
    original = command(store, paused=True, expected_version=0)
    first = port.apply_command(original, lease=lease, now=NOW + timedelta(seconds=1))
    assert first.completed_at == NOW + timedelta(seconds=1)
    accepted = [(original, first)]
    for index in range(4095):
        auxiliary = command(store, paused=True, expected_version=0, request_id=UUID(int=index + 1))
        receipt = port.apply_command(auxiliary, lease=lease, now=NOW + timedelta(seconds=2))
        assert receipt.request_id == auxiliary.request_id and receipt.content_hash == auxiliary.content_hash
        assert receipt.status == "rejected" and receipt.desired_version == 1
        accepted.append((auxiliary, receipt))
    with closing(store._connect()) as connection:
        rows = connection.execute("SELECT request_id,content_hash,payload_json FROM lab_scheduler_control_receipt").fetchall()
    assert len(rows) == len(accepted) == 4096
    persisted = {row[0]: (row[1], row[2]) for row in rows}
    for auxiliary, receipt in accepted:
        assert persisted[str(auxiliary.request_id)] == (auxiliary.content_hash, receipt.model_dump_json())
        assert store.scheduling_receipt(auxiliary.request_id) == receipt
    head = store.scheduling_state()
    monkeypatch.setattr(port, "_read_locked", lambda: pytest.fail("old UUID and full capacity must be checked before a newer barrier"))
    assert port.apply_command(original, lease=lease, now=NOW + timedelta(seconds=3)) == first
    last, last_receipt = accepted[-1]
    assert port.apply_command(last, lease=lease, now=NOW + timedelta(seconds=3)) == last_receipt
    fresh = command(store, paused=False, expected_version=1)
    with pytest.raises(ValueError, match="4096"):
        port.apply_command(fresh, lease=lease, now=NOW + timedelta(seconds=3))
    assert store.scheduling_receipt(fresh.request_id) is None
    assert store.scheduling_state() == head and head.desired_version == 1
    print("TSC-08-06: 4096 original typed global receipts; old first/last lookup exact; 4097 refused before marker/CAS")


@pytest.mark.parametrize("family", ["C5", "PP", "M8"])
def test_tsc_10_schema16_complete_frozen_bindings_remain_exact_after17(tmp_path: Path, family: str, request: pytest.FixtureRequest) -> None:
    from hashlib import sha256

    from rquant.lab_job_center import LabCommandSubmissionFacade
    from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool
    from rquant.lab_artifact_protocol import LabArtifactCommitSpool
    from rquant.lab_artifacts import LabJobArtifactStore
    from rquant.lab_scheduling_control import LabSchedulingBarrierPort, LabSchedulingMaintenanceScope
    from rquant.lab_shard_protocol import LabReportSpool
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry

    cleanup = request.addfinalizer
    root = tmp_path / family
    root.mkdir(mode=0o700)
    if family == "C5":
        from rquant.strategy_authoring_commands import ArchiveStrategyTemplate
        from tests.unit.test_strategy_authoring import catalog, draft
        from tests.unit.test_strategy_authoring_page_control import submit
        from tests.unit.test_strategy_template_submission import run_service

        target, service, backend, request, jobs = run_service(root)
        original_page_receipt = submit(service, target, request)
        accepted = target.accepted_run(request, owner_id="alice", expected_identity=target.identity())
        assert accepted is not None and accepted.owner_id == "alice"
        facade = backend.facade
        envelope = facade.spool.pending()[0].envelope
        spec = accepted.spec
        now = backend.preparer.clock()
        directory = facade.template_directory
        assert directory is not None
        plan = directory.registry_for_spec(spec).plan(spec)
        owner_facts = accepted.model_dump_json()
        next_version = target.save(draft(strategy_id=request.strategy_id, expected_head=request.head), owner_id="alice", catalog=catalog())
        assert next_version.head.version == 2
        target.archive(ArchiveStrategyTemplate(command_id=str(uuid4()), requested_at=request.requested_at, generation_id=request.generation_id,
                                             strategy_id=request.strategy_id, expected_head=next_version.head), owner_id="alice")
        assert target.accepted_run(request, owner_id="alice", expected_identity=target.identity()).model_dump_json() == owner_facts
        assert directory.registry_for_spec(spec).plan(spec) == plan
    elif family == "PP":
        import gc

        from rquant.paper_research_runtime import PaperResearchRuntimeDirectory
        from tests.unit.test_paper_research_submission import fixture

        service, backend, runtime, request, jobs = fixture(root)
        source = backend.research_backend.preparer.sources[0]
        idle_writer = source.broker._connect()
        cleanup(idle_writer.close)
        gc.collect()
        assert source.broker.path.with_name(source.broker.path.name + "-wal").is_file()
        original_page_receipt = service._submit_trusted_paper_portfolio(request, authenticated_actor_id="alice", verified_metadata_identity=runtime.state.identity())
        accepted, _ = service.outbox.lookup_paper_portfolio_command(request, authenticated_actor_id="alice")
        assert accepted.owner_id == "alice"
        facade = backend.research_backend.facade
        envelope = facade.spool.pending()[0].envelope
        spec = accepted.spec
        now = backend.clock()
        directory = PaperResearchRuntimeDirectory(states=(runtime.state,), expected_identities=(runtime.state.identity(),))
        plan = directory.registry_for_spec(spec).plan(spec)
        owner_facts = accepted.model_dump_json()
        runtime.state.start_configuration(runtime.state.configuration.model_copy(update={"version": 2, "configured_at": now + timedelta(minutes=1)}))
        assert directory.catalog_for_spec(spec).configuration.version == 1
        assert directory.registry_for_spec(spec).plan(spec) == plan
    else:
        from tests.unit.test_experiment_platform import NOW as FAMILY_NOW, prepared_family

        platform, record, children, definitions = prepared_family(root)
        registered = platform.register_family_submission(owner=record.owner, request_id=record.request_id, children=children)
        platform.admit_publication(children[0].intent, now=FAMILY_NOW)
        prepared = platform.preparation("alice", record.family_id, 0)
        assert prepared is not None and prepared.owner == registered.owner == "alice"
        envelope = LabCommandEnvelope.model_validate_json(children[0].intent.envelope_json)
        spec = envelope.command.spec
        jobs = LabJobStore(root / "jobs.sqlite3")
        jobs.initialize()
        facade = LabCommandSubmissionFacade(reader=LabJobReader(jobs.path), spool=LabCommandSpool(root / "commands"),
                                            experiment_registry=platform.registry, definition_registry=definitions, clock=lambda: FAMILY_NOW)
        facade.recover_pending_experiment_submissions()
        assert facade.spool.find(envelope.request_id).envelope == envelope
        now = FAMILY_NOW
        plan = default_strategy_job_adapter_registry().plan(spec)
        owner_facts = prepared.model_dump_json()

    assert envelope.command.spec == spec and plan
    lease = jobs.acquire_scheduler_lease(owner_id="scheduler", lease_seconds=120, now=now)
    with closing(jobs._connect()) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 16
    receipt = jobs.apply_command(envelope, lease=lease, now=now,
                                 submission_authority=lambda value, at: facade.validate_prepared_experiment_submission(value, observed_at=at))
    jobs.plan_job(envelope.command.job_id, plan, lease=lease, now=now)
    reader = LabJobReader(jobs.path)
    before_job = reader.get_job(envelope.command.job_id).model_dump_json()
    before_shards = tuple(shard.model_dump_json() for shard in reader.list_shards(envelope.command.job_id))
    assert before_shards
    assert tuple((shard.shard_id, shard.shard_index, shard.adapter_id, shard.adapter_version, shard.plan_hash,
                  shard.payload_json, shard.payload_hash, shard.work_plan) for shard in reader.list_shards(envelope.command.job_id)) == tuple(
                      (definition.shard_id, definition.shard_index, definition.adapter_id, definition.adapter_version, definition.plan_hash,
                       definition.payload_json, definition.payload_hash, definition.work_plan) for definition in plan)
    original_spool_bytes = facade.spool.find(envelope.request_id).path.read_bytes()
    physical = jobs.path.lstat()
    input_files = set(root.glob("input-*.duckdb")) | set(root.glob("*.duckdb"))
    for directory_path in (root / "inputs", root / "lake", root / "definitions"):
        if directory_path.exists():
            input_files.update(path for path in directory_path.rglob("*") if path.is_file())
    assert input_files
    before_sources = {str(path.relative_to(root)): sha256(path.read_bytes()).hexdigest() for path in sorted(input_files)}
    claims = LabClaimSpool(root / "claims")
    reports = LabReportSpool(root / "reports")
    commits = LabArtifactCommitSpool(root / "commits")
    artifacts = LabJobArtifactStore(root / "final")
    cleanup(artifacts.close)
    maintenance = LabSchedulingMaintenanceScope(report_root=reports.root, artifact_commit_root=commits.root, final_artifact_root=artifacts.root)
    port = LabSchedulingBarrierPort(claims.root, store=jobs, maintenance_scope=maintenance)
    migrated = jobs.enable_scheduling_control(lease=lease, barrier_port=port, now=now + timedelta(seconds=1))
    assert migrated.queue_identity.schema_version == 17
    reopened = LabJobStore(jobs.path)
    reopened.initialize()
    after_physical = reopened.path.lstat()
    assert (after_physical.st_dev, after_physical.st_ino) == (physical.st_dev, physical.st_ino)
    reader = LabJobReader(reopened.path)
    assert reader.get_job(envelope.command.job_id).model_dump_json() == before_job
    assert tuple(shard.model_dump_json() for shard in reader.list_shards(envelope.command.job_id)) == before_shards
    assert reopened.apply_command(envelope, lease=lease, now=now + timedelta(seconds=2)) == receipt
    assert facade.spool.find(envelope.request_id).path.read_bytes() == original_spool_bytes
    if family == "C5":
        assert target.accepted_run(request, owner_id="alice", expected_identity=target.identity()).model_dump_json() == owner_facts
        assert service._resume_trusted_strategy_authoring(request, authenticated_actor_id="alice") == original_page_receipt
        assert directory.registry_for_spec(spec).plan(spec) == plan
    elif family == "PP":
        assert service.outbox.lookup_paper_portfolio_command(request, authenticated_actor_id="alice")[0].model_dump_json() == owner_facts
        assert service._resume_trusted_paper_portfolio(request, authenticated_actor_id="alice") == original_page_receipt
        assert directory.registry_for_spec(spec).plan(spec) == plan
    else:
        assert platform.preparation("alice", record.family_id, 0).model_dump_json() == owner_facts
        facade.validate_prepared_experiment_submission(envelope, observed_at=now + timedelta(seconds=2))
        assert default_strategy_job_adapter_registry().plan(spec) == plan
    assert {str(path.relative_to(root)): sha256(path.read_bytes()).hexdigest() for path in sorted(input_files)} == before_sources
    print(f"TSC-10-03/{family}: original schema16 complete accepted spec/plan/owner/code/source and spool bytes unchanged after explicit17 migration/reopen; no re-sign or rebind")


@pytest.mark.parametrize("checkpoint", ("pause_pending_written", "pause_sql_committed", "pause_closed_written", "resume_sql_committed"))
def test_tsc_09_original_pause_resume_checkpoint_recovery(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, checkpoint: str) -> None:
    from rquant.lab_scheduling_control import LabSchedulingBarrier, LabSchedulingBarrierPort
    from tests.unit.test_lab_jobs import _v1_definitions

    store, port = store_and_port(tmp_path)
    lease = store.acquire_scheduler_lease(owner_id="scheduler", lease_seconds=120, now=NOW)
    store.enable_scheduling_control(lease=lease, barrier_port=port, now=NOW)
    base = _spec()
    submitted = _submit(spec=type(base).model_validate(base.model_dump() | {"deadline": NOW + timedelta(days=1)}))
    store.apply_command(submitted, lease=lease, now=NOW)
    store.plan_job(submitted.command.job_id, _v1_definitions(1), lease=lease, now=NOW)
    original_job = LabJobReader(store.path).get_job(submitted.command.job_id).model_dump_json()
    original_shards = tuple(item.model_dump_json() for item in LabJobReader(store.path).list_shards(submitted.command.job_id))
    identity = store.scheduling_identity()
    pause = command(store, paused=True, expected_version=0)
    resume_case = checkpoint == "resume_sql_committed"
    if resume_case:
        first_pause = port.apply_command(pause, lease=lease, now=NOW + timedelta(seconds=1))
        assert first_pause.desired_version == 1
        assert port.reconcile(lease=lease, now=NOW + timedelta(seconds=2)).applied_paused
        request = command(store, paused=False, expected_version=1)
    else:
        request = pause
    original_request = request.model_dump_json()
    original_write = port._write_locked
    writes = 0

    def interrupt_original_write(value: LabSchedulingBarrier) -> None:
        nonlocal writes
        writes += 1
        if checkpoint == "pause_pending_written" and writes == 1:
            original_write(value)
            raise RuntimeError("checkpoint after original pending marker")
        if checkpoint in ("pause_sql_committed", "resume_sql_committed") and writes == 2:
            raise RuntimeError("checkpoint after original DB CAS before final marker")
        original_write(value)
        if checkpoint == "pause_closed_written" and writes == 2:
            raise RuntimeError("checkpoint after original closed marker")

    with monkeypatch.context() as interrupted:
        interrupted.setattr(port, "_write_locked", interrupt_original_write)
        with pytest.raises(RuntimeError, match="checkpoint after original"):
            port.apply_command(request, lease=lease, now=NOW + timedelta(seconds=3))
    assert writes == (1 if checkpoint == "pause_pending_written" else 2)
    marker = port.read_barrier()
    assert marker.queue_identity == identity
    assert marker.state == ("closed" if checkpoint == "pause_closed_written" else "transition_pending")
    if marker.state == "transition_pending":
        assert marker.pending_command == request
    receipt = store.scheduling_receipt(request.request_id)
    if checkpoint == "pause_pending_written":
        assert receipt is None
        assert store.scheduling_state().desired_version == 0
    else:
        assert receipt is not None and receipt.content_hash == request.content_hash
        assert receipt.desired_version == (2 if resume_case else 1)
        assert store.scheduling_state().desired_version == receipt.desired_version

    reopened = LabJobStore(store.path)
    reopened.initialize()
    next_port = LabSchedulingBarrierPort(port.root, store=reopened)
    assert reopened.scheduling_identity() == identity
    assert next_port.identity == port.identity
    assert next_port.read_barrier() == marker
    assert reopened.claim_next_shard(worker_id="premature", shard_lease_seconds=30, lease=lease, now=NOW + timedelta(seconds=4)) is None
    assert LabJobReader(reopened.path).get_job(submitted.command.job_id).model_dump_json() == original_job
    assert tuple(item.model_dump_json() for item in LabJobReader(reopened.path).list_shards(submitted.command.job_id)) == original_shards
    if receipt is None:
        restored = next_port.reconcile(lease=lease, now=NOW + timedelta(seconds=5))
        assert restored.desired_version == restored.applied_version == 0
        assert restored.desired_paused is restored.applied_paused is False
        assert reopened.scheduling_receipt(request.request_id) is None
        receipt = next_port.apply_command(request, lease=lease, now=NOW + timedelta(seconds=6))
        assert receipt.desired_version == 1
    else:
        assert next_port.apply_command(request, lease=lease, now=NOW + timedelta(seconds=5)) == receipt
        assert next_port.read_barrier() == marker
    assert reopened.scheduling_state().desired_version == receipt.desired_version
    assert request.model_dump_json() == original_request
    recovered = next_port.reconcile(lease=lease, now=NOW + timedelta(seconds=7))
    assert recovered.desired_version == recovered.applied_version == receipt.desired_version
    assert recovered.desired_paused is recovered.applied_paused is (not resume_case)
    assert recovered.draining_count == 0
    assert reopened.scheduling_receipt(request.request_id) == receipt
    if not resume_case:
        assert next_port.read_barrier().state == "closed"
        assert reopened.claim_next_shard(worker_id="paused", shard_lease_seconds=30, lease=lease, now=NOW + timedelta(seconds=8)) is None
        resume = command(reopened, paused=False, expected_version=1)
        resumed = next_port.apply_command(resume, lease=lease, now=NOW + timedelta(seconds=9))
        assert resumed.desired_version == 2
        assert reopened.claim_next_shard(worker_id="before-applied", shard_lease_seconds=30, lease=lease, now=NOW + timedelta(seconds=10)) is None
        recovered = next_port.reconcile(lease=lease, now=NOW + timedelta(seconds=11))
    assert recovered.desired_version == recovered.applied_version == 2
    assert recovered.desired_paused is recovered.applied_paused is False
    assert next_port.read_barrier().state == "open"
    assert next_port.apply_command(request, lease=lease, now=NOW + timedelta(seconds=12)) == receipt
    assert reopened.scheduling_state().desired_version == 2
    claim = reopened.claim_next_shard(worker_id="after-recovery", shard_lease_seconds=30, lease=lease, now=NOW + timedelta(seconds=13))
    assert claim is not None and claim.job_id == submitted.command.job_id
    assert reopened.scheduling_receipt(request.request_id) == receipt
    print(f"TSC-09-05/{checkpoint}: original pending/DB/marker checkpoint reopened; exact receipt and CAS restored, no premature dispatch, original request retained")
