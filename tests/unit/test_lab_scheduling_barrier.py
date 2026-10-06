from __future__ import annotations

from datetime import timedelta
from contextlib import contextmanager
from pathlib import Path
from threading import Event, Thread
from types import SimpleNamespace
from typing import Iterator

import pytest

from rquant.lab_jobs import LabJobStore, LabLeaseRecord
from rquant.lab_scheduling_control import LabSchedulingBarrierPort
from rquant.lab_shard_protocol import LabClaimSpool, LabShardClaim
from tests.unit.test_lab_jobs import _spec, _submit, _v1_definitions
from tests.unit.test_lab_scheduling_control import NOW, command, store_and_port


def claimed(tmp_path: Path) -> tuple[LabJobStore, LabSchedulingBarrierPort, LabLeaseRecord, LabClaimSpool, LabShardClaim]:
    store, port = store_and_port(tmp_path)
    lease = store.acquire_scheduler_lease(owner_id="scheduler", lease_seconds=120, now=NOW)
    store.enable_scheduling_control(lease=lease, barrier_port=port, now=NOW)
    base = _spec()
    request = _submit(spec=type(base).model_validate(base.model_dump() | {"deadline": NOW + timedelta(days=1)}))
    store.apply_command(request, lease=lease, now=NOW)
    store.plan_job(request.command.job_id, _v1_definitions(1), lease=lease, now=NOW)
    claim = store.claim_next_shard(worker_id="worker-a", shard_lease_seconds=30, lease=lease, now=NOW)
    assert isinstance(claim, LabShardClaim)
    spool = LabClaimSpool(port.root, expected_scheduling_barrier_identity=port.identity)
    spool.consume(spool.publish(claim))
    return store, port, lease, spool, claim


def test_tsc_09_unadmitted_claim_has_no_start_permit_after_pause(tmp_path: Path) -> None:
    store, port, lease, spool, claim = claimed(tmp_path)
    port.apply_command(command(store, paused=True, expected_version=0), lease=lease, now=NOW + timedelta(seconds=1))
    with pytest.raises(ValueError, match="scheduling|paused|barrier"):
        spool.admit_execution(claim)
    assert not spool.is_admitted(claim)
    assert port.reconcile(lease=lease, now=NOW + timedelta(seconds=2)).applied_paused


@pytest.mark.parametrize("phase", ["closed", "transition_pending"])
def test_m13_final_01_pause_before_first_ack_refuses_admitted_claim(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, phase: str) -> None:
    from rquant.lab_scheduling_control import LabSchedulingBarrier, read_scheduling_execution_at

    store, port, lease, spool, claim = claimed(tmp_path)
    spool.admit_execution(claim)
    with port.locked():
        assert read_scheduling_execution_at(port._active_fd, claim.claim_token) is None
    pause = command(store, paused=True, expected_version=0)
    if phase == "transition_pending":
        original_write = port._write_locked
        interrupted = False

        def stop_after_pending(value: LabSchedulingBarrier) -> None:
            nonlocal interrupted
            original_write(value)
            if value.state == "transition_pending" and not interrupted:
                interrupted = True
                raise InterruptedError("original pending marker fsynced before DB acceptance")

        monkeypatch.setattr(port, "_write_locked", stop_after_pending)
        with pytest.raises(InterruptedError, match="fsynced"):
            port.apply_command(pause, lease=lease, now=NOW + timedelta(seconds=1))
        assert store.scheduling_receipt(pause.request_id) is None
    else:
        port.apply_command(pause, lease=lease, now=NOW + timedelta(seconds=1))
        state = port.reconcile(lease=lease, now=NOW + timedelta(seconds=2))
        assert state.desired_paused and not state.applied_paused and state.draining_count == 1
    assert port.read_barrier().state == phase
    reached: list[str] = []
    try:
        for current in (spool, LabClaimSpool(port.root, expected_scheduling_barrier_identity=port.identity)):
            assert current.is_admitted(claim)
            with pytest.raises(ValueError, match="scheduling|paused|ACK|transition"):
                with current.scheduling_execution_start(claim, now=NOW + timedelta(seconds=3)):
                    reached.append("ACK")
            with port.locked():
                assert read_scheduling_execution_at(port._active_fd, claim.claim_token) is None
        assert reached == []
    finally:
        spool.close_scheduling_execution(claim, now=NOW + timedelta(seconds=4))
        recovered = port.reconcile(lease=lease, now=NOW + timedelta(seconds=5))
    assert recovered.draining_count == 0
    assert recovered.desired_version == recovered.applied_version == (1 if phase == "closed" else 0)
    assert recovered.applied_paused is (phase == "closed")


def test_tsc_09_admitted_drain_can_ack_but_unknown_child_survives_lease_expiry(tmp_path: Path) -> None:
    store, port, lease, spool, claim = claimed(tmp_path)
    spool.admit_execution(claim)
    with spool.scheduling_execution_start(claim, now=NOW):
        pass
    port.apply_command(command(store, paused=True, expected_version=0), lease=lease, now=NOW + timedelta(seconds=1))
    state = port.reconcile(lease=lease, now=NOW + timedelta(seconds=2))
    assert not state.applied_paused and state.draining_count == 1
    store.recover_expired_jobs(lease, now=NOW + timedelta(seconds=31))
    state = port.reconcile(lease=lease, now=NOW + timedelta(seconds=32))
    assert not state.applied_paused and state.draining_count == 1
    spool.close_scheduling_execution(claim, now=NOW + timedelta(seconds=33))
    state = port.reconcile(lease=lease, now=NOW + timedelta(seconds=34))
    assert state.applied_paused and state.draining_count == 0


def test_tsc_09_ack_intent_is_original_exact_single_attempt(tmp_path: Path) -> None:
    _store, port, _lease, spool, claim = claimed(tmp_path)
    spool.admit_execution(claim)
    with spool.scheduling_execution_start(claim, now=NOW):
        pass
    restarted = LabClaimSpool(port.root, expected_scheduling_barrier_identity=port.identity)
    with pytest.raises(ValueError, match="intent|attempt|unknown"):
        with restarted.scheduling_execution_start(claim, now=NOW + timedelta(seconds=1)):
            pytest.fail("original ACK intent must never start a second child")
    restarted.close_scheduling_execution(claim, now=NOW + timedelta(seconds=2))
    with pytest.raises(ValueError, match="closed|attempt|intent"):
        with restarted.scheduling_execution_start(claim, now=NOW + timedelta(seconds=3)):
            pytest.fail("closed execution cannot ACK again")


def test_tsc_09_missing_capability_and_corrupt_barrier_do_not_use_legacy(tmp_path: Path) -> None:
    _store, port, _lease, spool, claim = claimed(tmp_path)
    legacy = LabClaimSpool(port.root)
    with pytest.raises(ValueError, match="capability|identity|barrier"):
        legacy.admit_execution(claim)
    port.marker_path.write_bytes(b"{}")
    before = port.marker_path.read_bytes()
    with pytest.raises(ValueError):
        spool.admit_execution(claim)
    assert not spool.is_admitted(claim)
    assert port.marker_path.read_bytes() == before


def test_tsc_09_ack_and_pause_use_the_same_original_kernel_lock(tmp_path: Path) -> None:
    store, port, lease, spool, claim = claimed(tmp_path)
    spool.admit_execution(claim)
    entered = Event()
    release = Event()
    paused = Event()
    errors: list[BaseException] = []

    def ack() -> None:
        try:
            with spool.scheduling_execution_start(claim, now=NOW):
                entered.set()
                assert release.wait(3)
        except BaseException as exc:
            errors.append(exc)

    def pause() -> None:
        try:
            port.apply_command(command(store, paused=True, expected_version=0), lease=lease, now=NOW + timedelta(seconds=1))
            paused.set()
        except BaseException as exc:
            errors.append(exc)

    ack_thread = Thread(target=ack, daemon=False)
    pause_thread = Thread(target=pause, daemon=False)
    ack_thread.start()
    try:
        assert entered.wait(3)
        pause_thread.start()
        assert not paused.wait(.1)
    finally:
        release.set()
        ack_thread.join(3)
        if pause_thread.ident is not None:
            pause_thread.join(3)
    assert not ack_thread.is_alive() and not pause_thread.is_alive()
    assert errors == []
    assert paused.is_set()
    assert port.read_barrier().drains[0].token == str(claim.claim_token)


def test_tsc_08_resume_waits_for_reconciliation_before_admission(tmp_path: Path) -> None:
    store, port, lease, spool, claim = claimed(tmp_path)
    port.apply_command(command(store, paused=True, expected_version=0), lease=lease, now=NOW + timedelta(seconds=1))
    port.reconcile(lease=lease, now=NOW + timedelta(seconds=2))
    port.apply_command(command(store, paused=False, expected_version=1), lease=lease, now=NOW + timedelta(seconds=3))
    with pytest.raises(ValueError, match="scheduling|barrier|applied"):
        spool.admit_execution(claim)
    state = port.reconcile(lease=lease, now=NOW + timedelta(seconds=4))
    assert not state.applied_paused and state.applied_version == 2
    assert spool.admit_execution(claim).admission.claim == claim


def test_tsc_08_explicit_migration_does_not_repair_replaced_barrier(tmp_path: Path) -> None:
    store, port = store_and_port(tmp_path)
    lease = store.acquire_scheduler_lease(owner_id="scheduler", lease_seconds=120, now=NOW)
    store.enable_scheduling_control(lease=lease, barrier_port=port, now=NOW)
    port.marker_path.write_bytes(b"{}")
    with pytest.raises(ValueError):
        store.enable_scheduling_control(lease=lease, barrier_port=port, now=NOW + timedelta(seconds=1))
    assert port.marker_path.read_bytes() == b"{}"


def test_tsc_09_original_worker_rechecks_pause_at_actual_ack_boundary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import rquant.lab_worker as worker_module
    from rquant.resource_admission import TradingSession
    from tests.unit.test_lab_worker import _claim, _nshape_compare_spec, _worker

    store, port = store_and_port(tmp_path)
    lease = store.acquire_scheduler_lease(owner_id="scheduler", lease_seconds=120, now=NOW)
    store.enable_scheduling_control(lease=lease, barrier_port=port, now=NOW)
    spool = LabClaimSpool(port.root, expected_scheduling_barrier_identity=port.identity)
    worker = _worker(tmp_path, claims=spool)
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    validated = worker._validate_closed_claim(claim)
    accepted_acks: list[object] = []
    closed: list[object] = []
    child = SimpleNamespace(connection=object())

    def before_ack() -> None:
        port.apply_command(command(store, paused=True, expected_version=0), lease=lease, now=NOW + timedelta(seconds=1))

    def send(_connection: object, value: object, **_kwargs: object) -> None:
        if isinstance(value, worker_module._IsolationStartAck) and value.accepted:
            accepted_acks.append(value)
        raise InterruptedError("bounded wire stand-in; no process or socket")

    monkeypatch.setattr(worker, "_start_wire_child", lambda **_kwargs: child)
    monkeypatch.setattr(worker, "_close_wire_child", lambda value, **_kwargs: closed.append(value))
    monkeypatch.setattr(worker, "_before_isolation_start_commit_for_test", before_ack)
    monkeypatch.setattr(worker_module, "_send_wire", send)
    try:
        control = worker._execute_shard_isolated(claim, validated, runtime_code_sha="1" * 40, hard_limit_seconds=.05, initial_session=TradingSession.CLOSED)
        assert control.outcome is None
        assert accepted_acks == []
        assert closed == [child]
    finally:
        worker.close()


def _scheduling_source_type(*, hold: bool = False) -> type:
    from rquant.lab_scheduler import LabScheduler

    class SchedulingSourceScheduler(LabScheduler):
        hold_emit = hold

        def __init__(self, **kwargs: object) -> None:
            store = kwargs["store"]
            assert type(store) is LabJobStore
            port = LabSchedulingBarrierPort(store.path.parent / "claims", store=store)
            super().__init__(scheduling_control=port, **kwargs)

        @contextmanager
        def _v2_emit_permit(self, record: object) -> Iterator[object]:
            if self.hold_emit:
                yield False
            else:
                with super()._v2_emit_permit(record) as permission:
                    yield permission

    return SchedulingSourceScheduler


def test_tsc_09_v2_issued_source_permit_is_draining_without_worker_authority(tmp_path: Path) -> None:
    from tests.unit.test_lab_scheduler_source_stage import _pending_source_stage

    pending = _pending_source_stage(tmp_path, scheduler_type=_scheduling_source_type())
    scheduler = pending.scheduler
    try:
        assert scheduler.claim_spool is None and scheduler.claim_worker_ids == ()
        port = scheduler.scheduling_control
        marker = port.read_barrier()
        assert len(marker.source_permits) == 1
        port.apply_command(command(pending.store, paused=True, expected_version=0), lease=scheduler.lease, now=pending.current + timedelta(seconds=1))
        state = port.reconcile(lease=scheduler.lease, now=pending.current + timedelta(seconds=2))
        assert state.desired_paused and not state.applied_paused and state.draining_count == 1
        scheduler.clock = lambda: pending.current + timedelta(seconds=3)
        scheduler.run_once()
        assert len(port.read_barrier().source_permits) == 1
        assert pending.store.scheduling_state().draining_count == 1
    finally:
        scheduler.release()


def test_tsc_09_v2_held_unissued_source_is_parked_during_pause(tmp_path: Path) -> None:
    from tests.unit.test_lab_scheduler_source_stage import _pending_source_stage

    pending = _pending_source_stage(tmp_path, scheduler_type=_scheduling_source_type(hold=True), advance_source_stage=False)
    scheduler = pending.scheduler
    try:
        port = scheduler.scheduling_control
        assert port.read_barrier().source_permits == ()
        port.apply_command(command(pending.store, paused=True, expected_version=0), lease=scheduler.lease, now=pending.current + timedelta(seconds=1))
        assert port.reconcile(lease=scheduler.lease, now=pending.current + timedelta(seconds=2)).applied_paused
        scheduler.hold_emit = False
        scheduler.clock = lambda: pending.current + timedelta(seconds=3)
        scheduler.run_once()
        assert pending.stage_store.get(pending.binding) is None
        assert pending.store.get_claim_publication(pending.claim_token).status.value == "HELD_SOURCE"
        assert scheduler.claim_spool is None and scheduler.claim_worker_ids == ()
    finally:
        scheduler.release()


def test_tsc_09_multiple_original_claim_drains_survive_new_scheduler_lease(tmp_path: Path) -> None:
    from rquant.lab_jobs import SchedulerLeaseFencedError
    from rquant.runtime_contracts import canonical_sha256

    store, port, old, spool, first = claimed(tmp_path)
    base = _spec()
    second_request = _submit(spec=type(base).model_validate(base.model_dump() | {"deadline": NOW + timedelta(days=1)}))
    store.apply_command(second_request, lease=old, now=NOW)
    store.plan_job(second_request.command.job_id, _v1_definitions(1), lease=old, now=NOW)
    second = store.claim_next_shard(worker_id="worker-b", shard_lease_seconds=30, lease=old, now=NOW)
    assert isinstance(second, LabShardClaim)
    assert first.job_id != second.job_id and first.worker_id != second.worker_id
    spool.consume(spool.publish(second))
    for claim in (first, second):
        spool.admit_execution(claim)
        with spool.scheduling_execution_start(claim, now=NOW):
            pass
    pause = command(store, paused=True, expected_version=0)
    receipt = port.apply_command(pause, lease=old, now=NOW + timedelta(seconds=1))
    expected = {(str(claim.claim_token), canonical_sha256(claim)) for claim in (first, second)}
    assert {(drain.token, drain.identity_hash) for drain in port.read_barrier().drains} == expected
    before = port.reconcile(lease=old, now=NOW + timedelta(seconds=2))
    assert before.desired_paused and not before.applied_paused and before.draining_count == 2
    newer = store.acquire_scheduler_lease(owner_id="scheduler-new", lease_seconds=120, now=NOW + timedelta(seconds=121))
    assert newer.fencing_token > old.fencing_token
    store.recover_expired_jobs(newer, now=NOW + timedelta(seconds=122))
    state = port.reconcile(lease=newer, now=NOW + timedelta(seconds=122))
    assert state.queue_identity == before.queue_identity and state.scheduler_fence == newer.fencing_token
    assert state.desired_version == 1 and not state.applied_paused and state.draining_count == 2
    assert {(drain.token, drain.identity_hash) for drain in port.read_barrier().drains} == expected
    for late in (lambda: port.apply_command(pause, lease=old, now=NOW + timedelta(seconds=123)),
                 lambda: port.reconcile(lease=old, now=NOW + timedelta(seconds=123))):
        with pytest.raises(SchedulerLeaseFencedError):
            late()
    assert store.scheduling_state() == state
    assert store.scheduling_receipt(pause.request_id) == receipt
    spool.close_scheduling_execution(first, now=NOW + timedelta(seconds=123))
    one = port.reconcile(lease=newer, now=NOW + timedelta(seconds=123))
    assert one.draining_count == 1 and not one.applied_paused
    spool.close_scheduling_execution(second, now=NOW + timedelta(seconds=124))
    closed = port.reconcile(lease=newer, now=NOW + timedelta(seconds=124))
    assert closed.applied_paused and closed.draining_count == 0 and closed.applied_version == 1
    reopened = LabJobStore(store.path)
    reopened.initialize()
    next_port = LabSchedulingBarrierPort(port.root, store=reopened)
    assert reopened.scheduling_state() == closed
    assert next_port.read_barrier().queue_identity == before.queue_identity
    assert next_port.read_barrier().state == "closed"
    assert next_port.read_barrier().drains == ()
    print("TSC-09-08: two exact worker claims/ACK intents survive expired old lease; new fence preserves both; close1 waits, close2 applies pause; reopen exact")
