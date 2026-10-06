from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from rquant.runtime_contracts import canonical_sha256

if TYPE_CHECKING:
    from rquant.task_control_commands import OwnedRequestUnitRun, RequestUnitRun

NOW = datetime(2026, 10, 6, 1, 14, tzinfo=UTC)
REQUEST = "c2b8d5ac-dc25-4c55-af11-cb217465b0b4"
PREPARE = "848d1d8b-3b70-4258-a971-58b0beaa8f4d"


def request(**updates: object) -> RequestUnitRun:
    from rquant.task_control_commands import RequestUnitRun

    return RequestUnitRun.model_validate({"command_id": REQUEST, "requested_at": NOW, "generation_id": "generation-a", "unit": "rquant-backup.service"} | updates)


def owned(tmp_path: Path, **updates: object) -> OwnedRequestUnitRun:
    from rquant.task_control_commands import OwnedRequestUnitRun
    from tests.unit.test_task_unit_control import state

    return OwnedRequestUnitRun.model_validate(request().model_dump() | {
        "owner_id": "alice", "accepted_at": NOW + timedelta(seconds=1),
        "metadata_identity": {"path": str(tmp_path / "control.db"), "device": 1, "inode": 2, "instance_id": "a" * 32},
        "original_request_hash": request().request_hash,
        "context": {"host_name": "rquant-test", "boot_id": "12345678-1234-1234-1234-123456789abc", "manifest_digest": "a" * 64,
                    "policy_digest": "b" * 64, "source_payload_hash": "c" * 64, "generation_id": "generation-a", "observed_at": NOW, "mode": "readonly", "runtime_state": state()},
    } | updates)


@pytest.mark.parametrize("extra", [{"actor": "admin"}, {"owner_id": "admin"}, {"confirmed": True}, {"mode": "readonly"}, {"args": ["id"]}])
def test_tsc_01_client_cannot_assert_authority(extra: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        request(**extra)


@pytest.mark.parametrize("changes", [{"command_id": REQUEST.upper()}, {"command_id": "1"}, {"unit": "rquant-backup.service;id"}, {"generation_id": "a" * 129}, {"requested_at": datetime(2026, 10, 6)}, {"confirmation_id": "a" * 4096}])
def test_tsc_02_unit_request_budgets(changes: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        request(**changes)


def test_tsc_05_prepare_freezes_complete_run_uuid_and_draft() -> None:
    from rquant.task_control_commands import PrepareUnitRun, TaskUnitRunDraft

    draft = TaskUnitRunDraft.model_validate(request().model_dump(exclude={"kind", "confirmation_id"}))
    prepare = PrepareUnitRun(command_id=PREPARE, requested_at=NOW, generation_id="generation-a", run=draft)
    assert prepare.run.command_id == REQUEST
    assert prepare.run.draft_hash == request().draft_hash
    with pytest.raises(ValueError):
        PrepareUnitRun.model_validate(prepare.model_dump() | {"command_id": REQUEST})
    with pytest.raises(ValueError):
        PrepareUnitRun.model_validate(prepare.model_dump() | {"generation_id": "different"})


def test_tsc_02_global_scheduler_has_no_fake_job_and_strict_cas() -> None:
    from rquant.task_control_commands import SetLabSchedulingPaused

    raw = {"command_id": REQUEST, "requested_at": NOW, "generation_id": "generation-a", "paused": True, "expected_version": 0}
    command = SetLabSchedulingPaused.model_validate(raw)
    assert command.kind == "set_lab_scheduling_paused"
    for change in ({"job_id": REQUEST}, {"paused": "false"}, {"expected_version": True}, {"expected_version": 2**63}, {"expected_version": -1}):
        with pytest.raises(ValueError):
            SetLabSchedulingPaused.model_validate(raw | change)


def test_tsc_06_owned_original_body_hash_and_identity(tmp_path: Path) -> None:
    command = owned(tmp_path)
    assert command.original() == request()
    assert command.original_request_hash == canonical_sha256(request())
    for change in ({"unit": "rquant-daily.service"}, {"original_request_hash": "d" * 64}, {"generation_id": "elsewhere"}, {"accepted_at": NOW - timedelta(seconds=1)}):
        with pytest.raises(ValueError):
            owned(tmp_path, **change)


def test_tsc_01_public_journal_and_parser_reject_owned_task_controls(tmp_path: Path) -> None:
    from rquant.page_control import PageControlOutbox, parse_page_control_command

    outbox = PageControlOutbox(tmp_path / "control.db")
    command = owned(tmp_path)
    with pytest.raises(ValueError, match="trusted|private|protected"):
        outbox.enqueue(command)
    with pytest.raises(ValueError, match="trusted|private|protected"):
        parse_page_control_command(command.model_dump(mode="json"))
    assert outbox.receipt(REQUEST) is None


def test_tsc_06_original_journal_lookup_actor_body_and_kind(tmp_path: Path) -> None:
    from rquant.page_control import PageControlCommandConflictError, PageControlOutbox

    outbox = PageControlOutbox(tmp_path / "control.db")
    command = owned(tmp_path)
    receipt = outbox.enqueue_trusted_task_control(command)
    matched = outbox.lookup_task_control_command(request(), authenticated_actor_id="alice")
    assert matched == (command, receipt)
    assert outbox.lookup_task_control_command(request(command_id=PREPARE), authenticated_actor_id="alice") is None
    with pytest.raises(PageControlCommandConflictError):
        outbox.lookup_task_control_command(request(), authenticated_actor_id="bob")
    with pytest.raises(PageControlCommandConflictError):
        outbox.lookup_task_control_command(request(unit="rquant-daily.service"), authenticated_actor_id="alice")
    from rquant.task_control_commands import SetLabSchedulingPaused

    with pytest.raises(PageControlCommandConflictError):
        outbox.lookup_task_control_command(SetLabSchedulingPaused(command_id=REQUEST, requested_at=NOW, generation_id="generation-a", expected_version=0, paused=True), authenticated_actor_id="alice")


def test_tsc_06_intent_survives_reopen_and_old_boot_remains_unknown(tmp_path: Path) -> None:
    from rquant.page_control import PageControlOutbox
    from rquant.task_control import TaskControlJournal
    from rquant.task_control_commands import OwnedRequestUnitRun

    outbox = PageControlOutbox(tmp_path / "control.db")
    journal = TaskControlJournal(outbox)
    command = owned(tmp_path, metadata_identity=journal.identity())
    outbox.enqueue_trusted_task_control(command)
    journal.prepare_run(command, now=NOW + timedelta(seconds=1))
    intent = journal.start_intent(command, now=NOW + timedelta(seconds=2), monotonic_ns=1_000_000_000)
    assert intent.stage == "start_intent"
    reopened = TaskControlJournal(outbox)
    assert reopened.run_effect(command).stage == "start_intent"
    assert reopened.start_intent(command, now=NOW + timedelta(seconds=3), monotonic_ns=2_000_000_000) == intent
    fresh_request = request(command_id=PREPARE)
    fresh = OwnedRequestUnitRun.model_validate(command.model_dump() | fresh_request.model_dump() | {"original_request_hash": fresh_request.request_hash})
    outbox.enqueue_trusted_task_control(fresh)
    with pytest.raises(ValueError, match="unresolved"):
        reopened.prepare_run(fresh, now=NOW + timedelta(seconds=3))
    new_boot = "22345678-1234-1234-1234-123456789abc"
    new_context = command.context.model_dump() | {"boot_id": new_boot, "runtime_state": command.context.runtime_state.model_dump() | {"boot_id": new_boot}}
    fresh = OwnedRequestUnitRun.model_validate(fresh.model_dump() | {"context": new_context})
    # A different original accepted boot is a different journal row, never an update to this UUID.
    with pytest.raises(ValueError, match="journal|original"):
        reopened.prepare_run(fresh, now=NOW + timedelta(seconds=3))
    assert reopened.run_effect(command).stage == "start_intent"


def test_tsc_05_real_prepare_actor_draft_expiry_and_one_consumer(tmp_path: Path) -> None:
    from rquant.page_control import PageControlOutbox
    from rquant.task_control import TaskControlJournal
    from rquant.task_control_commands import OwnedPrepareUnitRun, OwnedRequestUnitRun, PrepareUnitRun, TaskUnitRunDraft

    outbox = PageControlOutbox(tmp_path / "control.db")
    journal = TaskControlJournal(outbox)
    base = owned(tmp_path, metadata_identity=journal.identity())
    context = base.context.model_dump() | {"mode": "writer"}
    run = TaskUnitRunDraft.model_validate(request().model_dump(exclude={"kind", "confirmation_id"}))
    prepare = PrepareUnitRun(command_id=PREPARE, requested_at=NOW, generation_id="generation-a", run=run)
    checked = OwnedPrepareUnitRun.model_validate(prepare.model_dump() | {"owner_id": "alice", "accepted_at": NOW, "metadata_identity": journal.identity(), "original_request_hash": prepare.request_hash, "context": context})
    outbox.enqueue_trusted_task_control(checked)
    challenge = journal.prepare_confirmation(checked)
    assert challenge.expires_at == NOW + timedelta(minutes=5)
    assert journal.prepare_confirmation(checked) == challenge
    run_request = request(confirmation_id=challenge.confirmation_id)
    command = OwnedRequestUnitRun.model_validate(base.model_dump() | run_request.model_dump() | {"original_request_hash": run_request.request_hash, "context": context})
    outbox.enqueue_trusted_task_control(command)
    journal.prepare_run(command, now=NOW + timedelta(seconds=2))
    with pytest.raises(ValueError, match="expired"):
        journal.start_intent(command, now=NOW + timedelta(minutes=5), monotonic_ns=1_000_000_000)
    assert journal.run_effect(command).stage == "prepared"
    intent = journal.start_intent(command, now=NOW + timedelta(seconds=3), monotonic_ns=1_000_000_000)
    assert intent.stage == "start_intent"
    assert journal.start_intent(command, now=NOW + timedelta(minutes=7), monotonic_ns=3_000_000_000) == intent
    assert journal.confirmation(challenge.confirmation_id).consumed_by == REQUEST


def test_tsc_06_original_private_journal_replacement_or_orphan_refused(tmp_path: Path) -> None:
    from rquant.page_control import PageControlOutbox
    from rquant.task_control import TaskControlJournal

    outbox = PageControlOutbox(tmp_path / "control.db")
    journal = TaskControlJournal(outbox)
    command = owned(tmp_path, metadata_identity=journal.identity())
    with pytest.raises(ValueError, match="original|journal"):
        journal.prepare_run(command, now=NOW + timedelta(seconds=2))
    outbox.enqueue_trusted_task_control(command)
    journal.prepare_run(command, now=NOW + timedelta(seconds=2))
    replacement = tmp_path / "replacement.db"
    PageControlOutbox(replacement)
    replacement.replace(outbox.path)
    with pytest.raises(ValueError, match="identity|replacement"):
        journal.run_effect(command)


def test_tsc_02_actual_4096_journal_budget_original_lookup_first(tmp_path: Path) -> None:
    from contextlib import closing
    from uuid import UUID

    from rquant.page_control import PageControlOutbox
    from rquant.task_control import TaskControlJournal, validate_task_journal_command
    from rquant.task_control_commands import OwnedRequestUnitRun

    outbox = PageControlOutbox(tmp_path / "control.db")
    identity = TaskControlJournal(outbox).identity()
    command = owned(tmp_path, metadata_identity=identity)
    first = outbox.enqueue_trusted_task_control(command)
    accepted = [command]
    for index in range(4095):
        original = request(command_id=str(UUID(int=index + 1)))
        auxiliary = OwnedRequestUnitRun.model_validate(command.model_dump() | original.model_dump() | {"original_request_hash": original.request_hash})
        receipt = outbox.enqueue_trusted_task_control(auxiliary)
        assert receipt.command_id == auxiliary.command_id
        accepted.append(auxiliary)
    with closing(outbox._connect()) as connection:
        assert connection.execute("SELECT COUNT(*) FROM page_control_command").fetchone()[0] == 4096
        for auxiliary in accepted:
            validate_task_journal_command(connection, auxiliary, identity)
    assert outbox.lookup_task_control_command(request(), authenticated_actor_id="alice") == (command, first)
    assert outbox.enqueue_trusted_task_control(command) == first
    last = accepted[-1]
    assert outbox.lookup_task_control_command(last.original(), authenticated_actor_id="alice")[0] == last
    new_request = request(command_id=PREPARE)
    fresh = OwnedRequestUnitRun.model_validate(command.model_dump() | new_request.model_dump() | {"original_request_hash": new_request.request_hash})
    with pytest.raises(ValueError, match="4096"):
        outbox.enqueue_trusted_task_control(fresh)
    assert outbox.receipt(PREPARE) is None
    assert TaskControlJournal(outbox).run_effect(command) is None
    print("TSC-02-03: 4096 canonical per-UUID unit rows; old lookup/enqueue exact; 4097 rejected; no unit effect")


def test_tsc_02_global_page_journal_4096_valid_records_lookup_first(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from contextlib import closing
    from uuid import UUID

    from rquant.lab_job_center import LabCommandSubmissionFacade
    from rquant.lab_job_protocol import LabCommandSpool
    from rquant.lab_jobs import LabJobReader
    from rquant.lab_scheduling_control import LabSchedulingCommandEnvelope
    from rquant.task_control import validate_task_journal_command
    from rquant.task_control_commands import OwnedSetLabSchedulingPaused, SetLabSchedulingPaused
    from tests.unit.test_lab_scheduling_control import store_and_port

    service, backend, source, _executor, calls = control_service(tmp_path, monkeypatch)
    (tmp_path / "lab").mkdir(mode=0o700)
    store, port = store_and_port(tmp_path / "lab")
    lease = store.acquire_scheduler_lease(owner_id="scheduler", lease_seconds=120, now=NOW)
    state = store.enable_scheduling_control(lease=lease, barrier_port=port, now=NOW)
    view = source.read(generation_id="generation-a")
    full = type(view).model_validate(view.model_dump() | {"scheduling_control": state})
    monkeypatch.setattr(source, "read", lambda **_: full)
    spool = LabCommandSpool(tmp_path / "commands")
    backend.lab_facade = LabCommandSubmissionFacade(reader=LabJobReader(store.path), spool=spool, clock=lambda: NOW)
    original = SetLabSchedulingPaused(command_id=REQUEST, requested_at=NOW, generation_id="generation-a", expected_version=0, paused=True)
    identity = backend.journal.identity()
    first = service._submit_trusted_task_control(original, authenticated_actor_id="admin", verified_metadata_identity=identity)
    checked, _ = service.outbox.lookup_task_control_command(original, authenticated_actor_id="admin")
    assert type(checked) is OwnedSetLabSchedulingPaused
    accepted = [checked]
    for index in range(4095):
        auxiliary_request = SetLabSchedulingPaused.model_validate(original.model_dump() | {"command_id": str(UUID(int=index + 1))})
        envelope = LabSchedulingCommandEnvelope(request_id=UUID(auxiliary_request.command_id), command=checked.envelope.command)
        auxiliary = OwnedSetLabSchedulingPaused.model_validate(checked.model_dump() | auxiliary_request.model_dump() | {"original_request_hash": auxiliary_request.request_hash, "envelope": envelope})
        receipt = service.outbox.enqueue_trusted_task_control(auxiliary)
        assert receipt.command_id == auxiliary_request.command_id
        accepted.append(auxiliary)
    with closing(service.outbox._connect()) as connection:
        assert connection.execute("SELECT COUNT(*) FROM page_control_command").fetchone()[0] == 4096
        for auxiliary in accepted:
            validate_task_journal_command(connection, auxiliary, identity)
    entry = spool.find(checked.envelope.request_id)
    applied = port.apply_command(entry.envelope, lease=lease, now=NOW + timedelta(seconds=1))
    spool.ack(entry, applied)
    head = store.scheduling_state()
    monkeypatch.setattr(source, "read", lambda **_: pytest.fail("original global UUID must lookup before a new Serving head"))
    assert service._resume_trusted_task_control(original, authenticated_actor_id="admin") == first
    assert service._submit_trusted_task_control(original, authenticated_actor_id="admin", verified_metadata_identity=identity) == first
    full = type(view).model_validate(view.model_dump() | {"scheduling_control": head})
    monkeypatch.setattr(source, "read", lambda **_: full)
    backend.clock = lambda: NOW + timedelta(seconds=2)
    fresh = SetLabSchedulingPaused(command_id=PREPARE, requested_at=NOW, generation_id="generation-a", expected_version=1, paused=False)
    with pytest.raises(ValueError, match="4096"):
        service._submit_trusted_task_control(fresh, authenticated_actor_id="admin", verified_metadata_identity=identity)
    assert service.outbox.receipt(PREPARE) is None
    assert spool.find(UUID(PREPARE)) is None and len(spool.pending()) == 0
    assert store.scheduling_state() == head and head.desired_version == 1
    assert calls == []
    print("TSC-02-03/TSC-08-06: 4096 canonical global PageControl rows; old service retry survives head v1; 4097 has no spool/CAS/unit effect")


def control_service(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, writer: bool = False):
    from contextlib import contextmanager
    from rquant.page_control import PageControlConsumer, PageControlOutbox, PageControlService
    from rquant.task_control import TaskControlJournal, TaskControlPageControlBackend
    from rquant.task_control_admission import TaskCenterServingSource, TaskCenterServingView
    from rquant.task_cpu import SLICES, TaskCpuResult
    from rquant.task_unit_control import SystemdUnitAttempt, SystemdUnitRunExecutor
    from tests.unit.test_task_center_projection import _sample
    from tests.unit.test_task_unit_control import policy, state
    from tests.unit.test_ops_status import _manifest

    outbox = PageControlOutbox(tmp_path / "control.db")
    journal = TaskControlJournal(outbox)
    source = TaskCenterServingSource(tmp_path / "serving", clock=lambda: NOW)
    manifest = _manifest()
    snapshot = type(_sample()).model_validate(_sample(at=NOW).model_dump() | {"host_name": manifest.host_name, "manifest_digest": manifest.digest})
    view = TaskCenterServingView(generation_id="generation-a", ops_generation_id="a" * 64, snapshot=snapshot,
        cpu=tuple(TaskCpuResult(slice_name=name, percent=None, reason="capture_unavailable") for name in SLICES), runs=())
    monkeypatch.setattr(source, "read", lambda **_: view)
    executor = SystemdUnitRunExecutor(manifest_path=tmp_path / "manifest", policy_path=tmp_path / "policy", manifest_public_key_pem=b"public", policy_public_key_pem=b"public", clock=lambda: NOW)
    monkeypatch.setattr(executor, "configuration", lambda: (manifest, policy(mode="writer" if writer else "readonly")))
    monkeypatch.setattr(executor, "read_state", lambda _: state())
    calls: list[str] = []

    @contextmanager
    def ready(*args: object, **kwargs: object):
        yield object()

    def lost(**kwargs: object):
        calls.append(str(kwargs["command_id"]))
        return SystemdUnitAttempt(stage="unknown", reason="synthetic_reply_lost")

    monkeypatch.setattr(executor, "prepare", ready)
    monkeypatch.setattr(executor, "invoke", lost)
    backend = TaskControlPageControlBackend(journal=journal, source=source, executor=executor,
        operators=("alice",), scheduling_admins=("admin",), enabled=True, clock=lambda: NOW)
    consumer = PageControlConsumer(outbox=outbox, data_dir=tmp_path / "data", log_dir=tmp_path / "logs", task_control_backend=backend, clock=lambda: NOW)
    return PageControlService(outbox=outbox, consumer=consumer), backend, source, executor, calls


def test_tsc_06_original_service_intent_reply_loss_retry_never_starts_twice(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant.page_control import PageControlStatus

    service, backend, source, _executor, calls = control_service(tmp_path, monkeypatch)
    first = service._submit_trusted_task_control(request(), authenticated_actor_id="alice", verified_metadata_identity=backend.journal.identity())
    assert first.status is PageControlStatus.SUCCEEDED
    matched = service.outbox.lookup_task_control_command(request(), authenticated_actor_id="alice")
    assert backend.journal.run_effect(matched[0]).stage == "unknown"
    assert calls == [REQUEST]
    backend.enabled = False
    monkeypatch.setattr(source, "read", lambda **_: pytest.fail("exact retry must not read a newer Serving generation"))
    assert service._resume_trusted_task_control(request(), authenticated_actor_id="alice") == first
    assert service._submit_trusted_task_control(request(), authenticated_actor_id="alice", verified_metadata_identity=backend.journal.identity()) == first
    assert calls == [REQUEST]
    backend.operators = ()
    with pytest.raises(PermissionError):
        service._resume_trusted_task_control(request(), authenticated_actor_id="alice")


def test_tsc_05_original_service_writer_requires_real_prepare_and_exact_draft(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant.task_control_commands import PrepareUnitRun, TaskUnitRunDraft

    service, backend, _source, _executor, calls = control_service(tmp_path, monkeypatch, writer=True)
    with pytest.raises(ValueError, match="prepare|confirmation"):
        service._submit_trusted_task_control(request(), authenticated_actor_id="alice", verified_metadata_identity=backend.journal.identity())
    assert service.outbox.receipt(REQUEST) is None and calls == []
    draft = TaskUnitRunDraft.model_validate(request().model_dump(exclude={"kind", "confirmation_id"}))
    prepare = PrepareUnitRun(command_id=PREPARE, requested_at=NOW, generation_id="generation-a", run=draft)
    receipt = service._submit_trusted_task_control(prepare, authenticated_actor_id="alice", verified_metadata_identity=backend.journal.identity())
    checked, _ = service.outbox.lookup_task_control_command(prepare, authenticated_actor_id="alice")
    challenge = backend.journal.prepare_confirmation(checked)
    assert receipt.result["command_id"] == PREPARE
    confirmed = request(confirmation_id=challenge.confirmation_id)
    service._submit_trusted_task_control(confirmed, authenticated_actor_id="alice", verified_metadata_identity=backend.journal.identity())
    assert calls == [REQUEST] and backend.journal.confirmation(challenge.confirmation_id).consumed_by == REQUEST


def test_tsc_06_preflight_failure_records_no_intent_and_replaced_original_is_not_retargeted(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from contextlib import contextmanager
    from rquant.page_control import PageControlOutbox, PageControlStatus

    service, backend, _source, executor, calls = control_service(tmp_path, monkeypatch)

    @contextmanager
    def denied(*args: object, **kwargs: object):
        raise PermissionError("synthetic monitor unavailable before durable intent")
        yield

    monkeypatch.setattr(executor, "prepare", denied)
    receipt = service._submit_trusted_task_control(request(), authenticated_actor_id="alice", verified_metadata_identity=backend.journal.identity())
    assert receipt.status is PageControlStatus.SUCCEEDED
    checked, _ = service.outbox.lookup_task_control_command(request(), authenticated_actor_id="alice")
    effect = backend.journal.run_effect(checked)
    assert effect.stage == "rejected" and effect.intent_at is None and calls == []
    replacement = tmp_path / "replacement.db"
    PageControlOutbox(replacement)
    replacement.replace(service.outbox.path)
    with pytest.raises((ValueError, KeyError), match="original|identity|found"):
        service._resume_trusted_task_control(request(), authenticated_actor_id="alice")


def test_tsc_08_owned_global_command_uses_original_facade_and_old_uuid_not_new_head(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant.lab_job_center import LabCommandSubmissionFacade
    from rquant.lab_job_protocol import LabCommandSpool
    from rquant.lab_jobs import LabJobReader
    from rquant.task_control_commands import SetLabSchedulingPaused
    from tests.unit.test_lab_scheduling_control import store_and_port

    service, backend, source, _executor, _calls = control_service(tmp_path, monkeypatch)
    (tmp_path / "lab").mkdir(mode=0o700)
    store, port = store_and_port(tmp_path / "lab")
    lease = store.acquire_scheduler_lease(owner_id="scheduler", lease_seconds=120, now=NOW)
    state = store.enable_scheduling_control(lease=lease, barrier_port=port, now=NOW)
    view = source.read(generation_id="generation-a")
    full = type(view).model_validate(view.model_dump() | {"scheduling_control": state})
    monkeypatch.setattr(source, "read", lambda **_: full)
    spool = LabCommandSpool(tmp_path / "commands")
    backend.lab_facade = LabCommandSubmissionFacade(reader=LabJobReader(store.path), spool=spool, clock=lambda: NOW)
    pause = SetLabSchedulingPaused(command_id=REQUEST, requested_at=NOW, generation_id="generation-a", expected_version=0, paused=True)
    receipt = service._submit_trusted_task_control(pause, authenticated_actor_id="admin", verified_metadata_identity=backend.journal.identity())
    checked, _ = service.outbox.lookup_task_control_command(pause, authenticated_actor_id="admin")
    assert checked.original() == pause and checked.request_hash == pause.request_hash
    entry = spool.find(checked.envelope.request_id)
    assert entry.envelope == checked.envelope
    applied = port.apply_command(entry.envelope, lease=lease, now=NOW + timedelta(seconds=1))
    spool.ack(entry, applied)
    monkeypatch.setattr(source, "read", lambda **_: pytest.fail("old global UUID cannot read the new head"))
    assert service._resume_trusted_task_control(pause, authenticated_actor_id="admin") == receipt
    assert store.scheduling_state().desired_version == 1


def test_tsc_01_private_admission_binds_original_result_and_rejects_actor_swap(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant.task_control_admission import TaskControlAdmission, TaskControlAdmissionResult

    service, backend, _source, _executor, calls = control_service(tmp_path, monkeypatch)
    admission = TaskControlAdmission(service, backend=backend)
    result = admission.submit(request(), authenticated_actor_id="alice", verified_metadata_identity=backend.journal.identity())
    assert result.original_request == request() and result.unit_effect.stage == "unknown"
    assert result.confirmation is None and result.scheduling_submission is None
    assert admission.lookup(request(), authenticated_actor_id="alice") == result
    assert admission.resume(request(), authenticated_actor_id="alice") == result and calls == [REQUEST]
    with pytest.raises(ValueError, match="identity|original|actor"):
        TaskControlAdmissionResult.model_validate(result.model_dump() | {"owner_id": "bob"})
    with pytest.raises(PermissionError):
        admission.lookup(request(), authenticated_actor_id="bob")


def test_tsc_01_private_client_does_not_accept_changed_request_receipt(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant.task_control_admission import TaskControlAdmission, TaskControlAdmissionClient, TaskControlAdmissionUnavailableError

    service, backend, _source, _executor, _calls = control_service(tmp_path, monkeypatch)
    result = TaskControlAdmission(service, backend=backend).submit(request(), authenticated_actor_id="alice", verified_metadata_identity=backend.journal.identity())
    client = TaskControlAdmissionClient(Path("/private/tmp/m13-private-client-test.sock"), expected_service_uid=1234, shared_gid=20)
    monkeypatch.setattr(client, "_call", lambda *args: result.model_dump(mode="json"))
    assert client.resume(request(), authenticated_actor_id="alice") == result
    with pytest.raises(TaskControlAdmissionUnavailableError):
        client.resume(request(unit="rquant-daily.service"), authenticated_actor_id="alice")
    with pytest.raises(TaskControlAdmissionUnavailableError):
        client.resume(request(), authenticated_actor_id="admin")


def test_tsc_06_new_uuid_for_unresolved_unit_is_rejected_before_original_journal_acceptance(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    service, backend, _source, _executor, calls = control_service(tmp_path, monkeypatch)
    service._submit_trusted_task_control(request(), authenticated_actor_id="alice", verified_metadata_identity=backend.journal.identity())
    second = request(command_id=PREPARE)
    with pytest.raises(ValueError, match="unresolved|original"):
        service._submit_trusted_task_control(second, authenticated_actor_id="alice", verified_metadata_identity=backend.journal.identity())
    assert service.outbox.receipt(PREPARE) is None and calls == [REQUEST]


def test_tsc_07_original_run_full_facts_must_follow_persisted_intent_and_stage_cannot_regress(tmp_path: Path) -> None:
    from rquant.page_control import PageControlOutbox
    from rquant.task_control import TaskControlJournal
    from rquant.task_unit_control import TaskSystemdRunWindow, bind_systemd_unit_run
    from tests.unit.test_task_unit_control import systemd_window

    outbox = PageControlOutbox(tmp_path / "control.db")
    journal = TaskControlJournal(outbox)
    command = owned(tmp_path, metadata_identity=journal.identity())
    outbox.enqueue_trusted_task_control(command)
    journal.prepare_run(command, now=NOW + timedelta(seconds=1))
    journal.start_intent(command, now=NOW + timedelta(seconds=2), monotonic_ns=800_000_000)
    raw = systemd_window(request_hash=command.original_request_hash, manifest_digest=command.context.manifest_digest,
        observed_at=NOW + timedelta(seconds=6))
    raw["invocation"] = dict(raw["invocation"]) | {"started_at": NOW + timedelta(seconds=3), "ended_at": NOW + timedelta(seconds=5)}
    window = TaskSystemdRunWindow.model_validate(raw)
    run = bind_systemd_unit_run(window)
    before = TaskSystemdRunWindow.model_validate(window.model_dump() | {"invocation": window.invocation.model_dump() | {"started_at": NOW + timedelta(seconds=1)}})
    with pytest.raises(ValueError, match="intent|original"):
        journal.record_run(command, now=NOW + timedelta(seconds=6), stage="completed", job_path=run.job_witness.job_path, run=bind_systemd_unit_run(before), window=before)
    started_window = TaskSystemdRunWindow.model_validate(window.model_dump() | {"events": (window.events[0],), "invocation": window.invocation.model_dump() | {"ended_at": None, "ended_monotonic_ns": None, "result": None, "exec_status": None}})
    started = bind_systemd_unit_run(started_window)
    journal.record_run(command, now=NOW + timedelta(seconds=6), stage="started", job_path=started.job_witness.job_path, run=started, window=started_window)
    with pytest.raises(ValueError, match="roll back"):
        journal.record_run(command, now=NOW + timedelta(seconds=6), stage="acknowledged")
    unknown = journal.record_run(command, now=NOW + timedelta(seconds=7), stage="unknown", reason="observation_unavailable")
    assert unknown.run == started and unknown.window == started_window
    completed = journal.record_run(command, now=NOW + timedelta(seconds=8), stage="completed", job_path=run.job_witness.job_path, run=run, window=window)
    assert completed.run.duration_ns == 2_000_000_000


def test_tsc_05_writer_confirmation_reuses_original_context_after_fresh_read_time(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant.task_control_commands import PrepareUnitRun, TaskUnitRunDraft
    from tests.unit.test_task_unit_control import state

    service, backend, _source, executor, calls = control_service(tmp_path, monkeypatch, writer=True)
    draft = TaskUnitRunDraft.model_validate(request().model_dump(exclude={"kind", "confirmation_id"}))
    prepared = PrepareUnitRun(command_id=PREPARE, requested_at=NOW, generation_id="generation-a", run=draft)
    service._submit_trusted_task_control(prepared, authenticated_actor_id="alice", verified_metadata_identity=backend.journal.identity())
    with backend.journal._transaction() as connection:
        identifier = connection.execute("SELECT confirmation_id FROM page_control_unit_confirmation").fetchone()[0]
    challenge = backend.journal.confirmation(identifier)
    later = NOW + timedelta(seconds=2)
    backend.clock = lambda: later
    service.consumer.clock = lambda: later
    monkeypatch.setattr(executor, "read_state", lambda _unit: state(observed_at=later))
    confirmed = type(request()).model_validate(request().model_dump() | {"confirmation_id": identifier})
    service._submit_trusted_task_control(confirmed, authenticated_actor_id="alice", verified_metadata_identity=backend.journal.identity())
    original, _receipt = service.outbox.lookup_task_control_command(confirmed, authenticated_actor_id="alice")
    assert original.context == challenge.context
    assert original.accepted_at == later and calls == [REQUEST]


def persisted_started_unit(tmp_path: Path) -> tuple[object, object, object, object]:
    from rquant.page_control import PageControlOutbox
    from rquant.task_control import TaskControlJournal
    from rquant.task_unit_control import TaskSystemdRunWindow, bind_systemd_unit_run
    from tests.unit.test_task_unit_control import systemd_window

    outbox = PageControlOutbox(tmp_path / "control.db")
    journal = TaskControlJournal(outbox)
    command = owned(tmp_path, metadata_identity=journal.identity())
    outbox.enqueue_trusted_task_control(command)
    journal.prepare_run(command, now=NOW + timedelta(seconds=1))
    journal.start_intent(command, now=NOW + timedelta(seconds=2), monotonic_ns=800_000_000)
    raw = systemd_window(request_hash=command.original_request_hash, manifest_digest=command.context.manifest_digest,
        observed_at=NOW + timedelta(seconds=6))
    raw["invocation"] = dict(raw["invocation"]) | {"started_at": NOW + timedelta(seconds=3), "ended_at": NOW + timedelta(seconds=5)}
    completed = TaskSystemdRunWindow.model_validate(raw)
    started = TaskSystemdRunWindow.model_validate(completed.model_dump() | {
        "events": (completed.events[0],), "invocation": completed.invocation.model_dump() | {
            "ended_at": None, "ended_monotonic_ns": None, "result": None, "exec_status": None}})
    run = bind_systemd_unit_run(started)
    journal.record_run(command, now=NOW + timedelta(seconds=6), stage="started", job_path=run.job_witness.job_path, run=run, window=started)
    return outbox, journal, command, completed


@pytest.mark.parametrize("altered", ["start", "caller", "call", "job_new"])
def test_tsc_07_later_completion_cannot_replace_original_start_facts(tmp_path: Path, altered: str) -> None:
    from rquant.task_unit_control import TaskSystemdRunWindow, bind_systemd_unit_run

    _outbox, journal, command, completed = persisted_started_unit(tmp_path)
    raw = completed.model_dump()
    if altered == "start":
        raw["invocation"]["started_at"] += timedelta(milliseconds=1)
    elif altered == "caller":
        raw["caller_pid"] += 1
        raw["calls"][0]["pid"] += 1
    elif altered == "call":
        raw["calls"][0]["monotonic_ns"] += 1
    else:
        raw["events"][0]["monotonic_ns"] += 1
    changed = TaskSystemdRunWindow.model_validate(raw)
    run = bind_systemd_unit_run(changed)
    with pytest.raises(ValueError, match="original.*(facts|witness)|replace"):
        journal.record_run(command, now=NOW + timedelta(seconds=8), stage="completed", job_path=run.job_witness.job_path, run=run, window=changed)


def test_tsc_07_readonly_run_source_uses_exact_original_journal_and_never_creates_db(tmp_path: Path) -> None:
    import sqlite3
    from rquant.task_center_runtime import TaskUnitRunSource

    outbox, journal, command, _completed = persisted_started_unit(tmp_path)
    source = TaskUnitRunSource(identity=journal.identity())
    arguments = {"host_name": command.context.host_name, "boot_id": command.context.boot_id,
        "manifest_digest": command.context.manifest_digest, "units": (command.unit,), "cutoff": NOW + timedelta(seconds=8)}
    assert source.read(**arguments) == (journal.run_effect(command).run,)
    assert source.read(**(arguments | {"boot_id": "22345678-1234-1234-1234-123456789abc"})) == ()
    with pytest.raises(ValueError, match="cutoff|future"):
        source.read(**(arguments | {"cutoff": NOW + timedelta(seconds=4)}))
    with sqlite3.connect(outbox.path) as connection:
        connection.execute("UPDATE page_control_command SET command_hash=? WHERE command_id=?", ("0" * 64, command.command_id))
    with pytest.raises(ValueError, match="original"):
        source.read(**arguments)
    absent = tmp_path / "absent.db"
    replacement = type(command.metadata_identity).model_validate(command.metadata_identity.model_dump() | {"path": str(absent)})
    with pytest.raises(ValueError):
        TaskUnitRunSource(identity=replacement).read(**arguments)
    assert not absent.exists()


def test_tsc_07_recovery_observes_original_started_invocation_without_another_start(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant.task_unit_control import SystemdUnitAttempt, bind_systemd_unit_run

    _service, backend, _source, executor, starts = control_service(tmp_path / "service", monkeypatch)
    _outbox, journal, command, completed = persisted_started_unit(tmp_path / "original")
    backend.journal = journal
    backend.clock = lambda: NOW + timedelta(seconds=8)
    seen: list[object] = []

    def observe(window: object, *, policy_digest: str) -> SystemdUnitAttempt:
        seen.append(window)
        assert policy_digest == command.context.policy_digest
        run = bind_systemd_unit_run(completed)
        return SystemdUnitAttempt(stage="completed", job_path=run.job_witness.job_path, run=run, window=completed)

    monkeypatch.setattr(executor, "observe", observe, raising=False)
    old = journal.run_effect(command)
    backend.recover(command)
    assert seen == [old.window] and starts == []
    assert journal.run_effect(command).stage == "completed"
    backend.recover(command)
    assert len(seen) == 1 and starts == []
