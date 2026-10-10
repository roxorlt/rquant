"""Original command claim facts and deferred study admission, without a new queue."""

from __future__ import annotations

import sqlite3
import threading
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest

from rquant.collaboration_commands import PageControlRoleAuthority
from rquant.collaboration_roles import RoleEntry, RoleState
from rquant.minute_backtest_parameter_study_commands import (
    MinuteParameterStudyExecutionRequest,
    SubmitMinuteParameterStudy,
)
from rquant.minute_backtest_parameters import MinuteNShapeParameters, MinuteParameterSet
from rquant.page_control import (
    DeleteCanvas,
    PageControlCommandConflictError,
    PageControlConsumer,
    PageControlOutbox,
    PageControlService,
    PageControlStatus,
    ExportMinuteReplayZip,
    parse_page_control_command,
)

NOW = datetime(2026, 10, 7, 12, tzinfo=UTC)


def study() -> SubmitMinuteParameterStudy:
    request = MinuteParameterStudyExecutionRequest(
        request_id=UUID(int=471), owner_id="researcher", source_key="synthetic.facts",
        source_version=1, full_input_hash="a" * 64,
        parameters=MinuteParameterSet(parameters=MinuteNShapeParameters()),
        formal_protocol={
            "train_range": {"start_date": date(2026, 7, 31), "end_date": date(2026, 7, 31)},
            "validation_range": {"start_date": date(2026, 8, 3), "end_date": date(2026, 8, 3)},
            "frozen_outer_test_range": {"start_date": date(2026, 8, 4), "end_date": date(2026, 8, 4)},
        },
        settings=({"score_profile": "v1", "top_n": 1, "min_trades": 1},),
        random_seed=17, requested_at=NOW, deadline=NOW + timedelta(hours=1), mode="single",
    )
    return SubmitMinuteParameterStudy(command_id=str(request.request_id), actor_id=request.owner_id,
        requested_at=request.requested_at, request=request)


def control(tmp_path: Path) -> PageControlService:
    roles_path = tmp_path / "roles.json"
    roles_path.write_text(RoleState.create(revision=1, users=(
        RoleEntry(username="admin", role="admin"),
        RoleEntry(username="researcher", role="researcher"),
        RoleEntry(username="viewer", role="viewer"),
    )).model_dump_json())
    roles_path.chmod(0o600)
    roles = PageControlRoleAuthority(mode="enforced", roles_path=roles_path, clock=lambda: NOW)
    outbox = PageControlOutbox(tmp_path / "page-control.sqlite3")
    outbox.path.chmod(0o600)
    consumer = PageControlConsumer(outbox=outbox, data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs", clock=lambda: NOW, consumer_id="actual-consumer")
    return PageControlService(outbox=outbox, consumer=consumer, collaboration=roles)


def original_claim(tmp_path: Path) -> tuple[PageControlOutbox, object, DeleteCanvas]:
    outbox = PageControlOutbox(tmp_path / "page-control.sqlite3")
    command = DeleteCanvas(command_id=str(UUID(int=31)), requested_at=NOW, name="synthetic")
    outbox.enqueue(command)
    claim = outbox.claim_records(limit=1, owner_id="consumer-1", now=NOW, lease_seconds=1)[0]
    outbox.begin_effect(command, owner_id=claim.owner_id, claim_token=claim.claim_token, now=NOW)
    return outbox, claim, command


def test_current_claim_check_is_read_only_and_checks_the_complete_original_body(tmp_path: Path) -> None:
    outbox, claim, command = original_claim(tmp_path)
    with sqlite3.connect(outbox.path) as connection:
        before = connection.iterdump()
        before_dump = tuple(before)
    outbox.require_active_claim(command, owner_id=claim.owner_id, claim_token=claim.claim_token)
    with sqlite3.connect(outbox.path) as connection:
        assert tuple(connection.iterdump()) == before_dump
    changed = command.model_copy(update={"name": "different"})
    with pytest.raises((PermissionError, ValueError), match="claim|original|binding"):
        outbox.require_active_claim(changed, owner_id=claim.owner_id, claim_token=claim.claim_token)


def test_reclaimed_command_rejects_old_token_before_new_effect_has_begun(tmp_path: Path) -> None:
    outbox, old, command = original_claim(tmp_path)
    new = outbox.claim_records(limit=1, owner_id="consumer-2", now=NOW + timedelta(seconds=2))[0]
    assert outbox.effect(command.command_id).claim_token == old.claim_token
    with pytest.raises((PermissionError, RuntimeError), match="claim"):
        outbox.require_active_claim(command, owner_id=old.owner_id, claim_token=old.claim_token)
    outbox.require_active_claim(command, owner_id=new.owner_id, claim_token=new.claim_token)


@pytest.mark.parametrize("field", ["owner_id", "claim_token"])
def test_current_claim_rejects_a_different_worker_or_token(tmp_path: Path, field: str) -> None:
    outbox, claim, command = original_claim(tmp_path)
    supplied = {"owner_id": claim.owner_id, "claim_token": claim.claim_token} | {field: "foreign"}
    with pytest.raises((PermissionError, RuntimeError), match="claim"):
        outbox.require_active_claim(command, **supplied)


def test_current_claim_bounds_stored_payload_before_decoding(tmp_path: Path) -> None:
    outbox, claim, command = original_claim(tmp_path)
    with sqlite3.connect(outbox.path) as connection:
        connection.execute("UPDATE page_control_command SET payload_json=? WHERE command_id=?",
            ("x" * (1024 * 1024 + 1), command.command_id))
    with pytest.raises(ValueError, match="capacity"):
        outbox.require_active_claim(command, owner_id=claim.owner_id, claim_token=claim.claim_token)


def test_authorized_study_is_saved_without_inline_prepare_or_drain(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    service = control(tmp_path)
    command = study()
    assert parse_page_control_command(command.model_dump(mode="json")) == command
    monkeypatch.setattr(service.consumer, "drain", lambda **_: pytest.fail("HTTP study must not drain"))
    proof = service.collaboration.issue_authorization(command.actor_id, command.model_dump(mode="json"))
    receipt = service.submit_authorized(command, proof)
    assert receipt.status is PageControlStatus.PENDING
    assert service.outbox.effect(command.command_id) is None
    assert service.outbox.trusted_command_actor(command.command_id, service.outbox.audit(command.command_id).command_hash) == command.actor_id
    assert service.submit_authorized(command, proof) == receipt
    changed_request = command.request.model_copy(update={"full_input_hash": "b" * 64})
    changed = command.model_copy(update={"request": changed_request})
    proof = service.collaboration.issue_authorization(changed.actor_id, changed.model_dump(mode="json"))
    with pytest.raises(PageControlCommandConflictError):
        service.submit_authorized(changed, proof)


def test_viewer_and_public_study_without_original_private_authorization_are_rejected(tmp_path: Path) -> None:
    service = control(tmp_path)
    command = study()
    with pytest.raises(PermissionError):
        service.collaboration.issue_authorization("viewer", command.model_dump(mode="json"))
    with pytest.raises(PermissionError):
        service.submit(command)


def test_minute_claim_filter_leaves_other_original_commands_pending(tmp_path: Path) -> None:
    outbox = PageControlOutbox(tmp_path / "page-control.sqlite3")
    other = DeleteCanvas(command_id=str(UUID(int=591)), requested_at=NOW, name="synthetic")
    minute = ExportMinuteReplayZip(command_id=str(UUID(int=592)), requested_at=NOW,
        actor_id="researcher", job_id=UUID(int=593), result_hash="a" * 64)
    outbox.enqueue(other)
    outbox.enqueue(minute)
    claims = outbox.claim_records(limit=1, owner_id="minute-consumer", now=NOW,
        target_command_kinds=("submit_minute_replay", "export_minute_replay_zip", "submit_minute_parameter_study"))
    assert tuple(item.command for item in claims) == (minute,)
    assert outbox.receipt(other.command_id).status is PageControlStatus.PENDING
    assert outbox.claim_records(limit=1, owner_id="original-consumer", now=NOW)[0].command == other
    with pytest.raises(ValueError, match="closed commands"):
        outbox.claim_records(limit=1, target_command_kinds=("delete_canvas",))


def test_current_role_read_can_finish_while_a_minute_command_is_consumed(tmp_path: Path) -> None:
    service = control(tmp_path)
    command = study()
    proof = service.collaboration.issue_authorization(command.actor_id, command.model_dump(mode="json"))
    service.submit_authorized(command, proof)
    entered = threading.Event()
    release = threading.Event()
    read_finished = threading.Event()
    observed: list[str] = []

    def consume() -> None:
        with service.outbox.command_fence(command):
            entered.set()
            assert release.wait(5)

    def read_role() -> None:
        observed.append(service.collaboration.current_role(command.actor_id))
        read_finished.set()

    consuming = threading.Thread(target=consume)
    reading = threading.Thread(target=read_role)
    consuming.start()
    assert entered.wait(2)
    reading.start()
    try:
        completed_while_consuming = read_finished.wait(0.5)
    finally:
        release.set()
        consuming.join(2)
        reading.join(2)
    assert not consuming.is_alive() and not reading.is_alive()
    assert observed == ["researcher"]
    assert completed_while_consuming, "original role LOCK_EX blocks background command status/auth reads"


def test_role_cas_exclusive_lock_waits_until_minute_effect_fence_ends(tmp_path: Path) -> None:
    service = control(tmp_path)
    command = study()
    proof = service.collaboration.issue_authorization(command.actor_id, command.model_dump(mode="json"))
    service.submit_authorized(command, proof)
    attempted = threading.Event()
    acquired = threading.Event()

    def write_lock() -> None:
        attempted.set()
        with service.collaboration.locked():
            acquired.set()

    writing = threading.Thread(target=write_lock)
    with service.outbox.command_fence(command):
        writing.start()
        assert attempted.wait(2)
        assert not acquired.wait(0.2)
        assert service.collaboration.current_role(command.actor_id) == "researcher"
    writing.join(2)
    assert not writing.is_alive() and acquired.is_set()


def test_read_only_role_lock_rejects_upgrade_and_direct_write(tmp_path: Path) -> None:
    service = control(tmp_path)
    roles = service.collaboration
    before = roles.roles_path.read_bytes()
    with roles.locked(read_only=True) as directory:
        state = roles.read_state()
        with pytest.raises(PermissionError, match="upgraded"):
            with roles.locked():
                pytest.fail("SH role lock cannot be upgraded")
        with pytest.raises(PermissionError, match="exclusive lock"):
            roles._write_locked(directory, state)
    assert roles.roles_path.read_bytes() == before
    assert not tuple(tmp_path.glob(".roles-*"))


def test_default_role_lock_preserves_original_exclusive_behavior(tmp_path: Path) -> None:
    roles = control(tmp_path).collaboration
    attempted = threading.Event()
    finished = threading.Event()

    def read_role() -> None:
        attempted.set()
        assert roles.current_role("researcher") == "researcher"
        finished.set()

    reading = threading.Thread(target=read_role)
    with roles.locked():
        reading.start()
        assert attempted.wait(2)
        assert not finished.wait(0.2)
    reading.join(2)
    assert not reading.is_alive() and finished.is_set()
