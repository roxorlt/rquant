from __future__ import annotations

import os
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from importlib import import_module, util
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import TYPE_CHECKING
from uuid import UUID

import pytest
from pydantic import JsonValue

from rquant.lab_job_center import CommandSubmissionReceipt
from rquant.minute_backtest_commands import MinuteCommandWriter, SubmitMinuteReplay
from rquant.minute_backtest_parameter_study_commands import SubmitMinuteParameterStudy
from rquant.page_control import PageControlClaim, PageControlService
from rquant.strict_json import canonical_json_bytes, strict_model_validate_json

if TYPE_CHECKING:
    from rquant.minute_backtest_parameter_study_journal import MinuteParameterStudyCommandWriter


def journal_api() -> ModuleType:
    name = "rquant.minute_backtest_parameter_study_journal"
    assert util.find_spec(name) is not None, "original study journal adapter is missing"
    return import_module(name)


def submission_receipt(index: int) -> CommandSubmissionReceipt:
    # Typed receipt examples are serialization input, not proof of a real submit.
    return CommandSubmissionReceipt(
        request_id=UUID(int=index + 1),
        command_type="submit",
        job_id=UUID(int=index + 101),
        spool={
            "path": Path("/explicit-synthetic-spool") / str(index),
            "state": "pending",
            "device": 0,
            "inode": index + 1,
            "content_hash": "a" * 64,
        },
    )


def test_journal_rejects_a_nonoriginal_minute_writer() -> None:
    api = journal_api()
    with pytest.raises(TypeError, match="original MinuteCommandWriter"):
        api.MinuteParameterStudyCommandWriter(object())


def test_submission_receipt_preserves_original_order_and_identities() -> None:
    api = journal_api()
    children = (submission_receipt(3), submission_receipt(1))
    result = api.MinuteParameterStudySubmissionReceipt(
        parent_command_id="00000000-0000-4000-8000-000000000001",
        plan_id="b" * 64,
        state="submitted",
        receipts=children,
    )
    restored = type(result).model_validate_json(result.model_dump_json())
    assert restored == result
    assert restored.receipts == children
    assert restored.receipts[0].request_id == UUID(int=4)


def test_submission_receipt_rejects_duplicate_child_identity() -> None:
    api = journal_api()
    child = submission_receipt(0)
    with pytest.raises(ValueError, match="duplicate child"):
        api.MinuteParameterStudySubmissionReceipt(
            parent_command_id="00000000-0000-4000-8000-000000000001",
            plan_id="b" * 64,
            state="submitted",
            receipts=(child, child),
        )


def test_unavailable_receipt_preserves_the_original_reason_without_a_job() -> None:
    api = journal_api()
    result = api.MinuteParameterStudySubmissionReceipt(
        parent_command_id="00000000-0000-4000-8000-000000000001",
        plan_id="b" * 64,
        state="unavailable",
        receipts=(),
        unavailable_reasons=("insufficient_fold_dates",),
    )
    assert result.receipts == ()
    assert result.unavailable_reasons == ("insufficient_fold_dates",)
    with pytest.raises(ValueError, match="unavailable"):
        type(result).model_validate(
            result.model_dump(mode="python") | {"receipts": (submission_receipt(0),)}
        )


def test_an_original_class_alone_does_not_authorize_study_work() -> None:
    api = journal_api()
    # Deliberately invalid installation: the original class is not a trusted binding.
    original = MinuteCommandWriter(object())
    writer = api.MinuteParameterStudyCommandWriter(original)
    for method, args in (
        (writer.bind_owner_authority, (object(),)),
        (writer.freeze, (object(),)),
        (writer.submit, (object(), {})),
        (writer.recover, (object(), {})),
    ):
        with pytest.raises((PermissionError, TypeError), match="original|bound"):
            method(*args)


def test_submitted_receipt_cannot_relabel_empty_progress_as_success() -> None:
    api = journal_api()
    with pytest.raises(ValueError, match="submitted"):
        api.MinuteParameterStudySubmissionReceipt(
            parent_command_id="00000000-0000-4000-8000-000000000001",
            plan_id="b" * 64,
            state="submitted",
            receipts=(),
        )


def test_submission_receipt_keeps_the_original_control_budget() -> None:
    api = journal_api()
    children = tuple(
        submission_receipt(index).model_copy(
            update={
                "spool": submission_receipt(index).spool.model_copy(
                    update={"path": Path("/explicit-synthetic/") / ("x" * 4096) / str(index)}
                )
            }
        )
        for index in range(256)
    )
    with pytest.raises(ValueError, match="byte budget"):
        api.MinuteParameterStudySubmissionReceipt(
            parent_command_id="00000000-0000-4000-8000-000000000001",
            plan_id="b" * 64,
            state="submitted",
            receipts=children,
        )


def test_claim_read_fence_denies_command_stolen_before_new_effect_starts(tmp_path: Path) -> None:
    from rquant.page_control import PageControlOutbox, PageControlStatus, SaveCanvas

    # Original legacy command exposes the same physical claim race, not study authorization.
    now = datetime(2026, 7, 31, 2, tzinfo=UTC)
    outbox = PageControlOutbox(tmp_path / "claim-proof.sqlite3")
    command = SaveCanvas(command_id="original-claim-proof", requested_at=now, name="原租约证明")
    outbox.enqueue(command)
    first = outbox.claim_records(limit=1, owner_id="original-worker", now=now)[0]
    old_effect, created = outbox.begin_effect(
        command, owner_id=first.owner_id, claim_token=first.claim_token, now=now
    )
    assert created
    second = outbox.claim_records(
        limit=1, owner_id="replacement-worker", now=now + timedelta(seconds=31)
    )[0]
    assert second.claim_token != first.claim_token
    assert outbox.audit(command.command_id).status is PageControlStatus.PROCESSING
    assert outbox.effect(command.command_id) == old_effect
    assert hasattr(outbox, "require_active_claim"), (
        "STUDY-CLAIM-01 original claim read fence missing"
    )
    with pytest.raises((PermissionError, RuntimeError), match="claim"):
        outbox.require_active_claim(
            command, owner_id=first.owner_id, claim_token=first.claim_token
        )


@dataclass(frozen=True)
class OriginalStudyControl:
    service: PageControlService
    writer: MinuteCommandWriter
    journal: MinuteParameterStudyCommandWriter
    carrier: SimpleNamespace
    roles_path: Path


@pytest.fixture(scope="module")
def actual_carrier(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SimpleNamespace]:
    from tests.unit.test_minute_backtest_parameter_study_execution import carrier_owner

    original = carrier_owner.__wrapped__(tmp_path_factory)
    carrier = next(original)
    try:
        yield carrier
    finally:
        original.close()


def original_control(carrier: SimpleNamespace, root: Path) -> OriginalStudyControl:
    from rquant.collaboration_commands import PageControlRoleAuthority
    from rquant.collaboration_roles import RoleEntry, RoleState
    from rquant.page_control import PageControlConsumer, PageControlOutbox

    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    root.chmod(0o700)
    roles_path = root / "roles.json"
    owner_id = carrier.baselines["n_shape"].receipt.frozen.runtime.owner_id
    roles_path.write_text(
        RoleState.create(
            revision=1,
            users=(
                RoleEntry(username="admin", role="admin"),
                RoleEntry(username=owner_id, role="researcher"),
                RoleEntry(username="foreign", role="researcher"),
                RoleEntry(username="viewer", role="viewer"),
            ),
        ).model_dump_json()
    )
    roles_path.chmod(0o600)
    roles = PageControlRoleAuthority(
        mode="enforced", roles_path=roles_path, clock=carrier.installed.clock
    )
    outbox = PageControlOutbox(root / "page-control.sqlite3")
    outbox.path.chmod(0o600)
    writer = MinuteCommandWriter(carrier.installed)
    journal = journal_api().MinuteParameterStudyCommandWriter(writer)
    consumer = PageControlConsumer(
        outbox=outbox,
        data_dir=root / "data",
        log_dir=root / "logs",
        clock=carrier.installed.clock,
        consumer_id="actual-study-consumer",
        minute_backend=writer,
        minute_study_backend=journal,
    )
    service = PageControlService(outbox=outbox, consumer=consumer, collaboration=roles)
    return OriginalStudyControl(service, writer, journal, carrier, roles_path)


def original_parent(carrier: SimpleNamespace, **updates: object) -> SubmitMinuteParameterStudy:
    from tests.unit.test_minute_backtest_parameter_study_execution import carrier_request

    request = carrier_request(carrier, **updates)
    return SubmitMinuteParameterStudy(
        command_id=str(request.request_id),
        requested_at=request.requested_at,
        actor_id=request.owner_id,
        request=request,
    )


def original_admit(
    control: OriginalStudyControl, command: SubmitMinuteParameterStudy
) -> PageControlClaim:
    original_enqueue(control, command)
    service = control.service
    claim = service.outbox.claim_records(
        limit=1, owner_id=service.consumer.consumer_id, now=control.carrier.installed.clock()
    )[0]
    assert claim.command == command
    service.outbox.begin_effect(
        command,
        owner_id=claim.owner_id,
        claim_token=claim.claim_token,
        now=control.carrier.installed.clock(),
    )
    return claim


def original_enqueue(
    control: OriginalStudyControl, command: SubmitMinuteParameterStudy
) -> None:
    from rquant.page_control import PageControlStatus

    service = control.service
    proof = service.collaboration.issue_authorization(
        command.actor_id, command.model_dump(mode="json")
    )
    receipt = service.submit_authorized(command, proof)
    assert receipt.status is PageControlStatus.PENDING
    assert service.outbox.effect(command.command_id) is None


@pytest.fixture(scope="module")
def complete_original_effect(
    actual_carrier: SimpleNamespace, tmp_path_factory: pytest.TempPathFactory
) -> tuple[SubmitMinuteParameterStudy, JsonValue]:
    control = original_control(actual_carrier, tmp_path_factory.mktemp("original-freeze"))
    command = original_parent(
        actual_carrier,
        settings=(
            {"score_profile": "v1", "top_n": 1, "min_trades": 1},
            {"score_profile": "accumulation_heavy", "top_n": 2, "min_trades": 1},
        ),
    )
    original_admit(control, command)
    marker = control.journal.freeze(command)
    assert control.service.outbox.effect(command.command_id).result is None
    assert not actual_carrier.installed.commands.spool.pending()
    control.writer.close()
    return command, marker


def original_persist(
    control: OriginalStudyControl, command: SubmitMinuteParameterStudy, marker: JsonValue
) -> PageControlClaim:
    claim = original_admit(control, command)
    control.service.outbox.record_started_effect_result(
        command.command_id,
        result=marker,
        owner_id=claim.owner_id,
        claim_token=claim.claim_token,
    )
    return claim


def test_real_partial_submit_lost_reply_keeps_original_effect_and_recovers_same_children(
    actual_carrier: SimpleNamespace,
    complete_original_effect: tuple[SubmitMinuteParameterStudy, JsonValue],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import hashlib
    import json

    from rquant.minute_backtest_parameter_study_execution import MinuteParameterStudyExecutionEffect
    from rquant.page_control import PageControlEffectStatus, PageControlStatus

    command, marker = complete_original_effect
    control = original_control(actual_carrier, tmp_path)
    original_enqueue(control, command)
    original_body = control.service.outbox.original_command_bytes(command.command_id)
    effect = strict_model_validate_json(
        MinuteParameterStudyExecutionEffect, canonical_json_bytes(marker)
    )
    submit = control.writer.submit
    calls: list[tuple[SubmitMinuteReplay, JsonValue, JsonValue]] = []

    def lose_reply(child: SubmitMinuteReplay, child_marker: JsonValue) -> JsonValue:
        receipt = submit(child, child_marker)
        calls.append((child, child_marker, receipt))
        raise OSError("actual child submitted; reply was lost")

    monkeypatch.setattr(control.writer, "submit", lose_reply)
    first = control.service.consumer.drain(limit=1)
    assert len(first) == 1 and first[0].status is PageControlStatus.PENDING
    assert len(calls) == 1 and calls[0][0] == effect.prepared[0].trial.command
    saved = control.service.outbox.effect(command.command_id)
    assert saved.status is PageControlEffectStatus.STARTED and saved.result == marker
    assert len(actual_carrier.installed.commands.spool.pending()) == 1
    monkeypatch.setattr(control.writer, "submit", submit)
    recover = control.journal.recover
    recovery_errors: list[str] = []

    def inspect_recovery(parent: SubmitMinuteParameterStudy, saved_marker: JsonValue) -> JsonValue:
        try:
            return recover(parent, saved_marker)
        except Exception as error:
            cause: BaseException | None = error
            while cause is not None:
                recovery_errors.append(f"{type(cause).__name__}: {cause}")
                cause = cause.__cause__
            raise

    monkeypatch.setattr(control.journal, "recover", inspect_recovery)
    recovered = control.service.consumer.drain(limit=1)
    assert not recovery_errors, recovery_errors
    assert len(recovered) == 1 and recovered[0].status is PageControlStatus.SUCCEEDED
    result = strict_model_validate_json(
        journal_api().MinuteParameterStudySubmissionReceipt,
        canonical_json_bytes(recovered[0].result),
    )
    assert [receipt.job_id for receipt in result.receipts] == [
        prepared.trial.job_id for prepared in effect.prepared
    ]
    assert result.receipts[0].model_dump(mode="json") == calls[0][2]
    assert len(actual_carrier.installed.commands.spool.pending()) == len(effect.prepared)
    proof = control.service.collaboration.issue_authorization(
        command.actor_id, command.model_dump(mode="json")
    )
    assert control.service.submit_authorized(command, proof) == recovered[0]
    assert len(actual_carrier.installed.commands.spool.pending()) == len(effect.prepared)
    assert control.service.outbox.original_command_bytes(command.command_id) == original_body
    marker_bytes = canonical_json_bytes(marker)
    stored_bytes = json.dumps(marker, ensure_ascii=True).encode("utf-8")
    assert len(original_body) <= 32 * 1024
    assert len(marker_bytes) <= 1024 * 1024 and len(stored_bytes) <= 1024 * 1024
    print(json.dumps({
        "proof": "actual_parent_outbox_partial_submit_and_original_uuid_recovery",
        "parent_command_id": command.command_id,
        "original_body_sha256": hashlib.sha256(original_body).hexdigest(),
        "original_body_bytes": len(original_body),
        "complete_effect_sha256": hashlib.sha256(marker_bytes).hexdigest(),
        "complete_effect_bytes": len(marker_bytes),
        "ensure_ascii_effect_bytes": len(stored_bytes),
        "actual_child_count": len(result.receipts),
        "actual_child_jobs": [str(receipt.job_id) for receipt in result.receipts],
        "original_lost_reply_request_id": str(result.receipts[0].request_id),
        "original_body_unchanged": True,
        "worker_started": False,
    }, sort_keys=True))


@pytest.mark.parametrize("changed", ["owner", "source", "seed", "requested_at"])
def test_actual_parent_uuid_never_accepts_a_changed_complete_body(
    actual_carrier: SimpleNamespace,
    complete_original_effect: tuple[SubmitMinuteParameterStudy, JsonValue],
    tmp_path: Path,
    changed: str,
) -> None:
    command, marker = complete_original_effect
    control = original_control(actual_carrier, tmp_path)
    original_persist(control, command, marker)
    body = command.model_dump(mode="python")
    if changed == "owner":
        body["actor_id"] = body["request"]["owner_id"] = "foreign"
    elif changed == "source":
        body["request"]["full_input_hash"] = "0" * 64
    elif changed == "seed":
        body["request"]["random_seed"] += 1
    else:
        body["requested_at"] = body["request"]["requested_at"] = (
            command.requested_at - timedelta(seconds=1)
        )
    other = SubmitMinuteParameterStudy.model_validate(body)
    before = control.service.outbox.original_command_bytes(command.command_id)
    with pytest.raises(PermissionError, match="original parent UUID/body"):
        control.journal.submit(other, marker)
    assert control.service.outbox.original_command_bytes(command.command_id) == before
    assert control.service.outbox.effect(command.command_id).result == marker


@pytest.mark.parametrize("method", ["freeze", "submit", "recover"])
def test_stolen_actual_study_claim_rejects_every_writer_entry(
    actual_carrier: SimpleNamespace,
    complete_original_effect: tuple[SubmitMinuteParameterStudy, JsonValue],
    tmp_path: Path,
    method: str,
) -> None:
    command, marker = complete_original_effect
    control = original_control(actual_carrier, tmp_path)
    old = original_persist(control, command, marker)
    replacement = control.service.outbox.claim_records(
        limit=1,
        owner_id="replacement-consumer",
        now=actual_carrier.installed.clock() + timedelta(seconds=31),
    )[0]
    assert replacement.claim_token != old.claim_token
    assert control.service.outbox.effect(command.command_id).claim_token == old.claim_token
    before = len(actual_carrier.installed.commands.spool.pending())
    args = (command,) if method == "freeze" else (command, marker)
    with pytest.raises(PermissionError, match="claim"):
        getattr(control.journal, method)(*args)
    assert len(actual_carrier.installed.commands.spool.pending()) == before


@pytest.mark.parametrize("method", ["freeze", "submit", "recover"])
def test_revoked_actual_current_role_rejects_every_writer_entry(
    actual_carrier: SimpleNamespace,
    complete_original_effect: tuple[SubmitMinuteParameterStudy, JsonValue],
    tmp_path: Path,
    method: str,
) -> None:
    from rquant.collaboration_roles import RoleEntry, RoleState

    command, marker = complete_original_effect
    control = original_control(actual_carrier, tmp_path)
    original_persist(control, command, marker)
    original = RoleState.model_validate_json(control.roles_path.read_bytes())
    control.roles_path.write_text(
        RoleState.create(
            revision=original.revision + 1,
            users=tuple(
                RoleEntry(username=entry.username, role="viewer")
                if entry.username == command.actor_id else entry
                for entry in original.users
            ),
        ).model_dump_json()
    )
    before = len(actual_carrier.installed.commands.spool.pending())
    args = (command,) if method == "freeze" else (command, marker)
    with pytest.raises(PermissionError):
        getattr(control.journal, method)(*args)
    assert len(actual_carrier.installed.commands.spool.pending()) == before


def test_current_service_backend_substitution_cannot_bind_a_journal(
    actual_carrier: SimpleNamespace, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    control = original_control(actual_carrier, tmp_path)
    foreign = MinuteCommandWriter(actual_carrier.installed)
    monkeypatch.setattr(control.service.consumer, "minute_backend", foreign)
    with pytest.raises(PermissionError, match="one original enforced owner"):
        control.journal.bind_owner_authority(control.service)


def test_child_submission_requires_a_durably_saved_complete_original_effect(
    actual_carrier: SimpleNamespace,
    complete_original_effect: tuple[SubmitMinuteParameterStudy, JsonValue],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command, marker = complete_original_effect
    control = original_control(actual_carrier, tmp_path)
    original_admit(control, command)
    monkeypatch.setattr(control.writer, "submit", lambda *_: pytest.fail("unsaved child submit"))
    with pytest.raises(PermissionError) as refused:
        control.journal.submit(command, marker)
    errors: list[str] = []
    cause: BaseException | None = refused.value
    while cause is not None:
        errors.append(str(cause))
        cause = cause.__cause__
    assert any("durably saved" in error for error in errors)
    assert control.service.outbox.effect(command.command_id).result is None


@pytest.mark.parametrize("fault", ["partial", "duplicate", "profile", "work"])
def test_saved_marker_rejects_missing_or_mismatched_original_children_before_any_submit(
    actual_carrier: SimpleNamespace,
    complete_original_effect: tuple[SubmitMinuteParameterStudy, JsonValue],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    import json

    command, marker = complete_original_effect
    changed = json.loads(canonical_json_bytes(marker))
    if fault == "partial":
        changed["prepared"] = changed["prepared"][:1]
    elif fault == "duplicate":
        changed["prepared"][1] = changed["prepared"][0]
    elif fault == "profile":
        changed["prepared"][1]["profile_hash"] = "f" * 64
    else:
        changed["prepared"][1]["work_units"] += 1
    control = original_control(actual_carrier, tmp_path)
    original_persist(control, command, changed)
    monkeypatch.setattr(control.writer, "submit", lambda *_: pytest.fail("invalid child submit"))
    with pytest.raises((PermissionError, ValueError)):
        control.journal.submit(command, changed)


def test_original_effect_budget_refuses_an_oversized_marker_before_submission(
    actual_carrier: SimpleNamespace,
    complete_original_effect: tuple[SubmitMinuteParameterStudy, JsonValue],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command, _ = complete_original_effect
    control = original_control(actual_carrier, tmp_path)
    original_admit(control, command)
    monkeypatch.setattr(control.writer, "submit", lambda *_: pytest.fail("oversized child submit"))
    with pytest.raises((ValueError, PermissionError)):
        control.journal.submit(command, {"oversized": "x" * (1024 * 1024)})


@pytest.mark.parametrize("fault", ["foreign_owner", "wrong_source", "future_source"])
def test_actual_catalog_refuses_an_unowned_or_unavailable_parent_source(
    actual_carrier: SimpleNamespace, tmp_path: Path, fault: str
) -> None:
    updates: dict[str, object] = {}
    if fault == "foreign_owner":
        updates["owner_id"] = "foreign"
    elif fault == "wrong_source":
        updates["full_input_hash"] = "0" * 64
    else:
        updates["requested_at"] = (
            actual_carrier.baselines["n_shape"].receipt.frozen.provenance.published_at
            - timedelta(seconds=1)
        )
    control = original_control(actual_carrier, tmp_path)
    command = original_parent(actual_carrier, **updates)
    original_admit(control, command)
    before = len(actual_carrier.installed.commands.spool.pending())
    with pytest.raises((PermissionError, ValueError)):
        control.journal.freeze(command)
    assert len(actual_carrier.installed.commands.spool.pending()) == before


def test_original_consumer_never_submits_if_complete_effect_persistence_fails(
    actual_carrier: SimpleNamespace, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.minute_backtest_parameter_study_execution import MinuteParameterStudyExecutionEffect
    from rquant.page_control import PageControlEffectStatus, PageControlStatus

    control = original_control(actual_carrier, tmp_path)
    command = original_parent(actual_carrier)
    original_enqueue(control, command)
    captured: list[JsonValue] = []

    def fail_persist(command_id: str, *, result: JsonValue, **_: object) -> None:
        assert command_id == command.command_id
        captured.append(result)
        raise OSError("original complete effect write failed before any child submit")

    monkeypatch.setattr(control.service.outbox, "record_started_effect_result", fail_persist)
    monkeypatch.setattr(control.writer, "submit", lambda *_: pytest.fail("failed admission submit"))
    before = len(actual_carrier.installed.commands.spool.pending())
    returned = control.service.consumer.drain(limit=1)
    assert len(returned) == 1 and returned[0].status is PageControlStatus.FAILED
    assert len(captured) == 1
    complete = strict_model_validate_json(
        MinuteParameterStudyExecutionEffect, canonical_json_bytes(captured[0])
    )
    assert complete.command == command and len(complete.prepared) == complete.plan.trial_count == 1
    effect = control.service.outbox.effect(command.command_id)
    assert effect.status is PageControlEffectStatus.FAILED and effect.result is None
    assert len(actual_carrier.installed.commands.spool.pending()) == before


def test_original_consumer_unknown_recovery_keeps_started_effect_and_does_not_blind_submit(
    actual_carrier: SimpleNamespace,
    complete_original_effect: tuple[SubmitMinuteParameterStudy, JsonValue],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.page_control import PageControlEffectStatus, PageControlStatus

    command, marker = complete_original_effect
    control = original_control(actual_carrier, tmp_path)
    claim = original_persist(control, command, marker)
    control.service.outbox.release_claim_for_retry(
        command.command_id, owner_id=claim.owner_id, claim_token=claim.claim_token
    )
    monkeypatch.setattr(control.writer, "recover", lambda *_: None)
    monkeypatch.setattr(control.writer, "submit", lambda *_: pytest.fail("blind recovery submit"))
    before = len(actual_carrier.installed.commands.spool.pending())
    returned = control.service.consumer.drain(limit=1)
    assert len(returned) == 1 and returned[0].status is PageControlStatus.PENDING
    effect = control.service.outbox.effect(command.command_id)
    assert effect.status is PageControlEffectStatus.STARTED and effect.result == marker
    assert len(actual_carrier.installed.commands.spool.pending()) == before


def test_actual_insufficient_fold_plan_has_no_invented_child_job(
    actual_carrier: SimpleNamespace, tmp_path: Path
) -> None:
    from rquant.page_control import PageControlStatus

    control = original_control(actual_carrier, tmp_path)
    command = original_parent(
        actual_carrier,
        mode="walk_forward",
        walk_forward={"fold_count": 6, "min_training_dates": 2, "validation_date_count": 1},
    )
    original_enqueue(control, command)
    before = len(actual_carrier.installed.commands.spool.pending())
    returned = control.service.consumer.drain(limit=1)
    assert len(returned) == 1 and returned[0].status is PageControlStatus.SUCCEEDED
    result = strict_model_validate_json(
        journal_api().MinuteParameterStudySubmissionReceipt,
        canonical_json_bytes(returned[0].result),
    )
    assert result.state == "unavailable"
    assert result.receipts == () and result.unavailable_reasons == ("insufficient_fold_dates",)
    assert len(actual_carrier.installed.commands.spool.pending()) == before


def test_physical_metadata_change_is_not_trusted_from_a_saved_private_marker(
    actual_carrier: SimpleNamespace,
    complete_original_effect: tuple[SubmitMinuteParameterStudy, JsonValue],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command, marker = complete_original_effect
    control = original_control(actual_carrier, tmp_path)
    original_persist(control, command, marker)
    path = actual_carrier.catalog.fact_sources[0].metadata_identity.source_path
    original_stat = path.stat()
    original = path.read_bytes()
    before = len(actual_carrier.installed.commands.spool.pending())
    monkeypatch.setattr(control.writer, "submit", lambda *_: pytest.fail("changed metadata submit"))
    try:
        path.write_bytes(original + b"explicit local integrity fault")
        with pytest.raises((ValueError, OSError, PermissionError)):
            control.journal.submit(command, marker)
        assert len(actual_carrier.installed.commands.spool.pending()) == before
    finally:
        path.write_bytes(original)
        os.utime(path, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))
