from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from rquant.experiment_registry import DateRange
from rquant.minute_backtest_commands import MinuteRunConfig, SubmitMinuteReplay, minute_job_id
from rquant.minute_backtest_formal import MinuteExperimentProtocol
from tests.unit.test_minute_backtest_producer import NOW


def configuration() -> MinuteRunConfig:
    return MinuteRunConfig(source_key="source", source_version=1, full_input_hash="a" * 64,
        native_id="n_shape", native_version=1,
        protocol=MinuteExperimentProtocol(
            train_range=DateRange(start_date=NOW.date() - timedelta(days=3), end_date=NOW.date() - timedelta(days=3)),
            validation_range=DateRange(start_date=NOW.date() - timedelta(days=2), end_date=NOW.date() - timedelta(days=2)),
            frozen_outer_test_range=DateRange(start_date=NOW.date() - timedelta(days=1), end_date=NOW.date())),
        deadline=NOW + timedelta(hours=1))


@pytest.mark.parametrize("field", ["hold_days", "freq", "pool", "optimizer", "ablation", "actor_id", "source_path", "receipt_path"])
def test_original_unsupported_parameters_and_paths_are_not_silently_dropped(field: str) -> None:
    with pytest.raises(ValueError):
        MinuteRunConfig.model_validate(configuration().model_dump(mode="python") | {field: 1})


def test_command_deterministic_job_identity_binds_authenticated_actor() -> None:
    command_id = uuid4()
    command = SubmitMinuteReplay(command_id=str(command_id), requested_at=NOW, actor_id="researcher", config=configuration())
    assert minute_job_id(command.actor_id, command.command_id) == minute_job_id("researcher", command_id)
    assert minute_job_id("other", command_id) != minute_job_id(command.actor_id, command.command_id)
    with pytest.raises(ValueError):
        MinuteRunConfig.model_validate(configuration().model_dump(mode="python") | {"native_version": True})


def test_original_authorization_admits_only_minute_research_operation(tmp_path: Path) -> None:
    from rquant.page_control import parse_page_control_command
    from tests.unit.test_web_collaboration import installed

    authority, _ = installed(tmp_path)
    prefix = "/api/v1/backtests/minute-runtime"
    for suffix in ("capabilities", "sources", "runs", "runs/{job_id}", "runs/{job_id}/nav", "runs/{job_id}/rows"):
        for actor in ("admin", "alice", "bob"):
            authority.require_operation(actor, "GET", prefix + "/" + suffix)
    authority.require_operation("alice", "POST", prefix + "/runs")
    authority.require_command("alice", "submit_minute_replay")
    for method, path in (("POST", prefix + "/runs"), ("POST", prefix + "/future"), ("DELETE", prefix + "/runs")):
        with pytest.raises(PermissionError):
            authority.require_operation("bob", method, path)
    with pytest.raises(PermissionError):
        authority.require_operation("admin", "POST", prefix + "/future")
    body = SubmitMinuteReplay(command_id=str(uuid4()), requested_at=NOW, actor_id="alice", config=configuration()).model_dump(mode="json")
    checked = parse_page_control_command(body)
    assert isinstance(checked, SubmitMinuteReplay)
    proof = authority.issue_authorization("alice", body)
    assert authority.verify_authorization(proof, checked.model_dump(mode="json")).actor_id == "alice"
    with pytest.raises(PermissionError):
        authority.verify_authorization(proof, body | {"actor_id": "admin"})


@pytest.mark.parametrize("status", ["submitted", "conflict"])
def test_original_minute_public_receipt_decodes_the_original_discriminated_union(status: str, tmp_path: Path) -> None:
    from uuid import NAMESPACE_URL, uuid5
    from rquant.lab_job_center import CommandSubmissionConflict, CommandSubmissionReceipt, SubmissionSpoolIdentity
    from rquant.minute_backtest_commands import minute_interaction
    from rquant.web.lab_control_gateway import LabControlInvalidReceiptError, LabControlWireReceipt
    from rquant.web.minute_backtest_routes import _public

    command = SubmitMinuteReplay(command_id=str(uuid4()), requested_at=NOW, actor_id="researcher", config=configuration())
    job = minute_job_id(command.actor_id, command.command_id)
    request = uuid5(NAMESPACE_URL, "rquant.lab-job-center.interaction:" + minute_interaction(command))
    # Public projection fixture only; real spool/seal acceptance is covered by the installed integration.
    result = CommandSubmissionReceipt(request_id=request, command_type="submit", job_id=job,
        spool=SubmissionSpoolIdentity(path=tmp_path / "unused-original-command.json", state="pending", device=0, inode=1,
            content_hash="a" * 64)) if status == "submitted" else CommandSubmissionConflict(
                request_id=request, job_id=job, reason="interaction_content_conflict")
    wire = LabControlWireReceipt(command_id=command.command_id, status="succeeded", enqueued_at=NOW,
        completed_at=NOW, result=result.model_dump(mode="json"))
    receipt = _public(command, wire)
    assert receipt.status == status
    assert receipt.job_id == (job if status == "submitted" else None)
    with pytest.raises(LabControlInvalidReceiptError):
        _public(command, wire.model_copy(update={"result": result.model_dump(mode="json") | {"job_id": str(uuid4())}}))


def test_minute_export_command_and_public_receipt_bind_exact_original_request(tmp_path: Path) -> None:
    from rquant.minute_backtest_commands import ExportMinuteReplayZip, minute_zip_request_id
    from rquant.minute_backtest_export import MinuteZipReceipt
    from rquant.page_control import parse_page_control_command
    from rquant.web.lab_control_gateway import LabControlInvalidReceiptError, LabControlWireReceipt
    from rquant.web.minute_backtest_routes import _public
    from tests.unit.test_web_collaboration import installed

    authority, _ = installed(tmp_path)
    command = ExportMinuteReplayZip(command_id=str(uuid4()), requested_at=NOW,
        actor_id="alice", job_id=uuid4(), result_hash="b" * 64)
    assert parse_page_control_command(command.model_dump(mode="json")) == command
    authority.require_command("alice", command.kind)
    authority.require_operation("alice", "POST", "/api/v1/backtests/minute-runtime/exports")
    for suffix in ("report.html", "exports/{request_id}.zip"):
        authority.require_operation("bob", "GET", "/api/v1/backtests/minute-runtime/runs/{job_id}/" + suffix)
    with pytest.raises(PermissionError):
        authority.require_command("bob", command.kind)
    assert minute_zip_request_id(command) != minute_zip_request_id(command.model_copy(update={"actor_id": "other"}))
    result = MinuteZipReceipt(request_id=minute_zip_request_id(command), job_id=command.job_id,
        path=tmp_path / "unused-result.zip", byte_size=10, sha256="c" * 64, result_hash=command.result_hash,
        full_input_hash="d" * 64, core_input_hash="e" * 64, seed_hash="f" * 64,
        html_sha256="1" * 64, owner_binding_hash="2" * 64)
    wire = LabControlWireReceipt(command_id=command.command_id, status="succeeded", enqueued_at=NOW,
        completed_at=NOW, result=result.model_dump(mode="json"))
    receipt = _public(command, wire)
    assert receipt.status == "exported" and receipt.zip_request_id == result.request_id
    assert receipt.job_id == result.job_id and receipt.result_hash == command.result_hash
    assert receipt.sha256 == result.sha256 and receipt.byte_size == result.byte_size
    assert "path" not in receipt.model_dump()
    with pytest.raises(LabControlInvalidReceiptError):
        _public(command, wire.model_copy(update={"result": result.model_dump(mode="json") | {"request_id": str(uuid4())}}))
