from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from uuid import UUID, uuid4

from rquant.lab_job_center import (
    CommandSubmissionConflict,
    CommandSubmissionReceipt,
    CommandSubmissionStale,
    CommandSubmissionUnavailable,
    LabCommandSubmissionFacade,
)
from rquant.lab_job_protocol import (
    LabCommandEnvelope,
    LabCommandReceipt,
    LabCommandSpool,
    PauseJobCommand,
    SubmitJobCommand,
)
from rquant.lab_jobs import JobStatus, LabJobReader, LabJobStore

from .test_lab_jobs import NOW, _lease, _spec, _submit_job, _transition_to


def _empty_store(tmp_path: Path) -> LabJobStore:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    return store


def test_create_submission_is_exactly_once_across_rerun_restart_and_ack(
    tmp_path: Path,
) -> None:
    store = _empty_store(tmp_path)
    spool_root = tmp_path / "commands"
    command = SubmitJobCommand(job_id=UUID(int=77), spec=_spec(), max_attempts=2)
    first_facade = LabCommandSubmissionFacade(
        reader=LabJobReader(store.path),
        spool=LabCommandSpool(spool_root),
    )

    first = first_facade.submit_create(command, interaction_key="create-form-77")
    rerun = first_facade.submit_create(command, interaction_key="create-form-77")
    restarted = LabCommandSubmissionFacade(
        reader=LabJobReader(store.path),
        spool=LabCommandSpool(spool_root),
    ).submit_create(command, interaction_key="create-form-77")

    assert isinstance(first, CommandSubmissionReceipt)
    assert first == rerun == restarted
    assert len(LabCommandSpool(spool_root).pending()) == 1

    entry = LabCommandSpool(spool_root).pending()[0]
    LabCommandSpool(spool_root).ack(
        entry,
        LabCommandReceipt(
            request_id=first.request_id,
            content_hash=entry.envelope.content_hash,
            job_id=command.job_id,
            status="applied",
            reason="submitted",
            job_version=0,
        ),
    )
    after_ack = LabCommandSubmissionFacade(
        reader=LabJobReader(store.path),
        spool=LabCommandSpool(spool_root),
    ).submit_create(command, interaction_key="create-form-77")

    assert isinstance(after_ack, CommandSubmissionReceipt)
    assert after_ack.request_id == first.request_id
    assert after_ack.spool.state == "acknowledged"


def test_same_interaction_key_with_different_content_fails_closed(tmp_path: Path) -> None:
    store = _empty_store(tmp_path)
    facade = LabCommandSubmissionFacade(
        reader=LabJobReader(store.path),
        spool=LabCommandSpool(tmp_path / "commands"),
    )
    first = facade.submit_create(
        SubmitJobCommand(job_id=UUID(int=1), spec=_spec()),
        interaction_key="same-click",
    )
    conflict = facade.submit_create(
        SubmitJobCommand(job_id=UUID(int=2), spec=_spec()),
        interaction_key="same-click",
    )

    assert isinstance(first, CommandSubmissionReceipt)
    assert isinstance(conflict, CommandSubmissionConflict)
    assert conflict.request_id == first.request_id
    assert len(facade.spool.pending()) == 1


def test_scheduler_stale_ack_is_typed_after_concurrent_version_change_and_restart(
    tmp_path: Path,
) -> None:
    store = _empty_store(tmp_path)
    lease = _lease(store)
    job = _submit_job(store, lease)
    spool_root = tmp_path / "commands"
    facade = LabCommandSubmissionFacade(
        reader=LabJobReader(store.path),
        spool=LabCommandSpool(spool_root),
    )
    submitted = facade.submit_cancel(
        job.job_id,
        expected_version=job.version,
        reason="cancel from stale page",
        interaction_key="concurrent-cancel",
    )
    assert isinstance(submitted, CommandSubmissionReceipt)

    current = store.transition_job(
        job.job_id,
        expected_version=job.version,
        target_status=JobStatus.RUNNING,
        lease=lease,
        reason="scheduler advanced first",
        now=NOW + timedelta(seconds=1),
    )
    entry = LabCommandSpool(spool_root).pending()[0]
    LabCommandSpool(spool_root).ack(
        entry,
        LabCommandReceipt(
            request_id=submitted.request_id,
            content_hash=entry.envelope.content_hash,
            job_id=job.job_id,
            status="rejected",
            reason=f"stale_version:{current.version}",
            job_version=current.version,
        ),
    )

    replay = LabCommandSubmissionFacade(
        reader=LabJobReader(store.path),
        spool=LabCommandSpool(spool_root),
    ).submit_cancel(
        job.job_id,
        expected_version=job.version,
        reason="cancel from stale page",
        interaction_key="concurrent-cancel",
    )

    assert isinstance(replay, CommandSubmissionStale)
    assert replay.authoritative_version == current.version
    assert replay.authoritative_status is JobStatus.RUNNING


def test_control_submission_uses_authoritative_version_and_never_writes_sqlite(
    tmp_path: Path,
) -> None:
    store = _empty_store(tmp_path)
    lease = _lease(store)
    job = _submit_job(store, lease)
    before_bytes = store.path.read_bytes()
    facade = LabCommandSubmissionFacade(
        reader=LabJobReader(store.path),
        spool=LabCommandSpool(tmp_path / "commands"),
    )

    stale = facade.submit_cancel(
        job.job_id,
        expected_version=job.version + 1,
        reason="stale tab",
        interaction_key="cancel-stale",
    )
    submitted = facade.submit_cancel(
        job.job_id,
        expected_version=job.version,
        reason="cancel queued job",
        interaction_key="cancel-current",
    )

    assert isinstance(stale, CommandSubmissionStale)
    assert stale.authoritative_version == job.version
    assert isinstance(submitted, CommandSubmissionReceipt)
    assert submitted.expected_version == job.version
    assert store.path.read_bytes() == before_bytes
    current = LabJobReader(store.path).get_job(job.job_id)
    assert current is not None and current.status is JobStatus.QUEUED
    assert len(facade.spool.pending()) == 1


def test_pause_resume_withdraw_cancel_and_retry_share_reader_availability(
    tmp_path: Path,
) -> None:
    running_store = _empty_store(tmp_path / "running")
    running_lease = _lease(running_store)
    running = _submit_job(running_store, running_lease)
    running = running_store.transition_job(
        running.job_id,
        expected_version=running.version,
        target_status=JobStatus.RUNNING,
        lease=running_lease,
        reason="start",
        now=NOW + timedelta(seconds=1),
    )
    running_facade = LabCommandSubmissionFacade(
        reader=LabJobReader(running_store.path),
        spool=LabCommandSpool(tmp_path / "running-commands"),
    )
    pause = running_facade.submit_pause(
        running.job_id,
        expected_version=running.version,
        reason="pause",
        interaction_key="pause",
    )
    unavailable_resume = running_facade.submit_resume(
        running.job_id,
        expected_version=running.version,
        reason="not paused",
        interaction_key="resume-unavailable",
    )

    checkpointed_store = _empty_store(tmp_path / "checkpointed")
    checkpointed_lease = _lease(checkpointed_store)
    checkpointed = _transition_to(checkpointed_store, checkpointed_lease, JobStatus.CHECKPOINTED)
    resume = LabCommandSubmissionFacade(
        reader=LabJobReader(checkpointed_store.path),
        spool=LabCommandSpool(tmp_path / "checkpointed-commands"),
    ).submit_resume(
        checkpointed.job_id,
        expected_version=checkpointed.version,
        reason="resume",
        interaction_key="resume",
    )

    assert isinstance(pause, CommandSubmissionReceipt)
    assert pause.command_type == "pause"
    assert isinstance(unavailable_resume, CommandSubmissionUnavailable)
    assert isinstance(resume, CommandSubmissionReceipt)
    assert resume.command_type == "resume"

    pause_receipt = running_store.apply_command(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=PauseJobCommand(
                job_id=running.job_id,
                expected_version=running.version,
                reason="pause directly",
            ),
        ),
        lease=running_lease,
        now=NOW + timedelta(seconds=2),
    )
    assert pause_receipt.status == "applied" and pause_receipt.job_version is not None
    withdraw = running_facade.submit_resume(
        running.job_id,
        expected_version=pause_receipt.job_version,
        reason="withdraw pause",
        interaction_key="withdraw-pause",
    )
    assert isinstance(withdraw, CommandSubmissionReceipt)
    assert withdraw.command_type == "resume"

    failed_store = _empty_store(tmp_path / "failed")
    failed_lease = _lease(failed_store)
    failed = _transition_to(failed_store, failed_lease, JobStatus.FAILED)
    retry = LabCommandSubmissionFacade(
        reader=LabJobReader(failed_store.path),
        spool=LabCommandSpool(tmp_path / "failed-commands"),
    ).submit_retry(
        failed.job_id,
        expected_version=failed.version,
        reason="retry recoverable failure",
        interaction_key="retry",
    )

    assert isinstance(retry, CommandSubmissionReceipt)
    assert retry.command_type == "retry"
    assert retry.expected_version == failed.version


def test_missing_job_returns_typed_conflict_without_spooling(tmp_path: Path) -> None:
    store = _empty_store(tmp_path)
    facade = LabCommandSubmissionFacade(
        reader=LabJobReader(store.path),
        spool=LabCommandSpool(tmp_path / "commands"),
    )

    result = facade.submit_retry(
        uuid4(),
        expected_version=0,
        reason="retry missing",
        interaction_key="missing",
    )

    assert isinstance(result, CommandSubmissionConflict)
    assert result.reason == "job_not_found"
    assert facade.spool.pending() == ()
