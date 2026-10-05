from __future__ import annotations

from datetime import datetime
from pathlib import Path
from uuid import UUID

import pytest

from rquant.lab_job_center import (
    CommandSubmissionConflict,
    ExperimentLifecycleCoordinator,
    LabCommandSubmissionFacade,
)
from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool
from rquant.lab_jobs import LabJobReader, LabJobStore
from tests.unit.test_experiment_platform import NOW, prepared_family


def test_exp08_c_lost_publish_receipt_then_cancel_replays_exact_original_child(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, record, children, definitions = prepared_family(tmp_path)
    store.register_family_submission(
        owner=record.owner, request_id=record.request_id, children=children
    )
    jobs = LabJobStore(tmp_path / "jobs.sqlite3")
    jobs.initialize()
    reader = LabJobReader(jobs.path)
    spool = LabCommandSpool(tmp_path / "spool")
    facade = LabCommandSubmissionFacade(
        reader=reader,
        spool=spool,
        experiment_registry=store.registry,
        definition_registry=definitions,
        clock=lambda: NOW,
    )
    calls = []
    publish = facade._publish
    mark = facade._mark_experiment_submission_published

    def counted(envelope: LabCommandEnvelope) -> None:
        calls.append(envelope)
        return publish(envelope)

    def interrupted(envelope: LabCommandEnvelope) -> None:
        raise RuntimeError("spool accepted before original receipt update")

    monkeypatch.setattr(facade, "_publish", counted)
    monkeypatch.setattr(facade, "_mark_experiment_submission_published", interrupted)
    with pytest.raises(RuntimeError, match="spool accepted"):
        facade.recover_pending_experiment_submissions()
    assert len(calls) == len(spool.pending()) == 1
    envelope = calls[0]
    intent = store.registry.get_submission_intent_for_job(envelope.command.job_id)
    grant = store.child(intent.job_id)
    assert grant.publish_grant_seq is not None and reader.get_job(intent.job_id) is None
    store.cancel_family(
        owner=record.owner, family_id=record.family_id, request_id=UUID(int=901), now=NOW
    )
    facade.recover_private_experiment_cancellations(observed_at=NOW)
    pending = store.child(intent.job_id)
    assert pending.cancel_state == "pending" and pending.publish_grant_seq < pending.cancel_seq
    assert store.registry.get_attempt(intent.experiment_id).status.value == "registered"
    assert store.registry.list_pending_submissions() == (intent,)
    monkeypatch.setattr(facade, "_mark_experiment_submission_published", mark)
    recovered = facade.recover_pending_experiment_submissions()
    assert len(recovered) == 1 and calls == [envelope, envelope] and len(spool.pending()) == 1
    lease = jobs.acquire_scheduler_lease(owner_id="synthetic-scheduler", lease_seconds=60, now=NOW)
    lifecycle = ExperimentLifecycleCoordinator(facade)

    def authority(value: LabCommandEnvelope, at: datetime) -> None:
        lifecycle.validate_submission(value, observed_at=at)

    first = jobs.apply_command(envelope, lease=lease, now=NOW, submission_authority=authority)
    assert (
        jobs.apply_command(envelope, lease=lease, now=NOW, submission_authority=authority) == first
    )
    assert reader.list_jobs().total_count in (None, 1) and len(reader.list_jobs().items) == 1
    assert reader.get_job(intent.job_id).status.value == "queued"
    facade.recover_private_experiment_cancellations(observed_at=NOW)
    current = store.child(intent.job_id)
    assert current.cancel_state == "pending" and len(current.cancel_request_chain) == 1
    request_id = current.cancel_request_chain[0]
    original_cancel = next(
        e.envelope for e in spool.pending() if e.envelope.request_id == request_id
    )
    pending_count = len(spool.pending())
    denied = facade.submit_cancel(
        intent.job_id,
        expected_version=current.cancel_job_version,
        reason="experiment family cancellation",
        interaction_key=f"experiment.cancel:{intent.job_id}:{current.cancel_job_version}:0",
    )
    assert isinstance(denied, CommandSubmissionConflict) and denied.reason == "job_not_found"
    assert len(spool.pending()) == pending_count
    facade.recover_private_experiment_cancellations(observed_at=NOW)
    assert store.child(intent.job_id).cancel_request_chain == (request_id,)
    receipt = jobs.apply_command(original_cancel, lease=lease, now=NOW)
    assert jobs.apply_command(original_cancel, lease=lease, now=NOW) == receipt
    assert reader.get_job(intent.job_id).status.value == "cancelled"
    lifecycle.recover(observed_at=NOW)
    assert store.child(intent.job_id).cancel_state == "confirmed"
    assert len(store.registry.list_family_attempts(record.family_id)) == 4
    assert all(
        a.status.value == "cancelled" for a in store.registry.list_family_attempts(record.family_id)
    )
    assert store.registry.get_submission_intent_for_job(intent.job_id) == intent
    with store.registry._connect() as connection:
        states = connection.execute(
            "SELECT state FROM experiment_submission_outbox WHERE job_id=?", (str(intent.job_id),)
        ).fetchall()
        assert len(states) == 1 and states[0][0] == "published"
    jobs.release_scheduler_lease(lease, now=NOW)
