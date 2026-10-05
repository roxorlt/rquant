from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import NoReturn
from uuid import UUID

import pytest

from rquant.experiment_platform_projection import legacy_job_page, legacy_job_snapshot
from rquant.lab_job_center import (
    CommandSubmissionConflict,
    CommandSubmissionReceipt,
    LabCommandSubmissionFacade,
)
from rquant.lab_job_protocol import (
    CancelJobCommand,
    LabCommandEnvelope,
    LabCommandSpool,
    SubmitJobCommand,
)
from rquant.lab_jobs import FormalSubmissionAuthorityError, LabJobReader, LabJobStore
from rquant.lab_jobs_serving_authority import LabJobsServingSourceReader
from rquant.portfolio_backtest_artifact import PortfolioResultReader
from rquant.research_run_spec import ResearchRunSpec
from rquant.web.portfolio_backtest_service import PortfolioWebService
from rquant.web.settings import WebSettings
from tests.support.web_proxy_identity import ProofTestClient, create_private_test_app
from tests.unit.test_experiment_platform import NOW, prepared_family
from tests.unit.test_lab_jobs import _spec


def test_exp01_private_original_jobs_never_exit_old_portfolio_tasks_events_or_binary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, family, children, definitions = prepared_family(tmp_path)
    store.register_family_submission(
        owner=family.owner, request_id=family.request_id, children=children
    )
    jobs = LabJobStore(tmp_path / "jobs.sqlite3")
    jobs.initialize()
    reader = LabJobReader(jobs.path)
    spool = LabCommandSpool(tmp_path / "commands")
    facade = LabCommandSubmissionFacade(
        reader=reader,
        spool=spool,
        experiment_registry=store.registry,
        definition_registry=definitions,
        clock=lambda: NOW,
    )
    facade.recover_pending_experiment_submissions()
    private_ids = {child.intent.job_id for child in children}

    # The existing exploratory DTO is metadata only; no worker or market claim.
    spec = _spec()
    regular = ResearchRunSpec.model_validate(
        {
            **spec.model_dump(mode="python"),
            "parameters": spec.parameters.model_copy(
                update={"strategy_name": "portfolio_backtest"}
            ),
            "deadline": NOW + timedelta(hours=1),
        }
    )
    ordinary_id = UUID(int=969)
    facade.submit_create(
        SubmitJobCommand(job_id=ordinary_id, spec=regular, max_attempts=2),
        interaction_key="ordinary-c6-exit-proof",
    )

    def authority(envelope: LabCommandEnvelope, observed_at: datetime) -> None:
        facade.validate_prepared_experiment_submission(envelope, observed_at=observed_at)

    lease = jobs.acquire_scheduler_lease(owner_id="synthetic-exit-proof", lease_seconds=60, now=NOW)
    try:
        for pending in spool.pending():
            jobs.apply_command(
                pending.envelope, lease=lease, now=NOW, submission_authority=authority
            )
    finally:
        jobs.release_scheduler_lease(lease, now=NOW)
    assert {item.job_id for item in reader.list_jobs().items} == private_ids | {ordinary_id}
    page = legacy_job_page(reader, limit=1)
    assert tuple(item.job_id for item in page.items) == (ordinary_id,)
    assert page.next_cursor is None
    snapshot = legacy_job_snapshot(reader, limit=1)
    assert snapshot.page.total_count == 1
    assert tuple(window.job_id for window in snapshot.windows) == (ordinary_id,)
    published = LabJobsServingSourceReader(reader=reader, max_jobs=1)(NOW).payload
    public_text = published.model_dump_json()
    assert all(str(job_id) not in public_text for job_id in private_ids)
    assert str(ordinary_id) in public_text

    # This is the same public facade used by the old authenticated task route.
    # A known private id must not authorize Bob through the ordinary control path.
    private_id = children[0].intent.job_id
    job = reader.get_job(private_id)
    pending_count = len(spool.pending())
    result = facade.submit_cancel(
        private_id,
        expected_version=job.version,
        reason="synthetic old-route cancellation",
        interaction_key=f"web.lab-control:bob:{private_id}:cancel:{job.version}",
    )
    print(
        "PRIVATE_OLD_CONTROL_FACT",
        {
            "result": result.model_dump(mode="json"),
            "spool_before": pending_count,
            "spool_after": len(spool.pending()),
        },
    )
    assert isinstance(result, CommandSubmissionConflict) and result.reason == "job_not_found"
    assert len(spool.pending()) == pending_count
    for action in ("pause", "resume", "retry", "cancel"):
        submit = getattr(facade, "submit_" + action)
        denied = submit(
            private_id,
            expected_version=job.version + 1,
            reason="synthetic default old control",
            interaction_key=f"web.lab-control:alice:{private_id}:{action}:stale",
        )
        assert isinstance(denied, CommandSubmissionConflict) and denied.reason == "job_not_found"
    rerun = facade.submit_rerun(
        private_id,
        new_job_id=UUID(int=971),
        max_attempts=2,
        interaction_key="ordinary-private-rerun-proof",
    )
    assert isinstance(rerun, CommandSubmissionConflict) and rerun.reason == "job_not_found"
    assert len(spool.pending()) == pending_count
    admission = store.child(private_id)
    # Caller-supplied typed owner or copied facts do not grant a private control.
    for copied in (admission, admission.model_copy(update={"owner": "bob"})):
        with pytest.raises(FormalSubmissionAuthorityError, match="persisted admission"):
            facade._submit_control(
                CancelJobCommand(
                    job_id=private_id,
                    expected_version=job.version,
                    reason="experiment family cancellation",
                ),
                interaction_key="copied-private-cancel-proof",
                private_cancellation=copied,
            )
    assert len(spool.pending()) == pending_count
    ordinary_cancel = facade.submit_cancel(
        ordinary_id,
        expected_version=reader.get_job(ordinary_id).version,
        reason="synthetic ordinary cancellation",
        interaction_key="ordinary-control-unchanged-proof",
    )
    assert isinstance(ordinary_cancel, CommandSubmissionReceipt)
    assert len(spool.pending()) == pending_count + 1

    results = PortfolioResultReader(reader=reader, artifact_root=tmp_path / "artifacts")
    payload_reads = 0

    def forbidden(*args: object, **kwargs: object) -> NoReturn:
        nonlocal payload_reads
        payload_reads += 1
        raise AssertionError("old exit reached private artifact bytes")

    monkeypatch.setattr(results.previews, "preview", forbidden)
    monkeypatch.setattr(results.views, "preview", forbidden)
    service = PortfolioWebService(reader=reader, results=results)
    app = create_private_test_app(
        WebSettings(serving_root=tmp_path / "serving"),
        clock=lambda: NOW,
        background=False,
        portfolio_backtests=service,
    )
    base = "/api/v1/backtests/portfolio/runs"
    with ProofTestClient(app) as client:
        for owner in ("alice", "bob"):
            headers = {"x-rquant-user": owner}
            listing = client.get(base, headers=headers)
            assert listing.status_code == 200
            assert [item["job_id"] for item in listing.json()["data"]["jobs"]] == [str(ordinary_id)]
            assert client.get(f"{base}/{ordinary_id}", headers=headers).status_code == 200
            for job_id in sorted(private_ids, key=str):
                private = f"{base}/{job_id}"
                for path, params in (
                    (private, {}),
                    (private + "/nav", {"result_hash": "a" * 64}),
                    (private + "/rows", {"result_hash": "a" * 64, "view": "daily"}),
                    (private + "/report.html", {"result_hash": "a" * 64}),
                    (private + f"/exports/{UUID(int=970)}.zip", {"result_hash": "a" * 64}),
                ):
                    response = client.get(path, params=params, headers=headers)
                    assert response.status_code == 404, (path, response.text)
                    assert str(job_id) not in response.text
    assert payload_reads == 0
