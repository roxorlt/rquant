"""Original plan, worker seal and finalizer close the paper result graph."""

from datetime import timedelta
from time import monotonic
from uuid import UUID, uuid4

import pytest

from rquant.lab_artifact_preview import ArtifactPreviewReader
from rquant.lab_artifact_protocol import LabArtifactCommitSpool, LabFinalizerAuthorityKey
from rquant.lab_artifacts import LabJobArtifactStore
from rquant.lab_finalizer import LabFinalizer
from rquant.lab_jobs import LabResultState, JobStatus
from rquant.lab_scheduler import LabScheduler
from rquant.lab_shard_protocol import LabClaimSpool, LabReportSpool, LabShardSucceeded, LabShardTelemetry, LabWorkerReport
from rquant.lab_worker import LabWorker, build_builtin_shard_runtime_manifest
from rquant.lab_worker_registry import execute_builtin_lab_shard
from rquant.paper_research_runtime import PaperResearchRuntimeDirectory
from rquant.paper_research_artifact import PaperResearchResultReader
from rquant.page_control import PageControlStatus
from tests.unit.test_paper_research_submission import fixture
from tests.unit.test_paper_signal_worker import EXECUTION_TIME


def host(tmp_path, test_request: pytest.FixtureRequest | None = None):
    page, backend, runtime, request, jobs = fixture(tmp_path)
    directory = PaperResearchRuntimeDirectory(states=(runtime.state,), expected_identities=(runtime.state.identity(),))
    runs = backend.research_backend
    claims, reports = LabClaimSpool(tmp_path/"claims"), LabReportSpool(tmp_path/"reports")
    commits, artifacts = LabArtifactCommitSpool(tmp_path/"commits"), LabJobArtifactStore(tmp_path/"results")
    key = LabFinalizerAuthorityKey(key_id="synthetic-paper-research-finalizer", secret=b"synthetic-paper-finalizer-key-000000000")
    scheduler = LabScheduler(store=jobs, spool=runs.facade.spool, owner_id="synthetic-paper-scheduler",
                              lease_seconds=120, heartbeat_seconds=10, poll_interval_ms=5, claim_spool=claims, report_spool=reports,
                              claim_worker_ids=("synthetic-paper-worker",), artifact_commit_spool=commits, artifact_store=artifacts,
                              finalizer_authority_key_provider=lambda requested: key if requested==key.key_id else None,
                              paper_directory=directory, runtime_guard=lambda: "a"*40, clock=lambda: EXECUTION_TIME)
    original = build_builtin_shard_runtime_manifest(catalog_path=tmp_path/"research.duckdb", forbidden_paths=(),
                                                     snapshot_root=tmp_path/"snapshots", research_lake_root=tmp_path/"lake")
    worker = LabWorker(worker_id="synthetic-paper-worker", claim_spool=claims, report_spool=reports, artifact_root=tmp_path/"shards",
                       shard_runtime_manifest=original, paper_directory=directory, verified_code_sha_provider=lambda: "a"*40,
                       heartbeat_interval_seconds=10, receipt_timeout_seconds=15, clock=lambda: EXECUTION_TIME)
    finalizer = LabFinalizer(reader=runs.facade.reader, shard_artifact_root=tmp_path/"shards", artifact_store=artifacts,
                              commit_spool=commits, paper_directory=directory, verified_code_sha_provider=lambda: "a"*40,
                              finalizer_authority_key_provider=lambda: key)
    result_reader = PaperResearchResultReader(backend=runs, reader=runs.facade.reader,
                                               artifact_reader=ArtifactPreviewReader(reader=runs.facade.reader, artifact_root=artifacts.root))
    source = runs.preparer.source_for(request.account_id, "alice")
    if test_request is not None:
        # Readonly peers need the original owner's WAL until pytest teardown.
        owner_connection = source.broker._connect()
        test_request.addfinalizer(owner_connection.close)
    source.research_results = result_reader
    return page, backend, runtime, request, scheduler, worker, finalizer, result_reader, source


def accept_and_change_head(page, backend, runtime, request, scheduler):
    receipt = page._submit_trusted_paper_portfolio(request, authenticated_actor_id="alice", verified_metadata_identity=runtime.state.identity())
    assert receipt.status is PageControlStatus.SUCCEEDED
    job_id = UUID(request.command_id)
    assert scheduler.run_once().plans_created == 1
    owned = backend.research_backend.lookup(request, owner_id="alice", expected_identity=runtime.state.identity())
    runtime.state.start_configuration(runtime.state.configuration.model_copy(update={"version": 2, "configured_at": EXECUTION_TIME+timedelta(minutes=1)}))
    return job_id, owned


def test_original_scheduler_worker_seal_finalizer_and_exact_reader_close_after_head_change(
    tmp_path, request: pytest.FixtureRequest
):
    page, backend, runtime, request, scheduler, worker, finalizer, reader, _ = host(
        tmp_path, request
    )
    try:
        job_id, owned = accept_and_change_head(page, backend, runtime, request, scheduler)
        entry = worker.claim_spool.pending()[0]
        claim = entry.claim
        validated = worker._validate_closed_claim(claim)
        manifest = worker.paper_directory.manifest_for_spec(validated.spec, worker.shard_runtime_manifest)
        from rquant.strict_json import strict_canonical_json_loads
        started = monotonic()
        result = execute_builtin_lab_shard(strict_canonical_json_loads(manifest.registry.configuration_json), validated, runtime_code_sha="a"*40)
        finished = monotonic()
        sealed = worker._seal_result(claim, result, deadline=claim.definition and owned.spec.deadline)
        worker.report_spool.publish(LabWorkerReport.from_claim(claim, report_id=uuid4(), reported_at=EXECUTION_TIME,
                                                              body=LabShardSucceeded.current(result_manifest_hash=sealed.manifest_hash, worker_code_sha="a"*40,
                                                                                              telemetry=LabShardTelemetry.from_work_plan(claim.definition.work_plan, monotonic_started=started, monotonic_finished=finished))))
        worker.claim_spool.consume(entry)
        assert scheduler.run_once().reports_accepted == 1
        assert reader.read(account_id=request.account_id, job_id=job_id, owner_id="alice", as_of=EXECUTION_TIME) is None
        assert finalizer.finalize(job_id).status == "published"
        assert scheduler.run_once().artifact_commits_accepted == 1
        value = reader.read(account_id=request.account_id, job_id=job_id, owner_id="alice", as_of=EXECUTION_TIME)
        assert value.configuration_version == 1 and value.reconcile.status == "consistent"
        assert value.reconcile.account.cash == 195 and value.reconcile.account.holdings[0].quantity == 800
        assert reader.reader.get_job(job_id).spec == owned.spec
        from rquant.paper_research_artifact import PaperResearchSealedAnalysis, PaperResearchSummary
        for changes in ({"result_hash": "f"*64}, {"task_name": "paper_backtest_band"}, {"account_id": "another-account"}):
            with pytest.raises(ValueError):
                PaperResearchSealedAnalysis.model_validate(value.model_copy(update=changes).model_dump(mode="python"))
        summary = reader.summary(account_id=request.account_id, job_id=job_id, owner_id="alice", as_of=EXECUTION_TIME)
        with pytest.raises(ValueError):
            PaperResearchSummary.model_validate(summary.model_copy(update={"configuration_version": 2}).model_dump(mode="python"))
        with pytest.raises(PermissionError):
            reader.read(account_id=request.account_id, job_id=job_id, owner_id="bob", as_of=EXECUTION_TIME)
    finally:
        worker.close()
        scheduler.release()
