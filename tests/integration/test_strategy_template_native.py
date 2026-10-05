"""Parent-only native proof: first ID, original process, broker and sealed read."""

from __future__ import annotations

import multiprocessing
import threading
import time
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

from rquant.experiment_registry import DateRange, ExperimentRegistry
from rquant.lab_artifact_preview import ArtifactPreviewReader
from rquant.lab_artifact_protocol import LabArtifactCommitSpool, LabFinalizerAuthorityKey
from rquant.lab_artifacts import LabJobArtifactStore
from rquant.lab_finalizer import LabFinalizer
from rquant.lab_job_center import ExperimentLifecycleCoordinator, LabCommandSubmissionFacade
from rquant.lab_job_protocol import LabCommandSpool
from rquant.lab_jobs import JobStatus, LabJobReader, LabJobStore, LabResultState
from rquant.lab_scheduler import LabScheduler
from rquant.lab_shard_protocol import LabClaimSpool, LabReportSpool
from rquant.lab_worker import LabWorker, build_builtin_shard_runtime_manifest
from rquant.page_control import PageControlStatus
from rquant.page_control_service import build_page_control_service
from rquant.portfolio_backtest_models import PortfolioSourceManifest
from rquant.portfolio_backtest_source import PortfolioExperimentProtocol, PortfolioSourceData
from rquant.research_catalog import ResearchCatalog
from rquant.runtime_contracts import canonical_sha256
from rquant.storage.duckdb import DuckDBStore
from rquant.strategy_authoring import StrategyAuthoringPageControlBackend, StrategyAuthoringStore
from rquant.strategy_authoring_commands import ArchiveStrategyTemplate
from rquant.strategy_authoring_projection import build_strategy_authoring_snapshot
from rquant.strategy_template import StrategyTemplate
from rquant.strategy_template_artifact import StrategyTemplateSealedResultReader
from rquant.strategy_template_execution import TemplateEntryEvidence
from rquant.strategy_template_run_commands import RunStrategyTemplate
from rquant.strategy_template_runtime import StrategyTemplateRuntimeDirectory
from rquant.strategy_template_source import StrategyTemplateSourceData, TemplateRawDay
from rquant.strategy_template_submission import (
    StrategyTemplateRunBackend,
    StrategyTemplateRunPreparer,
)
from tests.unit.test_portfolio_backtest import _CODES, _request
from tests.unit.test_strategy_authoring import NOW, catalog, draft
from tests.unit.test_strategy_authoring_page_control import submit


def test_new_template_cold_host_original_worker_and_sealed_latest(tmp_path: Path) -> None:
    research = tmp_path / "research"
    research.mkdir(mode=0o700)
    request = _request((_CODES[0],) * 3)
    clock = lambda: datetime.now(UTC)
    target = StrategyAuthoringStore(
        research / "metadata.sqlite",
        definition_root=research / "definitions",
        producer_commit=request.producer_commit,
        clock=clock,
    )
    target.initialize()
    directory = StrategyTemplateRuntimeDirectory(target, expected_identity=target.identity())
    jobs = LabJobStore(research / "jobs.sqlite")
    jobs.initialize()
    reader = LabJobReader(jobs.path)
    commands = LabCommandSpool(research / "commands")
    claims = LabClaimSpool(research / "claims")
    reports = LabReportSpool(research / "reports")
    commits = LabArtifactCommitSpool(research / "commits")
    artifacts = LabJobArtifactStore(research / "results")
    experiments = ExperimentRegistry(research / "experiments.sqlite", managed_trust_root=research)
    key = LabFinalizerAuthorityKey(
        key_id="synthetic-template-native-finalizer",
        secret=b"synthetic-template-native-key-0000000000",
    )
    facade = LabCommandSubmissionFacade(
        reader=reader,
        spool=commands,
        experiment_registry=experiments,
        template_directory=directory,
        clock=clock,
    )
    scheduler = LabScheduler(
        store=jobs,
        spool=commands,
        owner_id="synthetic-template-native-scheduler",
        lease_seconds=120,
        heartbeat_seconds=10,
        poll_interval_ms=5,
        report_spool=reports,
        claim_spool=claims,
        claim_worker_ids=("synthetic-template-native-worker",),
        shard_lease_seconds=120,
        artifact_commit_spool=commits,
        artifact_store=artifacts,
        finalizer_authority_key_provider=lambda requested: key if requested == key.key_id else None,
        lifecycle_synchronizer=ExperimentLifecycleCoordinator(facade),
        template_directory=directory,
        runtime_guard=lambda: request.producer_commit,
        clock=clock,
    )
    manifest = build_builtin_shard_runtime_manifest(
        catalog_path=research / "research.duckdb",
        forbidden_paths=(),
        snapshot_root=research / "immutable-metadata",
        research_lake_root=research / "lake",
    )
    worker = LabWorker(
        worker_id="synthetic-template-native-worker",
        claim_spool=claims,
        report_spool=reports,
        artifact_root=research / "shards",
        template_directory=directory,
        shard_runtime_manifest=manifest,
        verified_code_sha_provider=lambda: request.producer_commit,
        heartbeat_interval_seconds=10,
        receipt_timeout_seconds=15,
        clock=clock,
    )
    finalizer = LabFinalizer(
        reader=reader,
        shard_artifact_root=research / "shards",
        artifact_store=artifacts,
        commit_spool=commits,
        template_directory=directory,
        verified_code_sha_provider=lambda: request.producer_commit,
        finalizer_authority_key_provider=lambda: key,
    )
    # These original hosts are constructed before any template ID exists.
    assert target.list_current(owner_id="alice") == ()
    rules = StrategyTemplate.model_validate(
        {
            "entry": {"kind": "conditions", "conditions": [{"key": "not_st"}]},
            "exit": {"max_holding_days": 1},
            "weight_rule": request.weight_rule,
            "rebalance_rule": {"kind": "every_n", "every_n_days": 5},
        }
    )
    source_manifest = PortfolioSourceManifest(
        source_mode="captured_with_retrospective_prices",
        market_hash=canonical_sha256(request.days),
        reference_hash=canonical_sha256(request.calendar),
        opening_hash=canonical_sha256(tuple(day.instruments for day in request.days)),
        ranking_hash=canonical_sha256(tuple(day.ranking for day in request.days)),
    )
    portfolio = PortfolioSourceData(
        source_key="synthetic-native-screen",
        source_version=1,
        template=request,
        sources=source_manifest,
        benchmarks={},
    )
    raw_days = tuple(
        TemplateRawDay(
            trade_date=day.trade_date,
            entry=TemplateEntryEvidence(
                observed_at=day.ranking.observed_at,
                source_hash=day.ranking.source_identity,
                rows=({"ts_code": _CODES[0], "is_st": False},),
            ),
        )
        for day in request.days
    )
    source = StrategyTemplateSourceData(
        owner_id="alice", catalog=catalog(), portfolio=portfolio, days=raw_days
    )
    input_root = research / "inputs"
    input_root.mkdir(mode=0o700)
    protocol = PortfolioExperimentProtocol(
        train_range=DateRange(start_date="2025-01-01", end_date="2025-01-31"),
        validation_range=DateRange(start_date="2025-02-01", end_date="2025-02-28"),
        frozen_outer_test_range=DateRange(start_date="2025-03-01", end_date="2025-03-31"),
    )
    preparer = StrategyTemplateRunPreparer(
        source_provider=lambda owner, generation, version: (
            StrategyTemplateSourceData.model_validate(
                {
                    **source.model_dump(mode="python"),
                    "catalog": source.catalog.model_copy(update={"generation_id": generation}),
                    "material_hash": None,
                }
            )
        ),
        metadata_store_factory=lambda: DuckDBStore(research / "research.duckdb"),
        catalog=ResearchCatalog(research / "catalog.duckdb"),
        lake_root=research / "lake",
        input_root=input_root,
        experiments=experiments,
        protocol=protocol,
        code_commit=request.producer_commit,
        clock=clock,
    )
    backend = StrategyTemplateRunBackend(
        target, facade=facade, preparer=preparer, expected_identity=target.identity()
    )
    page = build_page_control_service(
        outbox_path=research / "page-control.sqlite",
        data_dir=research / "private-data",
        log_dir=research / "private-logs",
        allowed_lab_export_roots=(),
        load_default_lab_backend=False,
        strategy_authoring_backend=StrategyAuthoringPageControlBackend(
            target, editor_users=("alice",), enabled=True, run_backend=backend
        ),
        clock=clock,
    )
    save = draft().model_copy(update={"rules": rules})
    assert submit(page, target, save).status is PageControlStatus.SUCCEEDED
    saved = target.lookup_command(save, owner_id="alice")
    run = RunStrategyTemplate(
        command_id=str(uuid4()),
        requested_at=NOW,
        generation_id="generation-a",
        strategy_id=saved.strategy_id,
        head=saved.head,
        expected_head=saved.head,
        start_date=request.days[0].trade_date,
        end_date=request.days[-1].trade_date,
        initial_cash=request.initial_cash,
    )
    assert submit(page, target, run).status is PageControlStatus.SUCCEEDED
    assert scheduler.run_once().plans_created == 1
    job_id = UUID(run.command_id)
    accepted = target.accepted_run(run, owner_id="alice", expected_identity=target.identity())
    # New head and archive cannot rewrite the already accepted old claim.
    update = draft(strategy_id=saved.strategy_id, expected_head=saved.head).model_copy(
        update={"rules": rules}
    )
    assert submit(page, target, update).status is PageControlStatus.SUCCEEDED
    newest = target.lookup_command(update, owner_id="alice")
    archive = ArchiveStrategyTemplate(
        command_id=str(uuid4()),
        requested_at=NOW,
        generation_id="generation-a",
        strategy_id=saved.strategy_id,
        expected_head=newest.head,
    )
    assert submit(page, target, archive).status is PageControlStatus.SUCCEEDED
    before_children = {child.pid for child in multiprocessing.active_children()}
    outcomes: list[object] = []
    errors: list[BaseException] = []

    def drive_worker() -> None:
        try:
            outcomes.append(worker.run_once())
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=drive_worker, name="template-native-one-tick")
    try:
        thread.start()
        expires = time.monotonic() + 55
        while thread.is_alive() and time.monotonic() < expires:
            scheduler.run_once()
            thread.join(timeout=0.02)
        assert not thread.is_alive(), "native worker tick exceeded its bounded proof window"
        assert not errors, errors
        assert outcomes and outcomes[0].status == "succeeded", outcomes
        assert reader.get_job(job_id).result_state is LabResultState.READY
        assert finalizer.finalize(job_id).status == "published"
        assert scheduler.run_once().artifact_commits_accepted == 1
        job = reader.get_job(job_id)
        assert job.status is JobStatus.SUCCEEDED and job.result_state is LabResultState.SEALED
        assert job.spec == accepted.spec
        preview = ArtifactPreviewReader(reader=reader, artifact_root=artifacts.root)
        equity = preview.preview(job_id, table_name="equity", row_limit=3, column_limit=12)
        cash_column = equity.table.columns.index("cash")
        assert Decimal(str(equity.table.rows[1][cash_column])) == Decimal("2989.00")
        sealed = StrategyTemplateSealedResultReader(reader=reader, artifact_reader=preview)
        snapshot = build_strategy_authoring_snapshot(
            target, available_at=clock(), source_catalogs=(catalog(),), sealed_result_reader=sealed
        )
        assert snapshot.versions[0].latest_run.job_id == job_id
        assert snapshot.versions[0].metadata.head == saved.head
        assert snapshot.versions[1].latest_run is None
        assert all(version.archived for version in snapshot.versions)
        print(
            "NATIVE_TEMPLATE_PROOF first-id/cold-host/original-page-control/snapshot/experiment/scheduler/isolated-worker/broker/finalizer/sealed-reference; 100@10 +5 buy; 100@10 -5 commission -1 stamp => cash 2989.00"
        )
    finally:
        worker.request_stop()
        thread.join(timeout=15)
        worker.close()
        scheduler.release()
        assert not thread.is_alive(), "native fixture left its own worker thread"
        assert {child.pid for child in multiprocessing.active_children()} <= before_children
        assert not worker._managed_authority_children
        print(
            "NATIVE_TEMPLATE_CLEANUP own-thread-joined/worker-closed/scheduler-released/no-new-active-child"
        )
