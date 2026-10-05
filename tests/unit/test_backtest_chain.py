from __future__ import annotations

import hashlib
import io
import json
import os
import secrets
import shutil
import tempfile
import threading
import time
import zipfile
from dataclasses import asdict
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import uuid4

import pytest

from rquant.definition_registry import ImmutableDefinitionRegistry
from rquant.experiment_registry import DateRange, ExperimentRegistry
from rquant.lab_artifact_export import LabJobZipExportFacade
from rquant.lab_artifact_protocol import LabArtifactCommitSpool, LabFinalizerAuthorityKey
from rquant.lab_artifacts import LabJobArtifactStore
from rquant.lab_finalizer import LabFinalizer
from rquant.lab_job_center import ExperimentLifecycleCoordinator, LabCommandSubmissionFacade
from rquant.lab_job_protocol import LabCommandSpool
from rquant.lab_jobs import JobStatus, LabJobReader, LabJobStore, LabResultState
from rquant.lab_scheduler import LabScheduler
from rquant.lab_shard_protocol import LabClaimSpool, LabReportSpool
from rquant.lab_worker import LabWorker, build_builtin_shard_runtime_manifest
from rquant.page_control import PageControlConsumer, PageControlOutbox, PageControlService
from rquant.portfolio_backtest_artifact import PortfolioResultReader, PortfolioZipExportFacade
from rquant.portfolio_backtest_commands import (
    ExportPortfolioBacktestZip,
    PortfolioCommandWriter,
    SubmitPortfolioBacktest,
)
from rquant.portfolio_backtest_definition import bootstrap_portfolio_definition
from rquant.portfolio_backtest_source import PortfolioExperimentProtocol, PortfolioRequestPreparer
from rquant.research_catalog import ResearchCatalog
from rquant.runtime_definition_bootstrap import (
    bootstrap_builtin_definitions,
    plan_builtin_definitions,
)
from rquant.storage.duckdb import DuckDBStore
from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry
from rquant.strategy_job_adapters import default_strategy_job_adapter_registry
from tests.unit.test_backtest_platform import config
from tests.unit.test_backtest_preparation import source_data

NOW = datetime(2026, 10, 5, tzinfo=UTC)


def _private_directory(path: Path) -> Path:
    path.mkdir(mode=0o700)
    return path


def _remove_owned_tree(path: Path) -> None:
    for current, directories, _ in os.walk(path, followlinks=False):
        os.chmod(current, 0o700)
        for directory in directories:
            entry = Path(current) / directory
            if not entry.is_symlink():
                os.chmod(entry, 0o700)
    shutil.rmtree(path)


def test_pb02_pb03_pb09_actual_native_worker_original_journal_to_sealed_result(
    tmp_path: Path,
) -> None:
    data = source_data()
    code = data.template.producer_commit
    # This is synthetic v3 identity evidence, not an installed production capability.
    definitions_root = tmp_path / "definitions"
    plan = plan_builtin_definitions(producer_commit=code)
    bootstrap_builtin_definitions(
        definitions_root,
        producer_commit=code,
        registered_at=NOW,
        available_at=NOW,
        expected_plan_id=plan.plan_id,
    )
    bootstrap_portfolio_definition(definitions_root, producer_commit=code, now=NOW)
    builtins = BuiltinStrategyEvaluatorRegistry(producer_commit=code)
    definitions = ImmutableDefinitionRegistry(
        definitions_root, execution_registry=builtins.trusted_executable_registry()
    )
    experiments_root = _private_directory(tmp_path / "experiments")
    experiments = ExperimentRegistry(
        experiments_root / "registry.sqlite3", managed_trust_root=experiments_root
    )
    metadata = tmp_path / "metadata.duckdb"
    input_root = _private_directory(tmp_path / "prepared-inputs")
    catalog = ResearchCatalog(tmp_path / "research-catalog.duckdb")
    lake = tmp_path / "lake"
    protocol = PortfolioExperimentProtocol(
        train_range=DateRange(start_date=date(2025, 1, 1), end_date=date(2025, 6, 30)),
        validation_range=DateRange(start_date=date(2025, 7, 1), end_date=date(2025, 12, 31)),
        frozen_outer_test_range=DateRange(start_date=date(2026, 1, 1), end_date=date(2026, 6, 30)),
    )

    def source_provider(key: str, version: int):
        if (key, version) != (data.source_key, data.source_version):
            raise PermissionError("unknown synthetic source")
        return data

    prepare = PortfolioRequestPreparer(
        source_provider=source_provider,
        metadata_store_factory=lambda: DuckDBStore(metadata),
        catalog=catalog,
        lake_root=lake,
        input_root=input_root,
        definitions=definitions,
        experiments=experiments,
        protocol=protocol,
        code_commit=code,
        clock=lambda: NOW,
    )
    store = LabJobStore(tmp_path / "lab-jobs.sqlite3")
    store.initialize()
    reader = LabJobReader(store.path)
    commands = LabCommandSpool(tmp_path / "commands")
    claims, reports = LabClaimSpool(tmp_path / "claims"), LabReportSpool(tmp_path / "reports")
    commits = LabArtifactCommitSpool(tmp_path / "artifact-commits")
    artifacts = LabJobArtifactStore(tmp_path / "job-artifacts")
    result_reader = PortfolioResultReader(reader=reader, artifact_root=artifacts.root)
    old_exports = LabJobZipExportFacade(
        reader=reader,
        artifact_store=artifacts,
        export_root=_private_directory(tmp_path / "old-exports"),
    )
    exports = PortfolioZipExportFacade(
        result_reader=result_reader,
        original_exports=old_exports,
        reader=reader,
        artifact_store=artifacts,
        export_root=_private_directory(tmp_path / "exports"),
    )
    facade = LabCommandSubmissionFacade(
        reader=reader,
        spool=commands,
        experiment_registry=experiments,
        definition_registry=definitions,
        clock=lambda: NOW,
    )
    writer = PortfolioCommandWriter(commands=facade, prepare=prepare, exports=exports)
    outbox = PageControlOutbox(tmp_path / "page-control.sqlite3")
    service = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=tmp_path / "writer-data",
            log_dir=tmp_path / "writer-log",
            portfolio_backend=writer,
            clock=lambda: NOW,
        ),
    )
    original = SubmitPortfolioBacktest(
        command_id=str(uuid4()), requested_at=NOW, actor_id="researcher", config=config()
    )
    receipt = service.submit(original)
    assert receipt.status.value == "succeeded", receipt
    assert service.submit(original) == receipt
    assert len(tuple(input_root.iterdir())) == 1
    with pytest.raises(ValueError, match="conflict|different"):
        service.submit(original.model_copy(update={"actor_id": "admin"}))
    from rquant.portfolio_backtest_commands import portfolio_job_id

    job_id = portfolio_job_id(original.actor_id, original.command_id)
    initial_source_hashes = {
        str(path.relative_to(tmp_path)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in (*input_root.rglob("*.duckdb"), *lake.rglob("*.parquet"))
    }
    key = LabFinalizerAuthorityKey(key_id="portfolio-synthetic", secret=secrets.token_bytes(32))
    scheduler = LabScheduler(
        store=store,
        spool=commands,
        owner_id="synthetic-portfolio-scheduler",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=5,
        report_spool=reports,
        claim_spool=claims,
        claim_worker_ids=("synthetic-portfolio-worker",),
        shard_lease_seconds=120,
        artifact_commit_spool=commits,
        artifact_store=artifacts,
        adapter_registry=default_strategy_job_adapter_registry(),
        finalizer_authority_key_provider=lambda key_id: key if key_id == key.key_id else None,
        lifecycle_synchronizer=ExperimentLifecycleCoordinator(facade),
        clock=lambda: NOW,
    )
    socket_root = Path(tempfile.mkdtemp(prefix="pb-chain-", dir="/private/tmp"))
    old_tmp = os.environ.get("TMPDIR")
    old_tempdir = tempfile.tempdir
    os.environ["TMPDIR"] = str(socket_root)
    tempfile.tempdir = str(socket_root)
    outcomes: list[object] = []
    failures: list[BaseException] = []

    def execute(worker: LabWorker) -> None:
        try:
            outcomes.append(worker.run_once())
        except BaseException as error:
            failures.append(error)

    thread: threading.Thread | None = None
    try:
        first_tick = scheduler.run_once()
        print("initial_scheduler", first_tick)
        first_job = reader.get_job(job_id)
        print("initial_job", None if first_job is None else first_job.status.value)
        print("initial_claim_count", len(claims.pending()))
        worker = LabWorker(
            worker_id="synthetic-portfolio-worker",
            claim_spool=claims,
            report_spool=reports,
            artifact_root=tmp_path / "shard-artifacts",
            verified_code_sha_provider=lambda: code,
            shard_runtime_manifest=build_builtin_shard_runtime_manifest(
                catalog_path=metadata,
                forbidden_paths=(),
                snapshot_root=tmp_path / "worker-copies",
                research_lake_root=lake,
            ),
            heartbeat_interval_seconds=1,
            receipt_timeout_seconds=10,
            clock=lambda: NOW,
        )
        thread = threading.Thread(target=execute, args=(worker,), name="portfolio-native-proof")
        thread.start()
        deadline = time.monotonic() + 50
        while thread.is_alive() and time.monotonic() < deadline:
            scheduler.run_once()
            thread.join(0.02)
        assert not thread.is_alive(), "native worker did not return within proof budget"
        if failures:
            raise failures[0]
        assert len(outcomes) == 1 and outcomes[0].status == "succeeded", outcomes
        scheduler.run_once()
        finalizer = LabFinalizer(
            reader=reader,
            shard_artifact_root=tmp_path / "shard-artifacts",
            artifact_store=artifacts,
            commit_spool=commits,
            verified_code_sha_provider=lambda: code,
            finalizer_authority_key_provider=lambda: key,
        )
        finalized = finalizer.finalize(job_id)
        assert finalized.status == "published", finalized
        scheduler.run_once()
        job = reader.get_job(job_id)
        assert job is not None and job.status is JobStatus.SUCCEEDED
        assert job.result_state is LabResultState.SEALED
        result = result_reader.read(job_id, expected_result_hash=finalized.complete_result_hash)
        assert result.bundle.result.status == "complete"
        assert result.bundle.frozen.config == original.config
        export_command = ExportPortfolioBacktestZip(
            command_id=str(uuid4()),
            requested_at=NOW,
            actor_id="researcher",
            job_id=job_id,
            result_hash=result.result_hash,
        )
        export_receipt = service.submit(export_command)
        assert export_receipt.status.value == "succeeded", export_receipt
        assert service.submit(export_command) == export_receipt
        assert len(tuple(exports.export_root.iterdir())) == 1
        from pydantic import TypeAdapter

        from rquant.page_control import PageControlCommandValue
        from rquant.web.models.backtests import PortfolioSourceOption
        from rquant.web.portfolio_backtest_service import PortfolioWebService
        from rquant.web.settings import WebSettings
        from tests.support.web_proxy_identity import (
            ResearcherTestClient,
            create_private_test_app,
            with_test_proxy_identity,
        )

        web_service = PortfolioWebService(
            reader=reader,
            results=result_reader,
            exports=exports,
            preparation_available=True,
            default_config=config(),
            sources=(
                PortfolioSourceOption(
                    key=data.source_key,
                    version=data.source_version,
                    label="合成每日候选",
                    start_date=config().start_date,
                    end_date=config().end_date,
                    updated_at=NOW,
                    ranking_available=False,
                    industry_available=False,
                    opening_verified=True,
                ),
            ),
        )
        settings = with_test_proxy_identity(WebSettings(serving_root=tmp_path / "empty-serving"))
        settings = WebSettings.model_validate(
            settings.model_dump() | {"lab_control_users": ("researcher", "admin")}
        )
        command_adapter = TypeAdapter(PageControlCommandValue)

        def transport(payload: dict) -> dict:
            return service.submit(command_adapter.validate_python(payload)).model_dump(mode="json")

        app = create_private_test_app(
            settings,
            portfolio_backtests=web_service,
            lab_control_command_transport=transport,
            clock=lambda: NOW,
            background=False,
        )
        base = "/api/v1/backtests/portfolio"
        route = base + "/runs/" + str(job_id)
        params = {"result_hash": result.result_hash}
        write_headers = {"X-Rquant-Csrf": "1", "Origin": "http://testserver"}
        with ResearcherTestClient(app) as client:
            submitted_again = client.post(
                base + "/runs",
                headers=write_headers,
                json={
                    "command_id": original.command_id,
                    "requested_at": NOW.isoformat(),
                    "config": original.config.model_dump(mode="json"),
                },
            )
            assert submitted_again.status_code == 200 and submitted_again.json()["job_id"] == str(
                job_id
            ), submitted_again.text
            assert len(tuple(input_root.iterdir())) == 1
            summary = client.get(route)
            assert summary.status_code == 200, summary.text
            assert summary.json()["data"]["result_hash"] == result.result_hash
            assert summary.json()["data"]["performance"]["summary"] == asdict(
                result.bundle.performance.summary
            )
            nav = client.get(route + "/nav", params=params)
            assert nav.status_code == 200 and len(nav.json()["data"]["rows"]) == 2, nav.text
            for view in ("trades", "holdings", "daily", "monthly", "log"):
                response = client.get(route + "/rows", params=params | {"view": view, "limit": 50})
                assert (
                    response.status_code == 200
                    and response.json()["data"]["result_hash"] == result.result_hash
                ), response.text
            html = client.get(route + "/report.html", params=params)
            assert html.status_code == 200 and html.content == result.html_bytes(), html.text
            api_export = client.post(
                base + "/exports",
                headers=write_headers,
                json={
                    "command_id": export_command.command_id,
                    "requested_at": NOW.isoformat(),
                    "job_id": str(job_id),
                    "result_hash": result.result_hash,
                },
            )
            assert api_export.status_code == 200 and api_export.json()["status"] == "exported", (
                api_export.text
            )
            zip_response = client.get(
                route + "/exports/" + api_export.json()["zip_request_id"] + ".zip", params=params
            )
            assert zip_response.status_code == 200, zip_response.text
            assert hashlib.sha256(zip_response.content).hexdigest() == api_export.json()["sha256"]
            with zipfile.ZipFile(io.BytesIO(zip_response.content)) as archive:
                assert archive.read("report.html") == html.content
            assert len(tuple(exports.export_root.iterdir())) == 1
        assert initial_source_hashes == {
            str(path.relative_to(tmp_path)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in (*input_root.rglob("*.duckdb"), *lake.rglob("*.parquet"))
        }
        assert not list((tmp_path / "worker-copies").iterdir())
        assert not list((lake / ".execution_sessions").iterdir())
        print(
            json.dumps(
                {
                    "proof": "synthetic-native-portfolio/v1",
                    "job_id": str(job_id),
                    "spec_version": job.spec.schema_version,
                    "worker": outcomes[0].model_dump(mode="json"),
                    "finalizer": finalized.model_dump(mode="json"),
                    "result_status": result.bundle.result.status,
                    "result_hash": result.result_hash,
                    "source_unchanged": True,
                    "sessions_empty": True,
                    "journal_reused": True,
                    "real_authority_api_all_views": True,
                    "api_html_zip_exact": True,
                    "html_sha256": hashlib.sha256(html.content).hexdigest(),
                    "api_zip_sha256": hashlib.sha256(zip_response.content).hexdigest(),
                },
                ensure_ascii=False,
            )
        )
    finally:
        if thread is not None and thread.is_alive():
            thread.join(15)
        scheduler.release()
        artifacts.close()
        if old_tmp is None:
            os.environ.pop("TMPDIR", None)
        else:
            os.environ["TMPDIR"] = old_tmp
        tempfile.tempdir = old_tempdir
        _remove_owned_tree(socket_root)
