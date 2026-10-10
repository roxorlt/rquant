"""Serve one original Alice owner for synthetic report and promotion browser gates."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import sys
import tempfile
import threading
import time
from collections.abc import Awaitable, Callable, Sequence
from datetime import datetime
from pathlib import Path
from types import FrameType
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

ROOT = Path(__file__).resolve().parents[1]
for directory in (ROOT, ROOT / "src"):
    if str(directory) not in sys.path:
        sys.path.insert(0, str(directory))

if TYPE_CHECKING:
    from starlette.requests import Request
    from starlette.responses import Response

    from rquant.strategy_template_runtime import StrategyTemplateRuntimeDirectory
    from tests.support.strategy_promotion_fixture import StrategyPromotionIntegrationFixture


def _seal_parent_run(
    fixture: StrategyPromotionIntegrationFixture,
    job_id: UUID,
    directory: StrategyTemplateRuntimeDirectory,
) -> None:
    """Use the original parent directory; macro children use their separate private store."""
    from rquant.lab_finalizer import LabFinalizer
    from rquant.lab_worker import LabWorker, build_builtin_shard_runtime_manifest

    foundation = fixture.foundation
    ipc = Path(tempfile.mkdtemp(prefix="rq-owner-template-", dir=Path("/tmp").resolve()))
    ipc.chmod(0o700)
    previous_tempdir, previous_env = tempfile.tempdir, os.environ.get("TMPDIR")
    tempfile.tempdir, os.environ["TMPDIR"] = str(ipc), str(ipc)
    worker, thread = None, None
    try:
        foundation.scheduler.run_once()
        worker = LabWorker(
            worker_id="ai-synthetic-worker",
            claim_spool=foundation.original_worker_claim_spool(),
            report_spool=foundation.reports,
            artifact_root=foundation.shard_root,
            template_directory=directory,
            verified_code_sha_provider=lambda: foundation.code_commit,
            shard_runtime_manifest=build_builtin_shard_runtime_manifest(
                catalog_path=foundation.metadata_path,
                forbidden_paths=(),
                snapshot_root=foundation.shard_root.parent / "worker-copies",
                research_lake_root=foundation.lake_root,
            ),
            heartbeat_interval_seconds=1,
            receipt_timeout_seconds=10,
            clock=fixture.clock,
        )
        outcomes, errors = [], []

        def execute() -> None:
            try:
                outcomes.append(worker.run_once())
            except BaseException as error:
                errors.append(error)

        thread = threading.Thread(target=execute, name="owned-parent-template-worker")
        thread.start()
        while thread.is_alive():
            foundation.scheduler.run_once()
            thread.join(0.02)
        if errors:
            raise errors[0]
        if len(outcomes) != 1 or outcomes[0].status != "succeeded":
            raise RuntimeError("original parent template worker did not succeed")
        foundation.scheduler.run_once()
        result = LabFinalizer(
            reader=foundation.reader,
            shard_artifact_root=foundation.shard_root,
            artifact_store=foundation.artifacts,
            commit_spool=foundation.commits,
            template_directory=directory,
            verified_code_sha_provider=lambda: foundation.code_commit,
            finalizer_authority_key_provider=lambda: foundation.key,
        ).finalize(job_id)
        if result.status != "published":
            raise RuntimeError("original parent template finalizer did not publish")
        foundation.scheduler.run_once()
    finally:
        if worker is not None:
            worker.request_stop()
        if thread is not None:
            thread.join(10)
        if worker is not None:
            worker.close()
        tempfile.tempdir = previous_tempdir
        if previous_env is None:
            os.environ.pop("TMPDIR", None)
        else:
            os.environ["TMPDIR"] = previous_env
        alive = thread is not None and thread.is_alive()
        children = 0
        if worker is not None:
            with worker._managed_authority_children_lock:
                children = len(worker._managed_authority_children)
        if not alive and not children:
            shutil.rmtree(ipc)
        record = {
            "job_id": str(job_id),
            "original_worker_thread_alive": alive,
            "owned_authority_children_remaining": children,
            "owned_ipc_root": str(ipc),
            "owned_ipc_root_removed": not ipc.exists(),
            "tempdir_restored": tempfile.tempdir == previous_tempdir,
            "environment_restored": os.environ.get("TMPDIR") == previous_env,
        }
        (fixture.root / "parent-template-worker-cleanup.json").write_text(
            json.dumps(record, indent=2) + "\n"
        )
        if alive or children:
            raise RuntimeError("original parent template cleanup incomplete")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True, help="new private synthetic owner root")
    parser.add_argument("--bind", required=True, help="IPv4 loopback host:port")
    parser.add_argument("--proxy-proof-file", type=Path, required=True)
    parser.add_argument("--evidence-dir", type=Path)
    args = parser.parse_args(argv)
    host, port = args.bind.rsplit(":", 1)
    if host != "127.0.0.1" or not port.isdecimal() or not 1 <= int(port) <= 65535:
        parser.error("the original synthetic owner requires IPv4 loopback")
    root = args.root
    if not root.is_absolute() or root.resolve() != root:
        parser.error("--root must be an absolute canonical path")
    if args.proxy_proof_file.parent != root:
        parser.error("--proxy-proof-file must be inside the new private owner root")
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    os.environ.update(
        {
            "RQUANT_DISABLE_DOTENV": "1",
            "TUSHARE_TOKEN_MAIN": "synthetic-test-token-0000000000000000",
            "DATA_DIR": str(root / "data"),
            "DUCKDB_PATH": str(root / "unused-primary.duckdb"),
            "PARQUET_DIR": str(root / "parquet"),
            "LOG_DIR": str(root / "logs"),
            "NOTIFY_ENABLED": "false",
        }
    )
    if args.evidence_dir is not None:
        args.evidence_dir.mkdir(parents=True, exist_ok=True)
    origin = time.monotonic()

    def record(stage: str, **fields: object) -> None:
        value = {"stage": stage, "elapsed_seconds": time.monotonic() - origin, **fields}
        print(json.dumps(value, ensure_ascii=False, default=str), flush=True)
        if args.evidence_dir is not None:
            with (args.evidence_dir / "stages.jsonl").open("a") as output:
                output.write(json.dumps(value, ensure_ascii=False, default=str) + "\n")

    fixture, ipc = None, None
    servers, threads = [], []

    def stop(_signal: int, _frame: FrameType | None) -> None:
        raise SystemExit(0)

    previous = signal.signal(signal.SIGTERM, stop)
    try:
        import uvicorn

        from rquant.factor.definition_serving import project_factor_definition_serving_snapshot
        from rquant.factor.registry import FactorDefinitionRegistry
        from rquant.factor.result_serving import FactorResultProjectionReader
        from rquant.factor.run_backend import FactorRunPageControlBackend
        from rquant.factor.run_configuration import run_configured_factor_worker
        from rquant.factor.serving_projection import project_factor_definition_projections
        from rquant.factor_definition_admission import (
            FactorDefinitionAdmission,
            build_factor_definition_admission_server,
        )
        from rquant.factor_run_admission import FactorRunAdmission
        from rquant.lab_artifact_preview import ArtifactPreviewReader
        from rquant.lab_jobs_serving_authority import LabJobsServingSourceReader
        from rquant.portfolio_backtest_commands import (
            PortfolioCommandWriter,
            SubmitPortfolioBacktest,
        )
        from rquant.portfolio_backtest_source import PortfolioRequestPreparer
        from rquant.research_catalog import ResearchCatalog
        from rquant.serving_page_projection_source import _ReadonlyPageControlAuditReader
        from rquant.serving_publisher import ServingPublisher
        from rquant.serving_read_models import (
            SERVING_TABLE_SPECS,
            ServingProjectionInput,
            ServingProjectionPayload,
            ServingReadModelInput,
            build_serving_read_models,
        )
        from rquant.storage.duckdb import DuckDBStore
        from rquant.strategy_authoring import StrategyAuthoringPageControlBackend
        from rquant.strategy_authoring_admission import (
            StrategyAuthoringAdmissionClient,
            build_strategy_authoring_admission_server,
        )
        from rquant.strategy_authoring_projection import StrategyAuthoringProjectionSource
        from rquant.strategy_promotion_projection import StrategyPromotionProjectionReader
        from rquant.strategy_template_artifact import StrategyTemplateSealedResultReader
        from rquant.strategy_template_definition import StrategyTemplateExecutionVersion
        from rquant.strategy_template_run_commands import (
            RunStrategyTemplate,
            StrategyTemplateRunReceipt,
        )
        from rquant.strategy_template_runtime import StrategyTemplateRuntimeDirectory
        from rquant.strategy_template_source import StrategyTemplateSourceData
        from rquant.strategy_template_submission import (
            StrategyTemplateRunBackend,
            StrategyTemplateRunPreparer,
        )
        from rquant.strict_json import strict_json_loads
        from rquant.web.app import create_app
        from rquant.web.collaboration_gateway import CollaborationGateway
        from rquant.web.experiment_platform_service import ExperimentWebService
        from rquant.web.portfolio_backtest_service import PortfolioWebService
        from rquant.web.settings import WebSettings
        from scripts.serve_web_fixture import _prepare_proxy_proof
        from tests.support.ai_assistance_fixture import private_directory
        from tests.support.strategy_promotion_fixture import (
            build_original_promotion_fixture,
            seal_original_complete_promotion,
        )
        from tests.support.web_serving_fixture import _generation_ids, _watermarks
        from tests.unit.test_backtest_platform import config
        from tests.unit.test_backtest_preparation import source_data
        from tests.unit.test_factor_run_configuration import _configured

        record("prepare_macro")
        fixture = build_original_promotion_fixture(root)
        foundation = fixture.foundation
        approvals = seal_original_complete_promotion(fixture)
        record("macro_sealed", approvals=[value.model_dump(mode="json") for value in approvals])

        portfolio_source = source_data()
        portfolio_preparer = PortfolioRequestPreparer(
            source_provider=lambda _key, _version: portfolio_source,
            metadata_store_factory=lambda: DuckDBStore(foundation.metadata_path),
            catalog=ResearchCatalog(foundation.catalog_path),
            lake_root=foundation.lake_root,
            input_root=foundation.input_root,
            definitions=foundation.commands.definition_registry,
            experiments=foundation.commands.experiment_registry,
            protocol=foundation.protocol,
            code_commit=foundation.code_commit,
            clock=fixture.clock,
        )
        fixture.control.consumer.portfolio_backend = PortfolioCommandWriter(
            commands=foundation.commands, prepare=portfolio_preparer
        )
        request = SubmitPortfolioBacktest(
            command_id=str(uuid4()), requested_at=fixture.clock(), actor_id="alice", config=config()
        )
        receipt = fixture.control.submit_authorized(
            request,
            fixture.control.collaboration.issue_authorization(
                "alice", request.model_dump(mode="json")
            ),
        )
        if receipt.status.value != "succeeded":
            raise RuntimeError("original portfolio admission did not complete")
        portfolio_job = UUID(receipt.result["job_id"])
        record("portfolio_admitted", receipt=receipt.model_dump(mode="json"))
        portfolio_result = foundation.seal_with_original_worker(portfolio_job)
        record("portfolio_sealed", job_id=portfolio_job, result_hash=portfolio_result.result_hash)

        factor_root, factor_reference, factor_request = _configured(
            private_directory(root / "factor")
        )
        factor_backend = FactorRunPageControlBackend(
            factor_root, factor_reference, clock=fixture.clock
        )
        factor_config = factor_backend.configuration()
        fixture.control.consumer.factor_run_backend = factor_backend
        factor_request = type(factor_request).model_validate(
            factor_request.model_dump(mode="python") | {"requested_at": fixture.clock()}
        )
        factor_receipt = FactorRunAdmission(
            fixture.control, run_users=frozenset({"alice"}), enabled=True
        ).submit(
            factor_request,
            authenticated_actor_id="alice",
            verified_registry_instance_id=factor_config.registry_identity.instance_id,
        )
        if (
            factor_receipt.status != "submitted"
            or factor_receipt.original_request != factor_request
        ):
            raise RuntimeError("original factor admission did not complete")
        record("factor_admitted", result=factor_receipt.model_dump(mode="json"))
        factor_result = run_configured_factor_worker(
            factor_root, factor_reference, clock=fixture.clock
        )
        if (
            factor_result.status != "succeeded"
            or factor_result.record.job_id != factor_receipt.job_id
            or factor_result.record.spec_sha256 != factor_receipt.spec_sha256
        ):
            raise RuntimeError("original factor worker did not succeed")
        record("factor_sealed", result=factor_result.model_dump(mode="json"))

        parent = fixture.record.request.template
        version = fixture.binding.original.get_version(
            parent.strategy_id, parent.head.version, owner_id="alice"
        )
        directory = StrategyTemplateRuntimeDirectory(
            fixture.binding.original, expected_identity=fixture.binding.expected_original_identity
        )

        def parent_source(
            owner: str, generation: str, selected: StrategyTemplateExecutionVersion
        ) -> StrategyTemplateSourceData:
            from rquant.experiment_platform import ExperimentPhaseRead

            if owner != "alice" or selected.head != version.head:
                raise PermissionError("exact original authored parent is required")
            profile = fixture.family_preparer.profiles[0]
            phase = ExperimentPhaseRead(
                owner=owner,
                family_id=fixture.record.family_id,
                source_identity=profile.source_identity,
                source_key=profile.source_key,
                source_version=profile.source_version,
                phase="search",
                window=fixture.record.request.protocol.train_range.model_copy(
                    update={"end_date": fixture.record.request.protocol.validation_range.end_date}
                ),
            )
            material = fixture.binding.phase_provider(phase, selected)
            return StrategyTemplateSourceData.model_validate(
                material.model_dump(mode="python")
                | {
                    "catalog": material.catalog.model_copy(update={"generation_id": generation}),
                    "material_hash": None,
                }
            )

        template_preparer = StrategyTemplateRunPreparer(
            source_provider=parent_source,
            metadata_store_factory=fixture.family_preparer.metadata_store_factory,
            catalog=fixture.family_preparer.catalog,
            lake_root=foundation.lake_root,
            input_root=foundation.input_root,
            experiments=foundation.commands.experiment_registry,
            protocol=foundation.protocol,
            code_commit=foundation.code_commit,
            clock=fixture.clock,
        )
        foundation.commands.template_directory = foundation.scheduler.template_directory = directory
        run_backend = StrategyTemplateRunBackend(
            fixture.binding.original,
            facade=foundation.commands,
            preparer=template_preparer,
            expected_identity=fixture.binding.expected_original_identity,
        )
        fixture.control.consumer.strategy_authoring_backend = StrategyAuthoringPageControlBackend(
            fixture.binding.original, editor_users=("alice",), enabled=True, run_backend=run_backend
        )
        template_request = RunStrategyTemplate(
            command_id=str(uuid4()),
            requested_at=fixture.clock(),
            generation_id=fixture.binding.catalogs[0].generation_id,
            strategy_id=version.strategy_id,
            head=version.head,
            expected_head=version.head,
            start_date=fixture.record.request.base_config.start_date,
            end_date=fixture.record.request.base_config.end_date,
            initial_cash=fixture.record.request.base_config.initial_cash,
        )
        template_receipt = fixture.admission.submit(
            template_request,
            authenticated_actor_id="alice",
            verified_metadata_identity=fixture.binding.expected_original_identity,
        )
        if template_receipt.receipt.status.value != "succeeded":
            raise RuntimeError("original template admission did not complete")
        template_job = StrategyTemplateRunReceipt.model_validate(
            template_receipt.receipt.result
        ).job_id
        record("template_admitted", receipt=template_receipt.model_dump(mode="json"))
        _seal_parent_run(fixture, template_job, directory)
        template_results = StrategyTemplateSealedResultReader(
            reader=foundation.reader,
            artifact_reader=ArtifactPreviewReader(
                reader=foundation.reader, artifact_root=foundation.artifacts.root
            ),
        )
        template_authority = foundation.reader.get_artifact_preview_authority(template_job)
        sealed_template = template_results.read_run(
            template_job,
            store=fixture.binding.original,
            expected_identity=fixture.binding.expected_original_identity,
            private_owner="alice",
            expected_result_hash=template_authority.evidence.complete_result_hash,
        )
        record("template_sealed", job_id=template_job, result_hash=sealed_template.result_hash)

        audit = _ReadonlyPageControlAuditReader(fixture.control.outbox.path)
        authoring_source = StrategyAuthoringProjectionSource(
            fixture.binding.original,
            expected_identity=fixture.binding.expected_original_identity,
            source_catalog_provider=lambda _at: fixture.binding.catalogs,
            sealed_result_reader=template_results,
        )
        factor_registry = FactorDefinitionRegistry(factor_config.registry_identity.path)

        def page_projections(at: datetime) -> tuple[ServingProjectionPayload, ...]:
            definitions = project_factor_definition_serving_snapshot(
                factor_registry, expected_identity=factor_config.registry_identity, available_at=at
            )
            return (*authoring_source(at), *project_factor_definition_projections(definitions))

        lab_source = LabJobsServingSourceReader(
            reader=foundation.reader,
            page_projection_reader=page_projections,
            factor_result_projection_reader=FactorResultProjectionReader(
                factor_config.ledger_identity,
                factor_config.artifact_root,
                collaboration_audit_reader=audit,
                collaboration=fixture.control.collaboration,
            ),
            collaboration_audit_reader=audit,
            collaboration=fixture.control.collaboration,
        )
        publisher = ServingPublisher(
            root / "serving",
            producer_commit=foundation.code_commit,
            schema_version=3,
            table_specs=SERVING_TABLE_SPECS,
        )

        def publish() -> None:
            at = fixture.clock()
            source = lab_source(at)
            generations = _generation_ids("baseline", fixture.publication_sequence[0])
            promotion = (
                *fixture.projection(at),
                *StrategyPromotionProjectionReader(fixture.binding.private)(at),
            )
            inputs = tuple(
                ServingProjectionInput.bind(
                    value, owner_dataset_id=dataset, owner_generation_id=generations[dataset]
                )
                for dataset, values in (
                    ("lab_jobs", source.payload.projections),
                    ("promotions", promotion),
                )
                for value in values
            )
            manifest = publisher.publish(
                build_serving_read_models(
                    ServingReadModelInput(
                        observed_at=at, lab_jobs=source.payload.lab_jobs, projections=inputs
                    )
                ),
                watermarks=_watermarks(
                    "baseline",
                    built_at=at,
                    generations=generations,
                    sequence=fixture.publication_sequence[0],
                ),
                source_generations=generations,
                built_at=at,
            )
            fixture.publication_sequence[0] += 1
            record("published", generation_id=manifest.generation_id, server_time=at.isoformat())

        publish()
        _prepare_proxy_proof(args.proxy_proof_file)
        ipc = Path(tempfile.mkdtemp(prefix="rq-owned-api-", dir=Path("/tmp").resolve()))
        synthetic_web_uid, gid = os.geteuid() + 10_000, os.getegid()
        collaboration_socket, strategy_socket = (
            ipc / "collaboration/ipc.sock",
            ipc / "strategy/ipc.sock",
        )
        peers = {
            "trusted_web_uid": synthetic_web_uid,
            "shared_gid": gid,
            "peer_uid": lambda _connection: synthetic_web_uid,
        }
        servers.append(
            build_factor_definition_admission_server(
                FactorDefinitionAdmission(
                    fixture.control, editor_users=frozenset(), save_enabled=False
                ),
                socket_path=collaboration_socket,
                **peers,
            )
        )
        servers.append(
            build_strategy_authoring_admission_server(
                fixture.admission, socket_path=strategy_socket, **peers
            )
        )
        for index, server in enumerate(servers):
            thread = threading.Thread(target=server.serve_forever, name=f"owned-authority-{index}")
            threads.append(thread)
            thread.start()
        client_options = {
            "expected_service_uid": os.geteuid(),
            "shared_gid": gid,
            "client_uid": lambda: synthetic_web_uid,
        }
        collaboration = CollaborationGateway(collaboration_socket, **client_options)
        strategy_client = StrategyAuthoringAdmissionClient(
            strategy_socket, timeout_seconds=5.0, **client_options
        )
        app = create_app(
            WebSettings(
                serving_root=root / "serving",
                ingress_socket_path=root / "web-private.sock",
                proxy_proof_file=args.proxy_proof_file,
                collaboration_mode="enforced",
                strategy_authoring_enabled=True,
                strategy_authoring_users=frozenset({"alice"}),
                strategy_promotion_enabled=True,
                strategy_promotion_users=frozenset({"alice"}),
            ),
            clock=fixture.clock,
            collaboration_gateway=collaboration,
            strategy_authoring_gateway=strategy_client,
            strategy_promotion_gateway=strategy_client,
            portfolio_backtests=PortfolioWebService(
                reader=foundation.reader, results=foundation.results
            ),
            experiment_platform=ExperimentWebService(
                results=foundation.results,
                template_results=template_results,
                private_authority=fixture.projection.authority,
            ),
        )

        @app.middleware("http")
        async def publish_original_command(
            request: Request, call_next: Callable[[Request], Awaitable[Response]]
        ) -> Response:
            promotion_command = (
                request.method == "POST"
                and request.url.path == "/api/v1/strategy-promotions/commands"
            )
            original_body = await request.body() if promotion_command else None
            response = await call_next(request)
            if (
                promotion_command
                and response.status_code == 200
                and strict_json_loads(original_body)["kind"]
                in {"request_promotion_review", "approve_promotion"}
            ):
                publish()
                app.state.web.tracker.refresh()
            return response

        record(
            "ready",
            actor="alice",
            server_time=fixture.clock().isoformat(),
            portfolio_job_id=portfolio_job,
            template_job_id=template_job,
            synthetic_market=True,
            r03_native_paper_passed=False,
        )
        uvicorn.run(
            app,
            host=host,
            port=int(port),
            workers=1,
            proxy_headers=False,
            server_header=False,
            access_log=False,
            log_level="warning",
        )
    except BaseException as error:
        if not isinstance(error, SystemExit) or error.code:
            record("failed", error_type=type(error).__name__, error=str(error))
        raise
    finally:
        try:
            for index, server in enumerate(servers):
                if index < len(threads) and threads[index].is_alive():
                    server.shutdown()
                    threads[index].join()
                server.server_close()
            if fixture is not None:
                if args.evidence_dir is not None:
                    for source in (
                        *root.glob("worker-cleanup-*.json"),
                        *root.glob("parent-template-worker-cleanup.json"),
                        *root.glob("lab/worker-cleanup-*.json"),
                    ):
                        shutil.copyfile(source, args.evidence_dir / source.name)
                fixture.foundation.scheduler.release()
                fixture.close()
        finally:
            if ipc is not None:
                shutil.rmtree(ipc)
            for directory, _, _ in os.walk(root, followlinks=False):
                os.chmod(directory, 0o700)
            shutil.rmtree(root)
            signal.signal(signal.SIGTERM, previous)
            record(
                "cleanup",
                owner_root_removed=not root.exists(),
                ipc_root_removed=ipc is None or not ipc.exists(),
                authority_threads_alive=[thread.name for thread in threads if thread.is_alive()],
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
