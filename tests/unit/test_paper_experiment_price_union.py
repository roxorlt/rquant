"""Accepted paper, private experiments and price facts share the original hosts."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
import sqlite3
from time import monotonic
from uuid import UUID, uuid4

import pytest

from rquant.lab_artifact_protocol import LabArtifactCommitSpool, LabFinalizerAuthorityKey
from rquant.lab_artifacts import LabJobArtifactStore
from rquant.lab_finalizer import LabFinalizer
from rquant.lab_job_center import LabCommandSubmissionFacade
from rquant.lab_jobs import LabJobReader
from rquant.lab_scheduler import LabScheduler
from rquant.lab_shard_protocol import (
    LabClaimSpool,
    LabReportSpool,
    LabShardSucceeded,
    LabShardTelemetry,
    LabWorkerReport,
)
from rquant.lab_worker import LabWorker, build_builtin_shard_runtime_manifest
from rquant.lab_worker_registry import execute_builtin_lab_shard
from rquant.page_control import PageControlStatus, parse_page_control_command
from rquant.page_control_service import build_page_control_service
from tests.unit.test_experiment_platform import NOW
from tests.unit.test_experiment_platform_flow import preparation as preparation


def test_one_original_lab_host_seals_paper_and_private_template_jobs(
    preparation: tuple,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    from rquant.experiment_platform_commands import (
        RegisterExperimentFamily,
        bind_experiment_platform,
    )
    from rquant.experiment_platform_templates import ExperimentTemplateRuntimeBinding
    from rquant.paper_research_runtime import PaperResearchRuntimeDirectory
    from rquant.portfolio_backtest_artifact import PortfolioResultReader
    from rquant.resource_admission import TradingSession
    from rquant.strict_json import strict_canonical_json_loads
    from tests.unit.test_experiment_platform_templates import configure_template
    from tests.unit.test_paper_research_submission import fixture
    from tests.unit.test_paper_signal_worker import EXECUTION_TIME

    platform, prepare, profile, selected, search = configure_template(preparation, tmp_path)
    platform.install_policy(months=0, now=NOW)
    private = ExperimentTemplateRuntimeBinding(store=platform, binding=selected)
    paper_root = tmp_path / "paper"
    paper_root.mkdir(mode=0o700)
    _, paper_backend, runtime, paper_request, jobs = fixture(paper_root)
    paper = PaperResearchRuntimeDirectory(
        states=(runtime.state,), expected_identities=(runtime.state.identity(),)
    )
    reader = LabJobReader(jobs.path)
    runs = paper_backend.research_backend
    pinned_writer = sqlite3.connect(runs.preparer.sources[0].broker.path)
    pinned_writer.execute("SELECT count(*) FROM paper_order").fetchone()
    request.addfinalizer(pinned_writer.close)
    # Both real producers publish to the same synthetic host metadata and lake.
    runs.preparer.metadata_store_factory = preparation[5]["metadata_store_factory"]
    runs.preparer.research_catalog = preparation[5]["catalog"]
    runs.preparer.lake_root = prepare.lake_root
    runs.preparer.code_sha = profile.producer_commit
    clock = [EXECUTION_TIME]
    facade = LabCommandSubmissionFacade(
        reader=reader,
        spool=runs.facade.spool,
        experiment_registry=platform.registry,
        definition_registry=prepare.definitions,
        experiment_template_binding=private,
        clock=lambda: clock[0],
    )
    runs.facade = facade
    commits = LabArtifactCommitSpool(tmp_path / "commits")
    artifacts = LabJobArtifactStore(tmp_path / "results")
    binding = bind_experiment_platform(
        store=platform,
        commands=facade,
        prepare=prepare,
        results=PortfolioResultReader(reader=reader, artifact_root=artifacts.root),
        default_config=search.base_config,
        owners=frozenset({"alice"}),
        administrators=frozenset({"alice"}),
        enabled=True,
    )
    service = build_page_control_service(
        outbox_path=tmp_path / "journal.sqlite",
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
        allowed_lab_export_roots=(),
        load_default_lab_backend=False,
        experiment_backend=binding.command_backend,
        paper_portfolio_backend=paper_backend,
        clock=lambda: clock[0],
    )
    claims, reports = LabClaimSpool(tmp_path / "claims"), LabReportSpool(tmp_path / "reports")
    key = LabFinalizerAuthorityKey(
        key_id="union-synthetic", secret=b"synthetic-union-key-00000000000000"
    )
    scheduler = LabScheduler(
        store=jobs,
        spool=facade.spool,
        owner_id="union-scheduler",
        lease_seconds=120,
        heartbeat_seconds=10,
        poll_interval_ms=5,
        claim_spool=claims,
        report_spool=reports,
        claim_worker_ids=("union-worker",),
        artifact_commit_spool=commits,
        artifact_store=artifacts,
        finalizer_authority_key_provider=lambda requested: key if requested == key.key_id else None,
        experiment_template_binding=private,
        paper_directory=paper,
        lifecycle_synchronizer=binding.lifecycle,
        clock=lambda: clock[0],
    )
    worker = LabWorker(
        worker_id="union-worker",
        claim_spool=claims,
        report_spool=reports,
        artifact_root=tmp_path / "shards",
        shard_runtime_manifest=build_builtin_shard_runtime_manifest(
            catalog_path=tmp_path / "metadata.duckdb",
            forbidden_paths=(),
            snapshot_root=tmp_path / "worker-copies",
            research_lake_root=prepare.lake_root,
        ),
        verified_code_sha_provider=lambda: profile.producer_commit,
        experiment_template_binding=private,
        paper_directory=paper,
        heartbeat_interval_seconds=10,
        receipt_timeout_seconds=15,
        clock=lambda: clock[0],
    )
    finalizer = LabFinalizer(
        reader=reader,
        shard_artifact_root=tmp_path / "shards",
        artifact_store=artifacts,
        commit_spool=commits,
        verified_code_sha_provider=lambda: profile.producer_commit,
        finalizer_authority_key_provider=lambda: key,
        experiment_template_binding=private,
        paper_directory=paper,
    )

    class WireCaptured(Exception):
        pass

    wire_requests: list[dict[str, object]] = []

    def capture_wire(**values: object) -> None:
        wire_requests.append(values)
        raise WireCaptured

    # Stop before child/thread creation, then execute the original fixed child entry inline.
    import rquant.lab_worker as original_worker

    monkeypatch.setattr(original_worker, "_ShardWireRequest", capture_wire)
    completed: set[UUID] = set()

    def close_next_job() -> UUID:
        entries = claims.pending()
        assert entries, "one original queue must continue both families"
        entry = entries[0]
        claim = entry.claim
        validated = worker._validate_closed_claim(claim)
        with pytest.raises(WireCaptured):
            worker._execute_shard_isolated(
                claim,
                validated,
                runtime_code_sha=profile.producer_commit,
                hard_limit_seconds=1,
                initial_session=TradingSession.CLOSED,
            )
        manifest = wire_requests[-1]["manifest"]
        started = monotonic()
        result = execute_builtin_lab_shard(
            strict_canonical_json_loads(manifest.registry.configuration_json),
            validated,
            runtime_code_sha=profile.producer_commit,
        )
        finished = monotonic()
        sealed = worker._seal_result(claim, result, deadline=validated.spec.deadline)
        reports.publish(
            LabWorkerReport.from_claim(
                claim,
                report_id=uuid4(),
                reported_at=clock[0],
                body=LabShardSucceeded.current(
                    result_manifest_hash=sealed.manifest_hash,
                    worker_code_sha=profile.producer_commit,
                    telemetry=LabShardTelemetry.from_work_plan(
                        claim.definition.work_plan,
                        monotonic_started=started,
                        monotonic_finished=finished,
                    ),
                ),
            )
        )
        claims.consume(entry)
        assert scheduler.run_once().reports_accepted == 1
        assert finalizer.finalize(claim.job_id).status == "published"
        assert scheduler.run_once().artifact_commits_accepted == 1
        completed.add(claim.job_id)
        return claim.job_id

    try:
        paper_receipt = service._submit_trusted_paper_portfolio(
            paper_request,
            authenticated_actor_id="alice",
            verified_metadata_identity=runtime.state.identity(),
        )
        assert paper_receipt.status is PageControlStatus.SUCCEEDED
        assert len(facade.spool.pending()) == 1
        owned = runs.lookup(
            paper_request, owner_id="alice", expected_identity=runtime.state.identity()
        )
        with pytest.raises(ValueError):
            parse_page_control_command(owned.model_dump(mode="json"))
        runtime.state.start_configuration(
            runtime.state.configuration.model_copy(
                update={"version": 2, "configured_at": EXECUTION_TIME + timedelta(minutes=1)}
            )
        )
        assert scheduler.run_once().plans_created == 1
        assert close_next_job() == UUID(paper_request.command_id)

        # The existing synthetic families have different observation dates.
        # Advance the same host clock only after its earlier accepted task closes.
        scheduler.release()
        clock[0] = NOW
        registration = RegisterExperimentFamily(
            command_id=str(UUID(int=950)), requested_at=NOW, actor_id="alice", request=search
        )
        experiment_receipt = service.submit(registration)
        assert experiment_receipt.status is PageControlStatus.SUCCEEDED
        job_ids = tuple(UUID(v) for v in experiment_receipt.result["job_ids"]) + (
            UUID(paper_request.command_id),
        )
        assert len(job_ids) == 5 and len(facade.spool.pending()) == 4
        monkeypatch.setattr(
            prepare, "phase_provider", lambda *_: pytest.fail("retry read new data")
        )
        monkeypatch.setattr(
            runs.preparer, "prepare", lambda *_a, **_k: pytest.fail("retry recompiled")
        )
        assert service.submit(registration) == experiment_receipt
        assert (
            service._resume_trusted_paper_portfolio(paper_request, authenticated_actor_id="alice")
            == paper_receipt
        )
        assert scheduler.run_once().plans_created == 4
        for _ in experiment_receipt.result["job_ids"]:
            close_next_job()
        assert completed == set(job_ids)
        assert all(reader.get_job(job_id).result_state.value == "sealed" for job_id in job_ids)
        assert reader.get_job(UUID(paper_request.command_id)).spec == owned.spec
        with pytest.raises(PermissionError):
            private.directory_for_job(UUID(int=999), reader.get_job(job_ids[0]).spec)
        assert facade.spool.pending() == () and claims.pending() == ()
    finally:
        worker.close()
        scheduler.release()


def test_serving_retains_three_owned_groups_and_rejects_partial_or_mixed_generation(
    tmp_path: Path,
) -> None:
    from rquant.experiment_platform_projection import ExperimentPrivateProjectionReader
    from rquant.experiment_registry import ExperimentRegistryReadonlyReader
    from rquant.lab_jobs import LabJobStore
    from rquant.paper_portfolio_projection import paper_portfolio_projections
    from rquant.promotions_serving_authority import PromotionsSourceReader
    from rquant.serving_read_models import (
        ServingProjectionInput,
        ServingReadModelInput,
        build_serving_read_models,
    )
    from tests.unit.test_experiment_platform_templates import admitted, binding_for
    from tests.unit.test_paper_portfolio_projection import publication
    from tests.unit.test_price_alert_runtime_capacity import exact_projection_inputs

    paper_root, experiment_root = tmp_path / "paper", tmp_path / "experiments"
    paper_root.mkdir(mode=0o700)
    experiment_root.mkdir(mode=0o700)
    paper = publication(paper_root)
    selected, _, _, _, request = binding_for(experiment_root)
    store, _ = admitted(experiment_root, selected, request)
    jobs = LabJobStore(experiment_root / "jobs.sqlite")
    jobs.initialize()
    private = ExperimentPrivateProjectionReader(
        registry=ExperimentRegistryReadonlyReader(
            store.registry.path,
            managed_trust_root=store.registry._path_authority._managed_trust_root,
        ),
        jobs=LabJobReader(jobs.path),
        owners=frozenset({"alice", "bob"}),
    )
    source = PromotionsSourceReader(
        registry=private.registry,
        include_experiments=True,
        private_experiment_reader=private,
    )(NOW)
    groups = (
        *(
            ServingProjectionInput.bind(
                p, owner_dataset_id="paper_accounts", owner_generation_id="a" * 64
            )
            for p in paper_portfolio_projections(paper)
        ),
        *(
            ServingProjectionInput.bind(
                p, owner_dataset_id="promotions", owner_generation_id=source.generation_id
            )
            for p in source.payload.projections
        ),
        *exact_projection_inputs(2 * 1024 * 1024),
    )
    model = ServingReadModelInput(observed_at=NOW, projections=groups)
    tables = build_serving_read_models(model)
    assert len(tables["paper_portfolio_account"]) == 1
    assert len(tables["experiment_private_family"]) == 1
    assert len(tables["price_alert_runtime_state"]) == 1
    for missing in (
        "paper_portfolio_state",
        "experiment_private_window",
        "price_alert_runtime_state",
    ):
        with pytest.raises(ValueError):
            ServingReadModelInput(
                observed_at=NOW,
                projections=tuple(p for p in groups if p.table_name != missing),
            )
    changed = tuple(
        p.model_copy(update={"owner_generation_id": "f" * 64})
        if p.table_name == "paper_portfolio_material"
        else p
        for p in groups
    )
    with pytest.raises(ValueError):
        ServingReadModelInput(observed_at=NOW, projections=changed)


def test_configured_paper_role_keeps_price_events_out_of_original_order_queue(
    tmp_path: Path,
) -> None:
    from rquant.paper_signal_consumer import PaperSignalConsumerStateStore
    from rquant.paper_signal_worker import PaperSignalQueueStore
    from rquant.runtime_service_builtin import build_builtin_registry
    from rquant.signal_route_spool import SignalRouteSpool, publish_mixed_notification_bus_prefix
    from tests.unit.test_paper_portfolio_builder import fixture
    from tests.unit.test_price_alert_event_contracts import AT
    from tests.unit.test_price_alert_route_spool import mixed_fixture

    manifest, operator, catalog, control = fixture(tmp_path)
    producer, bus, _, one, price, three = mixed_fixture(tmp_path)
    try:
        publish_mixed_notification_bus_prefix(
            bus=bus,
            spool=SignalRouteSpool(tmp_path / "signal-spool"),
            limit=100,
            observed_at=AT,
        )
        state = PaperSignalConsumerStateStore(Path(manifest.settings["consumer_state_path"]))
        state.install_mixed_notification_history()
        result = build_builtin_registry(
            clock=lambda: AT,
            paper_portfolio_catalog=catalog,
            paper_quote_resolver=lambda *_: pytest.fail("price event reached broker"),
            trade_date_resolver=lambda now: now.date(),
        ).build(manifest)()
        assert result.output_sequence == 3
        assert result.source_generations["paper_operator_control"] == control.fingerprint
        assert operator.current().status == "applied"
        assert state.non_trading_receipt(2).record.event_id == price.event.event_id
        from rquant.runtime_builder_paper import PaperBrokerSettings

        settings = PaperBrokerSettings.model_validate(dict(manifest.settings))
        queue = PaperSignalQueueStore(
            settings.queue_path, policy=settings.signal_policy(manifest.producer_commit)
        )
        assert queue.record(price.event.event_id) is None
        assert queue.record(str(one.signal_id)) is not None
        assert queue.record(str(three.signal_id)) is not None
    finally:
        producer.close()


def test_combined_web_host_keeps_identity_and_all_three_write_caps(tmp_path: Path) -> None:
    from fastapi.testclient import TestClient
    from rquant.web.app import create_app
    from rquant.web.settings import WebSettings
    from tests.support.web_proxy_identity import with_test_proxy_identity

    app = create_app(
        with_test_proxy_identity(WebSettings(serving_root=tmp_path / "serving")),
        background=False,
    )
    with TestClient(app, headers={"x-rquant-user": "alice"}) as client:
        for path in (
            "/api/v1/paper-portfolios",
            "/api/v1/experiments/mine",
            "/api/v1/monitor/price-rules/runtime",
        ):
            assert client.get(path).status_code == 401
        for path, cap in (
            ("/api/v1/experiments/commands", 40 * 1024),
            ("/api/v1/monitor/price-rules/commands", 8 * 1024),
            ("/api/v1/paper-portfolios/paper-main/configuration", 16 * 1024),
            ("/api/v1/paper-portfolios/paper-main/pause/prepare", 4 * 1024),
            ("/api/v1/paper-portfolios/paper-main/pause/confirm", 4 * 1024),
            ("/api/v1/paper-portfolios/paper-main/recover", 16 * 1024),
            ("/api/v1/paper-portfolios/paper-main/reconcile", 4 * 1024),
            ("/api/v1/paper-portfolios/paper-main/band", 4 * 1024),
        ):
            response = client.post(
                path, content=b" " * (cap + 1), headers={"Content-Type": "application/json"}
            )
            assert response.status_code == 413, (path, response.text)
    assert app.state.web.paper_portfolio_gateway is None
    assert app.state.web.experiment_platform is None
