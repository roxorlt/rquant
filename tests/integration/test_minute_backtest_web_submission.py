from __future__ import annotations

import json
import os
import time
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from rquant.web.app import create_app
from rquant.web.models.minute_backtests import MinuteCreateRequest
from rquant.web.settings import WebSettings
from tests.unit.test_minute_backtest_commands import configuration
from tests.unit.test_minute_backtest_producer import NOW
from tests.support.minute_backtest_installed import installed_minute


def test_actual_cold_openapi_registers_minute_routes_without_reading_private_installation(tmp_path: Path) -> None:
    app = create_app(WebSettings(serving_root=tmp_path / "serving",
        minute_replay_installation=tmp_path / "does-not-exist.json", minute_replay_expected_code_sha="a" * 40), background=False)
    paths = app.openapi()["paths"]
    for suffix in ("capabilities", "sources", "runs", "runs/{job_id}", "runs/{job_id}/nav", "runs/{job_id}/rows",
        "runs/{job_id}/report.html", "runs/{job_id}/exports/{request_id}.zip", "exports"):
        assert "/api/v1/backtests/minute-runtime/" + suffix in paths
    assert "post" in paths["/api/v1/backtests/minute-runtime/runs"]
    assert "post" in paths["/api/v1/backtests/minute-runtime/exports"]
    assert "MinuteReplayPerformance" in app.openapi()["components"]["schemas"]
    assert not (tmp_path / "does-not-exist.json").exists()


def test_actual_asgi_export_wire_requires_original_csrf_identity_and_bounded_strict_body(wire_client: object) -> None:
    from rquant.web.models.minute_backtests import MinuteExportRequest
    body = MinuteExportRequest(command_id=uuid4(), requested_at=NOW, job_id=uuid4(), result_hash="a" * 64).model_dump(mode="json")
    route = "/api/v1/backtests/minute-runtime/exports"
    assert wire_client.post(route, json=body).status_code == 403
    assert wire_client.post(route, json=body, headers={"X-Rquant-Csrf": "1", "Sec-Fetch-Site": "cross-site"}).status_code == 403
    assert wire_client.post(route, json=body, headers={"X-Rquant-Csrf": "1", "x-rquant-proxy-proof": "untrusted"}).status_code == 401
    valid = wire_client.post(route, json=body, headers={"X-Rquant-Csrf": "1"})
    assert valid.status_code == 503 and valid.json()["detail"] == "尚无已安装的分钟回测来源。"
    for field in ("actor_id", "source_path", "receipt_path", "export_path", "hold_days"):
        assert wire_client.post(route, json=body | {field: "ignored"}, headers={"X-Rquant-Csrf": "1"}).status_code == 422
    raw = json.dumps(body)
    assert wire_client.post(route, content=raw.replace('"result_hash":', '"result_hash":"b", "result_hash":'),
        headers={"X-Rquant-Csrf": "1", "Content-Type": "application/json"}).status_code == 422
    assert wire_client.post(route, content=raw + " " * (32 * 1024),
        headers={"X-Rquant-Csrf": "1", "Content-Type": "application/json"}).status_code == 413


@pytest.mark.parametrize("field", ["actor_id", "source_path", "receipt_path", "hold_days", "entry_mode", "ablation"])
def test_new_web_request_cannot_import_legacy_or_private_authority_fields(field: str) -> None:
    raw = {"command_id": uuid4(), "requested_at": NOW, "config": configuration()}
    with pytest.raises(ValueError):
        MinuteCreateRequest.model_validate(raw | {field: "ignored"})


@pytest.fixture
def wire_client(tmp_path: Path) -> Iterator[object]:
    from tests.support.web_proxy_identity import ProofTestClient, with_test_proxy_identity

    settings = with_test_proxy_identity(WebSettings(serving_root=tmp_path / "serving",
        ingress_socket_path=tmp_path / "unused-ingress.sock", lab_control_users=frozenset({"researcher"})))
    with ProofTestClient(create_app(settings, background=False), headers={"x-rquant-user": "researcher"}) as client:
        yield client


def wire_request() -> dict[str, object]:
    return MinuteCreateRequest(command_id=uuid4(), requested_at=NOW, config=configuration()).model_dump(mode="json")


def test_actual_asgi_valid_json_dates_uuid_reach_original_service_gate(wire_client: object) -> None:
    assert MinuteCreateRequest.model_validate_json(json.dumps(wire_request())).config == configuration()
    assert MinuteCreateRequest.model_validate(wire_request()).config == configuration()
    response = wire_client.post("/api/v1/backtests/minute-runtime/runs", json=wire_request(), headers={"X-Rquant-Csrf": "1"})
    assert response.status_code == 503, response.text
    assert response.json()["detail"] == "尚无已安装的分钟回测来源。"


@pytest.mark.parametrize("suffix", ["capabilities", "sources", "runs"])
def test_actual_asgi_uninstalled_get_uses_the_original_unavailable_envelope(wire_client: object, suffix: str) -> None:
    response = wire_client.get("/api/v1/backtests/minute-runtime/" + suffix)
    assert response.status_code == 200, response.text
    assert response.json()["data"]["available"] is False
    assert response.json()["serving"]["state"] == "unavailable"


@pytest.mark.parametrize("mutation", ["duplicate", "boolean-version", "unknown-config"])
def test_actual_asgi_raw_wire_rejects_ambiguous_or_unsupported_input(wire_client: object, mutation: str) -> None:
    body = wire_request()
    if mutation == "boolean-version":
        body["config"]["native_version"] = True
    if mutation == "unknown-config":
        body["config"]["hold_days"] = 3
    raw = json.dumps(body)
    if mutation == "duplicate":
        raw = raw.replace('"source_version": 1', '"source_version": 2, "source_version": 1')
    response = wire_client.post("/api/v1/backtests/minute-runtime/runs", content=raw,
        headers={"X-Rquant-Csrf": "1", "Content-Type": "application/json"})
    assert response.status_code == 422, response.text


def test_actual_asgi_wire_budget_and_original_csrf_and_identity(wire_client: object) -> None:
    raw = json.dumps(wire_request())
    route = "/api/v1/backtests/minute-runtime/runs"
    assert wire_client.post(route, content=raw, headers={"Content-Type": "application/json"}).status_code == 403
    assert wire_client.post(route, content=raw, headers={"Content-Type": "application/json", "X-Rquant-Csrf": "1",
        "Sec-Fetch-Site": "cross-site"}).status_code == 403
    assert wire_client.post(route, content=raw, headers={"Content-Type": "application/json", "X-Rquant-Csrf": "1",
        "x-rquant-proxy-proof": "untrusted"}).status_code == 401
    assert wire_client.post(route, content=raw + " " * (32 * 1024),
        headers={"Content-Type": "application/json", "X-Rquant-Csrf": "1"}).status_code == 413


def test_actual_installed_source_gate_and_owner_selection(installed_minute: SimpleNamespace) -> None:
    from rquant.web.minute_backtest_service import MinuteWebService

    context = installed_minute
    frozen = context.published.receipt.frozen
    service = MinuteWebService(context.readonly)
    selected = service.sources(owner_id=frozen.runtime.owner_id)
    assert selected.available and selected.unavailable_count == 0
    source, = selected.sources
    assert source.full_input_hash == frozen.full_input_hash
    assert source.core_input_hash == frozen.core_input_hash
    assert source.seed_hash == context.published.receipt.seed.seed_hash
    assert (source.native_id, source.native_version) == (frozen.runtime.strategy.strategy_id, frozen.runtime.strategy.strategy_version)
    assert source.provenance.source_kind == "reconstructed"
    assert source.provenance.real_capture_times == ()
    assert "Synthetic parity fixture only" in source.provenance.visibility_limitations
    assert service.sources(owner_id="other-owner").sources == ()
    assert context.sealer_calls == ["recover", "begin", "commit"]


def native_configuration(context: SimpleNamespace) -> object:
    from rquant.experiment_registry import DateRange
    from rquant.minute_backtest_commands import MinuteRunConfig
    from rquant.minute_backtest_formal import MinuteExperimentProtocol

    frozen = context.published.receipt.frozen
    return MinuteRunConfig(source_key=frozen.runtime.source_key, source_version=frozen.runtime.source_version,
        full_input_hash=frozen.full_input_hash, native_id=frozen.runtime.strategy.strategy_id,
        native_version=frozen.runtime.strategy.strategy_version, deadline=NOW + timedelta(hours=1),
        protocol=MinuteExperimentProtocol(
            train_range=DateRange(start_date=frozen.runtime.start_date - timedelta(days=2), end_date=frozen.runtime.start_date - timedelta(days=2)),
            validation_range=DateRange(start_date=frozen.runtime.start_date - timedelta(days=1), end_date=frozen.runtime.start_date - timedelta(days=1)),
            frozen_outer_test_range=DateRange(start_date=frozen.runtime.start_date, end_date=frozen.runtime.end_date)))


@pytest.fixture(scope="module")
def web_minute(installed_minute: SimpleNamespace) -> Iterator[SimpleNamespace]:
    from rquant.collaboration_commands import CommandAuthorization, PageControlRoleAuthority
    from rquant.collaboration_roles import RoleEntry, RoleState
    from rquant.minute_backtest_commands import MinuteCommandWriter
    from rquant.page_control import PageControlConsumer, PageControlOutbox, PageControlService, parse_page_control_command
    from rquant.web.collaboration_gateway import CollaborationGateway
    from tests.support.web_proxy_identity import ProofTestClient, with_test_proxy_identity

    context = installed_minute
    owner = context.published.receipt.frozen.runtime.owner_id
    roles_path = context.root / "roles.json"
    roles_path.write_text(RoleState.create(revision=1, users=(RoleEntry(username=owner, role="researcher"),
        RoleEntry(username="other-owner", role="researcher"), RoleEntry(username="viewer", role="viewer"),
        RoleEntry(username="admin", role="admin"))).model_dump_json())
    roles_path.chmod(0o600)
    role_authority = PageControlRoleAuthority(mode="enforced", roles_path=roles_path, clock=lambda: context.now[0])
    outbox = PageControlOutbox(context.root / "page-control.sqlite3")
    outbox.path.chmod(0o600)
    backend = MinuteCommandWriter(context.writer)
    consumer = PageControlConsumer(outbox=outbox, data_dir=context.root / "page-data", log_dir=context.root / "page-logs",
        minute_backend=backend, clock=lambda: context.now[0])
    control = PageControlService(outbox=outbox, consumer=consumer, collaboration=role_authority)
    posted: list[dict[str, object]] = []

    def transport(message: dict[str, object]) -> dict[str, object]:
        assert set(message) == {"command", "authorization"}
        command = parse_page_control_command(message["command"])
        posted.append(command.model_dump(mode="json"))
        proof = CommandAuthorization.model_validate_json(json.dumps(message["authorization"]))
        from rquant.page_control import PageControlCommandConflictError
        from rquant.web.lab_control_gateway import LabControlConflictError
        try:
            return control.submit_authorized(command, proof).model_dump(mode="json")
        except PageControlCommandConflictError as error:
            # Original PageControl HTTP ingress maps this exact exception to
            # 409; the socket-free transport keeps that same boundary.
            raise LabControlConflictError("PageControl command ID conflicts") from error

    def collaboration_transport(message: object) -> bytes:
        return control.collaboration_request(message).model_dump_json().encode()

    collaboration = CollaborationGateway(Path("/private/tmp") / ("c6-unused-" + uuid4().hex + ".sock"),
        expected_service_uid=0 if os.geteuid() != 0 else 1, shared_gid=os.getegid(), transport=collaboration_transport)
    settings = with_test_proxy_identity(WebSettings(serving_root=context.root / "serving", collaboration_mode="enforced",
        ingress_socket_path=context.root / "unused-web-ingress.sock",
        lab_control_users=frozenset({owner, "other-owner", "viewer"}), minute_replay_installation=context.path,
        minute_replay_expected_code_sha=context.profile.code_sha))
    app = create_app(settings, clock=lambda: context.now[0], background=False,
        lab_control_command_transport=transport, collaboration_gateway=collaboration)
    try:
        with ProofTestClient(app, headers={"x-rquant-user": owner}) as client:
            yield SimpleNamespace(context=context, owner=owner, control=control, backend=backend, outbox=outbox,
                consumer=consumer, roles=role_authority, roles_path=roles_path, client=client, posted=posted, app=app)
    finally:
        backend.close()


@pytest.fixture(scope="module")
def sealed_web_minute(web_minute: SimpleNamespace) -> Iterator[SimpleNamespace]:
    from rquant.lab_artifact_protocol import LabArtifactCommitSpool, LabFinalizerAuthorityKey
    from rquant.lab_artifacts import LabJobArtifactStore
    from rquant.lab_finalizer import LabFinalizer
    from rquant.lab_job_protocol import LabCommandSpool
    from rquant.lab_jobs import JobStatus, LabResultState
    from rquant.lab_shard_protocol import LabClaimSpool, LabReportSpool, LabShardSucceeded, LabShardTelemetry, LabWorkerReport
    from rquant.lab_worker import LabWorker
    from rquant.lab_worker_registry import execute_builtin_lab_shard
    from tests.unit.test_minute_backtest_producer import original_fixture

    web, context = web_minute, web_minute.context
    create = MinuteCreateRequest(command_id=uuid4(), requested_at=NOW, config=native_configuration(context))
    first = web.client.post("/api/v1/backtests/minute-runtime/runs", json=create.model_dump(mode="json"), headers={"X-Rquant-Csrf": "1"})
    assert first.status_code == 200, first.text
    receipt = first.json()
    assert receipt["status"] == "submitted", receipt
    from uuid import UUID
    job_id = UUID(receipt["job_id"])
    again = web.client.post("/api/v1/backtests/minute-runtime/runs", json=create.model_dump(mode="json"), headers={"X-Rquant-Csrf": "1"})
    assert again.status_code == 200 and again.json() == receipt
    assert web.posted == [web.posted[0], web.posted[0]]
    spool = LabCommandSpool(context.profile.command_spool_path)
    entry, = spool.pending()
    assert entry.envelope.command.job_id == job_id
    lease = context.jobs.acquire_scheduler_lease(owner_id="synthetic-installed-minute-scheduler", lease_seconds=3600, now=NOW)
    accepted = context.jobs.apply_command(entry.envelope, lease=lease, now=NOW,
        submission_authority=lambda envelope, at: context.writer.commands.validate_prepared_experiment_submission(envelope, observed_at=at))
    assert accepted.status == "applied"
    spool.ack(entry, accepted)
    spec = entry.envelope.command.spec
    context.jobs.plan_job(job_id, context.writer.registry.plan(spec), lease=lease, now=NOW)
    claim = context.jobs.claim_next_shard(worker_id="synthetic-installed-minute-worker", shard_lease_seconds=3600, lease=lease, now=NOW)
    assert claim is not None and claim.job_id == job_id
    validated = context.writer.registry.validate_claim(claim)
    started = time.monotonic()
    configuration = json.loads(context.writer.shard_manifest.registry.configuration_json)
    actual = execute_builtin_lab_shard(configuration, validated, runtime_code_sha=spec.code_sha)
    finished = time.monotonic()
    assert [(a.adapter_id, a.adapter_version) for a in context.writer.registry.closed_descriptor().adapters][-1] == ("minute-runtime-replay", "2")
    print("minute_web: actual installed adapter produced nonzero original result", flush=True)
    context.now[0] = NOW + timedelta(seconds=1)
    before = web.client.get(f"/api/v1/backtests/minute-runtime/runs/{job_id}")
    assert before.status_code == 200 and before.json()["data"]["result_hash"] is None
    commits = LabArtifactCommitSpool(context.root / "artifact-commits")
    key = LabFinalizerAuthorityKey(key_id="synthetic-offline-minute-key", secret=b"synthetic-offline-test-key-only-32bytes")
    claims = LabClaimSpool(context.root / "claims")
    reports = LabReportSpool(context.root / "reports")
    claims.publish(claim)
    artifacts = LabJobArtifactStore(context.profile.final_artifact_root)
    worker = None
    try:
        worker = LabWorker(worker_id=claim.worker_id, claim_spool=claims, report_spool=reports,
            artifact_root=context.root / "shard-artifacts", shard_runtime_manifest=context.writer.shard_manifest,
            verified_code_sha_provider=lambda: spec.code_sha, clock=lambda: context.now[0])
        assert worker.adapter_registry.closed_descriptor() == context.writer.registry.closed_descriptor()
        manifest = worker._seal_result(claim, actual)
        telemetry = LabShardTelemetry.from_work_plan(claim.definition.work_plan, monotonic_started=started, monotonic_finished=finished)
        report = LabWorkerReport.from_claim(claim, report_id=uuid4(), reported_at=context.now[0],
            body=LabShardSucceeded.current(result_manifest_hash=manifest.manifest_hash, worker_code_sha=spec.code_sha, telemetry=telemetry))
        report_entry = reports.publish(report)
        success = context.jobs.apply_worker_report(report, lease=lease, now=context.now[0])
        assert success.status == "accepted"
        reports.ack(report_entry, success)
        assert context.writer.reader.get_artifact_preview_authority(job_id) is None
        finalizer = LabFinalizer(reader=context.writer.reader, shard_artifact_root=worker.artifact_root, artifact_store=artifacts,
            commit_spool=commits, verified_code_sha_provider=lambda: spec.code_sha, finalizer_authority_key_provider=lambda: key,
            adapter_registry=context.writer.registry)
        finalized = finalizer.finalize(job_id)
        assert finalized.status == "published"
        print("minute_web: actual original finalizer published physical seal", flush=True)
        context.now[0] = NOW + timedelta(seconds=2)
        commit, = commits.pending()
        with artifacts.bind_verified_sealed(commit.envelope.commit.sealed_path, indexed_at=context.now[0]) as binding:
            with context.jobs.stage_artifact_commit(commit.envelope, binding, authority_key_provider=lambda key_id: key if key_id == key.key_id else None,
                lease=lease, now=context.now[0]) as staged:
                committed = staged.commit(lease=lease, now=context.now[0])
        assert committed.status == "accepted"
        commits.ack(commit, committed)
        job = context.writer.reader.get_job(job_id)
        assert job.status is JobStatus.SUCCEEDED and job.result_state is LabResultState.SEALED
        context.now[0] = NOW + timedelta(minutes=1)
        service = web.app.state.web.minute_backtests.load()
        full = service.read_result(job_id, owner_id=web.owner)
        original = original_fixture()
        for field in original["zero_tolerance_fields"]:
            assert full.result.replay.model_dump(mode="json")[field] == original["minute_replay"][field]
        (context.root / "sealed-web-result.json").write_text(full.model_dump_json(exclude_computed_fields=True))
        yield SimpleNamespace(web=web, context=context, create=create, job_id=job_id, spec=spec, full=full, service=service,
            finalized=finalized, committed=committed)
    finally:
        if worker is not None:
            worker.close()
        artifacts.close()


def test_actual_asgi_pagecontrol_installed_worker_finalizer_and_full_eight_tables(sealed_web_minute: SimpleNamespace) -> None:
    case = sealed_web_minute
    response = case.web.client.get(f"/api/v1/backtests/minute-runtime/runs/{case.job_id}")
    assert response.status_code == 200, response.text
    summary = response.json()["data"]
    assert summary["job"]["status"] == "completed"
    assert (summary["signal_count"], summary["order_count"], summary["fill_count"]) == (3, 2, 2)
    assert len(summary["tables"]) == 8
    nav = case.web.client.get(f"/api/v1/backtests/minute-runtime/runs/{case.job_id}/nav", params={"result_hash": case.full.complete_result_hash})
    assert nav.status_code == 200, nav.text
    points = nav.json()["data"]["points"]
    assert len(points) == 2 and all(point["status"] == "complete" for point in points)
    assert [point["nav"] for point in points] == [str(day.account.nav) for day in case.full.result.replay.daily_valuations]
    assert all(point["basis"] == "pit_asof_15:00" for point in points)
    assert points[0]["price_times"][0]["event_time"] == case.full.result.replay.daily_valuations[0].price_proofs[0].quote.event_time.isoformat().replace("+00:00", "Z")
    for table in summary["tables"]:
        rows = case.web.client.get(f"/api/v1/backtests/minute-runtime/runs/{case.job_id}/rows",
            params={"result_hash": case.full.complete_result_hash, "table": table, "limit": 2})
        assert rows.status_code == 200, (table, rows.text)
        assert rows.json()["data"]["table"] == table
        assert rows.json()["data"]["rows"]
    prefix = f"/api/v1/backtests/minute-runtime/runs/{case.job_id}"
    for actor in ("other-owner", "viewer", "admin"):
        assert case.web.client.get(prefix, headers={"x-rquant-user": actor}).status_code == 404
        assert case.web.client.get(prefix + "/nav", params={"result_hash": case.full.complete_result_hash},
            headers={"x-rquant-user": actor}).status_code == 404
    assert case.web.client.get(prefix + "/nav", params={"result_hash": "0" * 64}).status_code == 409
    assert case.web.client.get(prefix + "/rows", params={"result_hash": case.full.complete_result_hash,
        "table": "fills", "limit": 51}).status_code == 422
    assert case.web.client.post("/api/v1/backtests/minute-runtime/runs", json=case.create.model_dump(mode="json"),
        headers={"X-Rquant-Csrf": "1", "x-rquant-user": "viewer"}).status_code == 403
    altered = case.create.model_dump(mode="json")
    altered["config"]["random_seed"] += 1
    conflict = case.web.client.post("/api/v1/backtests/minute-runtime/runs", json=altered, headers={"X-Rquant-Csrf": "1"})
    assert conflict.status_code == 409 and conflict.json()["status"] == "conflict"
    assert len(case.web.posted) == 3 and case.web.posted[-1]["command_id"] == str(case.create.command_id)
