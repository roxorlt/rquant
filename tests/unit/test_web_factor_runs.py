"""First lost-POST replay must find the whole original before Serving preflight."""

import json
import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from threading import Thread
from uuid import uuid4

import pytest

from rquant.factor.registry import FactorHeadRef
from rquant.factor.run_request import (
    FactorRunOperationResult,
    FactorRunParameters,
    FactorRunRequest,
)
from rquant.web.settings import WebSettings
from tests.support.web_proxy_identity import ResearcherTestClient, create_private_test_app
from tests.unit.test_factor_source_prepare import _AS_OF, _FIRST


def _body() -> FactorRunRequest:
    return FactorRunRequest(
        command_id=str(uuid4()),
        requested_at=_AS_OF,
        serving_generation_id="a" * 64,
        parameters=FactorRunParameters(
            factor_id="test_factor",
            expected_head=FactorHeadRef(version=1, content_sha256="b" * 64),
            selection="all",
            start_date=_FIRST,
            end_date=_FIRST + timedelta(days=5),
            holding_sessions=5,
        ),
    )


def test_lost_first_post_replays_without_serving_and_checks_csrf(tmp_path: Path) -> None:
    request = _body()
    result = FactorRunOperationResult(
        original_request=request, status="submitted", job_id="c" * 32, spec_sha256="d" * 64
    )
    calls = []

    class Client:
        def lookup(
            self, body: FactorRunRequest, *, authenticated_actor_id: str
        ) -> FactorRunOperationResult:
            calls.append("lookup")
            assert body == request and authenticated_actor_id == "researcher"
            return result

        def resume(
            self, body: FactorRunRequest, *, authenticated_actor_id: str
        ) -> FactorRunOperationResult:
            calls.append("resume")
            return result

        def submit(self, *args: object, **kwargs: object) -> None:
            raise AssertionError("a replay must not compile")

    settings = WebSettings(
        serving_root=tmp_path / "missing-serving",
        ingress_socket_path=tmp_path / "web" / "app.sock",
        factor_run_enabled=True,
        factor_run_users=frozenset({"researcher"}),
        factor_run_admission_socket_path=tmp_path / "run" / "run.sock",
        factor_run_admission_service_uid=os.geteuid() + 1,
        factor_run_admission_shared_gid=os.getegid(),
    )
    app = create_private_test_app(settings, factor_run_admission_client=Client(), background=False)
    with ResearcherTestClient(app) as client:
        assert (
            client.post("/api/v1/factors/runs", json=request.model_dump(mode="json")).status_code
            == 403
        )
        response = client.post(
            "/api/v1/factors/runs",
            json=request.model_dump(mode="json"),
            headers={"x-rquant-csrf": "1"},
        )
        assert response.status_code == 200, response.text
        assert response.json()["data"]["job_id"] == result.job_id
        assert calls == ["lookup", "resume"]


@pytest.mark.parametrize("selection", ["all", "hs300", "zz1000", "gem"])
def test_actual_files_page_run_worker_serving_and_original_version(
    tmp_path: Path,
    selection: str,
) -> None:
    from rquant.factor.definition_serving import project_factor_definition_serving_snapshot
    from rquant.factor.registry import FactorDefinitionRegistry, SaveFactorDefinitionRequest
    from rquant.factor.result_serving import project_factor_result_projections
    from rquant.factor.run_backend import FactorRunPageControlBackend
    from rquant.factor.run_configuration import (
        open_factor_run_configuration,
        run_configured_factor_worker,
    )
    from rquant.factor.serving_projection import project_factor_definition_projections
    from rquant.factor_run_admission import FactorRunAdmission
    from rquant.page_control import PageControlConsumer, PageControlOutbox, PageControlService
    from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture
    from tests.unit.test_factor_run_configuration import _configured

    root, reference, body = _configured(tmp_path, all_pools=True, selection=selection)
    backend = FactorRunPageControlBackend(root, reference, clock=lambda: _AS_OF)
    config = backend.configuration()
    registry = FactorDefinitionRegistry(Path(config.registry_identity.path))
    definitions = project_factor_definition_projections(
        project_factor_definition_serving_snapshot(
            registry,
            expected_identity=config.registry_identity,
            available_at=FIXTURE_BUILT_AT,
        )
    )
    serving = tmp_path / "serving"
    generation = build_web_fixture(serving, "baseline", factor_definition_projections=definitions)
    body = body.model_copy(update={"serving_generation_id": generation.generation_id})
    outbox = PageControlOutbox(tmp_path / "outbox.sqlite")
    service = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=tmp_path,
            log_dir=tmp_path,
            factor_run_backend=backend,
            clock=lambda: _AS_OF,
        ),
    )
    admission = FactorRunAdmission(service, run_users=frozenset({"alice"}), enabled=True)
    app = create_private_test_app(
        WebSettings(
            serving_root=serving,
            stale_after_seconds=1e9,
            ingress_socket_path=tmp_path / "web-private" / "app.sock",
            factor_run_enabled=True,
            factor_run_users=frozenset({"alice"}),
            factor_run_admission_socket_path=tmp_path / "run-private" / "run.sock",
            factor_run_admission_service_uid=os.geteuid() + 1,
            factor_run_admission_shared_gid=os.getegid(),
        ),
        factor_run_admission_client=admission,
        clock=lambda: FIXTURE_BUILT_AT + timedelta(minutes=1),
        background=False,
    )
    with ResearcherTestClient(app, headers={"x-rquant-user": "alice"}) as client:
        response = client.post(
            "/api/v1/factors/runs",
            json=body.model_dump(mode="json"),
            headers={"x-rquant-csrf": "1"},
        )
        assert response.status_code == 200, response.text
        submitted = response.json()["data"]
        assert submitted["status"] == "submitted"
        assert client.get("/api/v1/factors/results").json()["data"]["availability"] == "unavailable"
        result = run_configured_factor_worker(root, reference, clock=lambda: _AS_OF)
        assert result.status == "succeeded", result
        assert result.job_id == submitted["job_id"]
        with open_factor_run_configuration(root, reference) as loaded:
            jobs = project_factor_result_projections(
                loaded.configuration.ledger_identity,
                loaded.configuration.artifact_root,
                available_at=FIXTURE_BUILT_AT,
                other_projections=definitions,
            )
        old = registry.get_head(
            body.parameters.factor_id, expected_identity=config.registry_identity
        )
        new_definition = old.definition.model_copy(update={"version": 2, "name_zh": "更新后的因子"})
        registry.save(
            SaveFactorDefinitionRequest(
                command_id="update-after-run",
                definition=new_definition,
                expected_head=body.parameters.expected_head,
            ),
            expected_identity=config.registry_identity,
        )
        updated = project_factor_definition_projections(
            project_factor_definition_serving_snapshot(
                registry,
                expected_identity=config.registry_identity,
                available_at=FIXTURE_BUILT_AT,
            )
        )
        build_web_fixture(
            serving,
            "baseline",
            sequence=1,
            factor_definition_projections=updated,
            factor_result_projections=jobs,
        )
        app.state.web.tracker.refresh()
        restored = client.post(
            "/api/v1/factors/runs",
            json=body.model_dump(mode="json"),
            headers={"x-rquant-csrf": "1"},
        )
        assert restored.status_code == 200 and restored.json()["data"] == submitted
        detail = client.get(f"/api/v1/factors/results/{result.job_id}")
        assert detail.status_code == 200, detail.text
        item, research = detail.json()["data"]["result"], detail.json()["data"]["research"]
        assert item["spec_sha256"] == submitted["spec_sha256"]
        assert item["definition_content_sha256"] == body.parameters.expected_head.content_sha256
        assert item["factor_version"] == 1 and item["factor_name_zh"] == "试验因子"
        assert item["definition_status"] == "historical_unavailable"
        assert research["schema_version"] == 2 and research["holding_sessions"] == 5
        assert [day["coverage"]["expected_count"] for day in research["coverage_days"]][-1] == 0
        for private in (
            str(config.lake_root),
            str(config.ledger_identity.path),
            config.registry_identity.instance_id,
        ):
            assert private not in detail.text


@contextmanager
def _real_run_web_client(tmp_path: Path) -> Iterator[tuple[object, ...]]:
    from rquant.factor.definition_serving import project_factor_definition_serving_snapshot
    from rquant.factor.registry import FactorDefinitionRegistry
    from rquant.factor.run_backend import FactorRunPageControlBackend
    from rquant.factor.serving_projection import project_factor_definition_projections
    from rquant.factor_run_admission import (
        FactorRunAdmission,
        FactorRunAdmissionClient,
        build_factor_run_admission_server,
    )
    from rquant.page_control import PageControlConsumer, PageControlOutbox, PageControlService
    from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture
    from tests.unit.test_factor_run_configuration import _configured

    root, reference, body = _configured(tmp_path)
    backend = FactorRunPageControlBackend(root, reference, clock=lambda: _AS_OF)
    config = backend.configuration()
    definitions = project_factor_definition_projections(
        project_factor_definition_serving_snapshot(
            FactorDefinitionRegistry(Path(config.registry_identity.path)),
            expected_identity=config.registry_identity,
            available_at=FIXTURE_BUILT_AT,
        )
    )
    serving = tmp_path / "serving"
    generation = build_web_fixture(serving, "baseline", factor_definition_projections=definitions)
    body = body.model_copy(
        update={
            "serving_generation_id": generation.generation_id,
            "parameters": body.parameters.model_copy(update={"start_date": _FIRST}),
        }
    )
    outbox = PageControlOutbox(tmp_path / "outbox.sqlite")
    service = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=tmp_path,
            log_dir=tmp_path,
            factor_run_backend=backend,
            clock=lambda: _AS_OF,
        ),
    )
    directory = Path(tempfile.mkdtemp(prefix="frfix-", dir="/private/tmp"))
    os.chown(directory, os.geteuid(), os.getegid())
    directory.chmod(0o710)
    socket = directory / "run.sock"
    web_uid = os.geteuid() + 1
    server = build_factor_run_admission_server(
        FactorRunAdmission(service, run_users=frozenset({"alice"}), enabled=True),
        socket_path=socket,
        trusted_web_uid=web_uid,
        shared_gid=os.getegid(),
        peer_uid=lambda _: web_uid,
    )
    assert server is not None
    thread = Thread(target=server.serve_forever, daemon=True, name="factor-run-fix-listener")
    thread.start()
    try:
        admission = FactorRunAdmissionClient(
            socket,
            expected_service_uid=os.geteuid(),
            shared_gid=os.getegid(),
            client_uid=lambda: web_uid,
        )
        app = create_private_test_app(
            WebSettings(
                serving_root=serving,
                stale_after_seconds=1e9,
                ingress_socket_path=tmp_path / "web-private" / "app.sock",
                factor_run_enabled=True,
                factor_run_users=frozenset({"alice"}),
                factor_run_admission_socket_path=socket,
                factor_run_admission_service_uid=web_uid,
                factor_run_admission_shared_gid=os.getegid(),
            ),
            factor_run_admission_client=admission,
            clock=lambda: FIXTURE_BUILT_AT + timedelta(minutes=1),
            background=False,
        )
        with ResearcherTestClient(app, headers={"x-rquant-user": "alice"}) as client:
            yield client, body, backend, outbox, server
    finally:
        server.shutdown()
        thread.join(timeout=3)
        server.server_close()
        assert not thread.is_alive() and not socket.exists()
        directory.rmdir()


def test_real_unix_web_plan_rejection_is_bound_and_editable(tmp_path: Path) -> None:
    from rquant.factor.run_configuration import open_factor_run_configuration

    responses = {}
    with _real_run_web_client(tmp_path) as (client, body, backend, outbox, _):
        availability = client.get("/api/v1/factors/run-availability").json()["data"]
        for name, path in (("submit", "runs"), ("resume", "runs/resume"), ("retry", "runs/retry")):
            response = client.post(
                f"/api/v1/factors/{path}",
                json=body.model_dump(mode="json"),
                headers={"x-rquant-csrf": "1"},
            )
            responses[name] = {"status": response.status_code, "body": response.json()}
        for name in ("submit", "retry"):
            assert responses[name]["status"] == 200, responses[name]
            result = FactorRunOperationResult.model_validate(responses[name]["body"]["data"])
            assert result.original_request == body and result.status == "rejected"
            assert result.reason == "开始日期前的历史数据不足，请调整开始日期。"
            assert result.job_id is None and result.spec_sha256 is None
        assert responses["resume"]["status"] == 404
        assert outbox.receipt(body.command_id) is None
        with open_factor_run_configuration(backend.root, backend.reference) as loaded:
            assert loaded.open_ledger(clock=lambda: _AS_OF).list_recent() == ()
            assert not list(loaded.configuration.lake_root.glob(".execution_sessions/*"))
        exported = {
            "actor": "alice",
            "request": body.model_dump(mode="json"),
            "availability": availability,
            "responses": responses,
            "context": (
                "Synthetic real factory, PageControl, Unix transport and authenticated Web. "
                "UID seam is the existing test fixture; not installed dual-UID proof."
            ),
        }
    exported["resources"] = {
        "listener_joined": True,
        "socket_removed": True,
        "execution_copies": 0,
        "jobs": 0,
        "outbox_receipt": None,
    }
    Path("/private/tmp/rquant-factor-run-entry-fr-final-01-http-contract.json").write_text(
        json.dumps(exported, ensure_ascii=False, indent=2)
    )


@pytest.mark.parametrize(
    "mode", ["existing_ledger_command", "changed_ledger_identity", "peer_unavailable"]
)
def test_real_unix_web_unknown_or_existing_work_never_becomes_plan_rejection(
    tmp_path: Path,
    mode: str,
) -> None:
    from rquant.factor.run_configuration import open_factor_run_configuration

    with _real_run_web_client(tmp_path) as (client, body, backend, outbox, server):
        with open_factor_run_configuration(backend.root, backend.reference) as loaded:
            config = loaded.configuration
            ledger = loaded.open_ledger(clock=lambda: _AS_OF)
        if mode == "existing_ledger_command":
            valid = body.model_copy(
                update={
                    "parameters": body.parameters.model_copy(
                        update={
                            "start_date": _FIRST + timedelta(days=5),
                        }
                    )
                }
            )
            plan = backend.compile(
                valid, verified_registry_instance_id=config.registry_identity.instance_id
            )
            ledger.submit(body.command_id, plan.spec)
        elif mode == "changed_ledger_identity":
            Path(config.ledger_identity.path).rename(tmp_path / "original-ledger.sqlite")
            Path(config.ledger_identity.path).touch(mode=0o600)
        else:
            server.peer_uid = lambda _: -1
        response = client.post(
            "/api/v1/factors/runs",
            json=body.model_dump(mode="json"),
            headers={"x-rquant-csrf": "1"},
        )
        if mode == "existing_ledger_command":
            assert response.status_code == 409
            assert len(ledger.list_recent()) == 1
        else:
            assert response.status_code == 200
            result = FactorRunOperationResult.model_validate(response.json()["data"])
            assert result.status == "uncertain" and result.original_request == body
        assert outbox.receipt(body.command_id) is None
        assert not list(config.lake_root.glob(".execution_sessions/*"))
