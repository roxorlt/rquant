"""First lost-POST replay must find the whole original before Serving preflight."""

import os
from datetime import timedelta
from pathlib import Path
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
