"""Original private Web identity and actual task journal admission boundaries."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from tests.support.web_proxy_identity import ProofTestClient, with_test_proxy_identity
from tests.unit.test_task_control_admission import NOW, REQUEST, control_service, request

PREFIX = "/api/v1/tasks"
WRITE = {"x-rquant-csrf": "1"}


def setup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, scheduler: bool = False):
    from rquant.serving_read_models import ServingProjectionInput
    from rquant.ops_status_serving import ops_status_projections
    from rquant.task_center_projection import TaskOpsEvidence, TaskOpsSample
    from rquant.task_control_admission import TaskControlAdmission
    from rquant.web.app import create_app
    from rquant.web.settings import WebSettings
    from tests.support import web_serving_fixture as fixture

    service, backend, source, _executor, calls = control_service(tmp_path, monkeypatch)
    view = source.read(generation_id="generation-a")
    task = TaskOpsSample(snapshot=view.snapshot, evidence=TaskOpsEvidence(cpu=None, cpu_unavailable_reason="capture_unavailable"))
    original = fixture._projections
    control_projection = ()
    if scheduler:
        from rquant.lab_job_center import LabCommandSubmissionFacade
        from rquant.lab_job_protocol import LabCommandSpool
        from rquant.lab_jobs import LabJobReader
        from rquant.task_center_projection import scheduling_projection
        from tests.unit.test_lab_scheduling_control import store_and_port

        (tmp_path / "lab").mkdir(mode=0o700)
        store, port = store_and_port(tmp_path / "lab")
        lease = store.acquire_scheduler_lease(owner_id="scheduler", lease_seconds=120, now=NOW)
        state = store.enable_scheduling_control(lease=lease, barrier_port=port, now=NOW)
        backend.lab_facade = LabCommandSubmissionFacade(reader=LabJobReader(store.path), spool=LabCommandSpool(tmp_path / "commands"), clock=lambda: NOW)
        control_projection = (scheduling_projection(state, cutoff=NOW),)
    monkeypatch.setattr(fixture, "_DATASETS", (*fixture._DATASETS, "ops_status"))
    monkeypatch.setattr(fixture, "fixture_built_at", lambda _: NOW + timedelta(seconds=40))

    def projections(*args: object, **kwargs: object):
        return original(*args, **kwargs) + tuple(ServingProjectionInput.bind(p, owner_dataset_id="ops_status", owner_generation_id=kwargs["generations"]["ops_status"]) for p in ops_status_projections(task)) + tuple(ServingProjectionInput.bind(p, owner_dataset_id="lab_jobs", owner_generation_id=kwargs["generations"]["lab_jobs"]) for p in control_projection)

    monkeypatch.setattr(fixture, "_projections", projections)
    published = fixture.build_web_fixture(source.root, "baseline")
    monkeypatch.delattr(source, "read")
    settings = with_test_proxy_identity(WebSettings(serving_root=source.root))
    settings = WebSettings.model_validate(settings.model_dump() | {"task_control_enabled": True, "task_unit_run_users": {"alice"}, "task_scheduling_admin_users": {"admin"}})
    clock = [NOW + timedelta(seconds=40)]
    backend.clock = source.clock = service.consumer.clock = lambda: clock[0]
    app = create_app(settings, task_control_gateway=TaskControlAdmission(service, backend=backend), clock=lambda: clock[0], background=False)
    body = request(generation_id=published.generation_id)
    return app, service, backend, source, calls, body, clock


def test_tsc_01_task_web_settings_default_off_and_exact_private_roles(tmp_path: Path) -> None:
    from rquant.web.settings import WebSettings

    settings = WebSettings(serving_root=tmp_path)
    assert settings.task_control_enabled is False and not settings.task_unit_run_users and not settings.task_scheduling_admin_users
    with pytest.raises(ValueError, match="private"):
        WebSettings(serving_root=tmp_path, task_control_enabled=True, task_unit_run_users={"alice"})


def test_tsc_01_actual_web_original_run_lookup_resume_and_no_internal_identity(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    app, service, backend, source, calls, body, clock = setup(tmp_path, monkeypatch)
    assert app.state.web.task_control_gateway.capabilities(authenticated_actor_id="alice", generation_id=body.generation_id).units
    with ProofTestClient(app, headers={"x-rquant-user": "alice"}) as client:
        capabilities = client.get(PREFIX + "/control-capabilities").json()
        assert capabilities["units"][0]["unit"] == body.unit
        assert capabilities["units"][0]["can_request"] and not capabilities["can_control_scheduling"]
        response = client.post(PREFIX + f"/units/{body.unit}/run", json=body.model_dump(mode="json"), headers=WRITE)
        assert response.status_code == 200, response.text
        assert response.json()["status"] == "unknown" and calls == [REQUEST]
        assert "inode" not in response.text and "policy_digest" not in response.text and "control.db" not in response.text
        backend.enabled = False
        clock[0] += timedelta(days=1)
        monkeypatch.setattr(source, "read", lambda **_: pytest.fail("original lookup cannot read current stale source"))
        for path in ("/controls/lookup", "/controls/resume"):
            result = client.post(PREFIX + path, json=body.model_dump(mode="json"), headers=WRITE)
            assert result.status_code == 200 and result.json()["status"] == "unknown"
        assert calls == [REQUEST] and service.outbox.receipt(REQUEST) is not None


def test_tsc_01_web_identity_csrf_role_and_url_body_unit_rejection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    app, service, _backend, _source, calls, body, _clock = setup(tmp_path, monkeypatch)
    raw = body.model_dump(mode="json")
    with ProofTestClient(app, headers={"x-rquant-user": "alice"}) as client:
        assert client.post(PREFIX + f"/units/{body.unit}/run", json=raw).status_code == 403
        assert client.post(PREFIX + "/units/rquant-daily.service/run", json=raw, headers=WRITE).status_code == 422
        assert client.post(PREFIX + f"/units/{body.unit}/run", json=raw | {"owner_id": "alice"}, headers=WRITE).status_code == 422
        assert client.post(PREFIX + "/scheduling/commands", json={"kind": "set_lab_scheduling_paused", "command_id": body.command_id, "requested_at": NOW.isoformat(), "generation_id": body.generation_id, "expected_version": 0, "paused": True}, headers=WRITE).status_code == 403
    with TestClient(app) as client:
        assert client.post(PREFIX + f"/units/{body.unit}/run", json=raw, headers=WRITE | {"x-rquant-user": "alice"}).status_code == 401
    assert calls == [] and service.outbox.receipt(REQUEST) is None


def test_tsc_02_web_caps_duplicate_keys_and_changed_generation_before_effect(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    app, service, _backend, _source, calls, body, _clock = setup(tmp_path, monkeypatch)
    with ProofTestClient(app, headers={"x-rquant-user": "alice"}) as client:
        for endpoint, maximum in ((f"/units/{body.unit}/run", 4096), ("/scheduling/commands", 1024), ("/controls/lookup", 4096)):
            assert client.post(PREFIX + endpoint, content=b" " * (maximum + 1), headers=WRITE | {"Content-Type": "application/json"}).status_code == 413
        duplicate = '{"command_id":"x","command_id":"y"}'
        assert client.post(PREFIX + "/controls/lookup", content=duplicate, headers=WRITE | {"Content-Type": "application/json"}).status_code == 422
        altered = body.model_dump(mode="json") | {"generation_id": "b" * 64}
        assert client.post(PREFIX + f"/units/{body.unit}/run", json=altered, headers=WRITE).status_code == 409
    assert calls == [] and service.outbox.receipt(REQUEST) is None


def test_tsc_05_web_prepare_is_an_actual_separate_original_request_before_writer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant.task_control_commands import PrepareUnitRun, TaskUnitRunDraft
    from tests.unit.test_ops_status import _manifest
    from tests.unit.test_task_control_admission import PREPARE
    from tests.unit.test_task_unit_control import policy

    app, _service, backend, _source, calls, body, _clock = setup(tmp_path, monkeypatch)
    monkeypatch.setattr(backend.executor, "configuration", lambda: (_manifest(), policy(mode="writer")))
    draft = TaskUnitRunDraft.model_validate(body.model_dump(exclude={"kind", "confirmation_id"}))
    prepare = PrepareUnitRun(command_id=PREPARE, requested_at=NOW, generation_id=body.generation_id, run=draft)
    with ProofTestClient(app, headers={"x-rquant-user": "alice"}) as client:
        response = client.post(PREFIX + f"/units/{body.unit}/run/prepare", json=prepare.model_dump(mode="json"), headers=WRITE)
        assert response.status_code == 200 and response.json()["status"] == "prepared", response.text
        assert response.json()["original_request"] == prepare.model_dump(mode="json") and calls == []
        confirmed = type(body).model_validate(body.model_dump() | {"confirmation_id": response.json()["confirmation_id"]})
        result = client.post(PREFIX + f"/units/{body.unit}/run", json=confirmed.model_dump(mode="json"), headers=WRITE)
        assert result.status_code == 200 and result.json()["status"] == "unknown" and calls == [REQUEST]


def test_tsc_08_web_global_desired_waits_for_original_scheduler_and_published_application(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from uuid import UUID
    from rquant.task_control_commands import SetLabSchedulingPaused

    app, _service, backend, _source, calls, unit_body, _clock = setup(tmp_path, monkeypatch, scheduler=True)
    body = SetLabSchedulingPaused(command_id=REQUEST, requested_at=NOW, generation_id=unit_body.generation_id, expected_version=0, paused=True)
    with ProofTestClient(app, headers={"x-rquant-user": "admin"}) as client:
        capabilities = client.get(PREFIX + "/control-capabilities").json()
        assert capabilities["can_control_scheduling"] and capabilities["units"] == []
        assert capabilities["scheduling"]["desired_version"] == 0
        result = client.post(PREFIX + "/scheduling/commands", json=body.model_dump(mode="json"), headers=WRITE)
        assert result.status_code == 200 and result.json()["status"] == "submitted", result.text
        assert result.json()["message"] == "请求已受理，等待调度应用。" and calls == []
        entry = backend.lab_facade.spool.find(UUID(body.command_id))
        assert entry.envelope.command.expected_version == 0
        assert backend.lab_facade.reader.scheduling_state().desired_version == 0
        overview = client.get(PREFIX + "/overview").json()["data"]
        assert overview["scheduling"]["applied_version"] == 0 and overview["scheduling"]["applied_paused"] is False


def test_tsc_06_caps_exclude_same_boot_unknown_request_before_a_new_uuid(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    app, _service, _backend, _source, calls, body, _clock = setup(tmp_path, monkeypatch)
    with ProofTestClient(app, headers={"x-rquant-user": "alice"}) as client:
        result = client.post(PREFIX + f"/units/{body.unit}/run", json=body.model_dump(mode="json"), headers=WRITE)
        assert result.json()["status"] == "unknown" and calls == [REQUEST]
        capabilities = client.get(PREFIX + "/control-capabilities").json()
        assert capabilities["units"][0]["can_request"] is False
        assert capabilities["units"][0]["reason"] == "任务正在运行。"
        assert capabilities["can_recover_units"] is True


def test_tsc_06_stale_source_allows_only_original_recovery_for_current_private_role(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    app, _service, _backend, _source, _calls, body, clock = setup(tmp_path, monkeypatch)
    clock[0] += timedelta(days=1)
    with ProofTestClient(app, headers={"x-rquant-user": "alice"}) as client:
        caps = client.get(PREFIX + "/control-capabilities").json()
        assert caps["can_recover_units"] is True and caps["can_recover_scheduling"] is False
        assert caps["units"] == [] and caps["can_control_scheduling"] is False
    with ProofTestClient(app, headers={"x-rquant-user": "bob"}) as client:
        caps = client.get(PREFIX + "/control-capabilities").json()
        assert caps["can_recover_units"] is False and caps["can_recover_scheduling"] is False


def test_tsc_01_blocking_private_leaf_does_not_block_the_original_asgi_loop(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import asyncio
    import time

    from fastapi import Request
    from rquant.web.routes.task_center_controls import _original

    app, _service, _backend, _source, _calls, body, _clock = setup(tmp_path, monkeypatch)
    original = app.state.web.task_control_gateway.lookup
    entered = False
    completed = False
    beats = 0

    def slow_lookup(*args: object, **kwargs: object):
        nonlocal entered, completed
        entered = True
        try:
            time.sleep(.05)
            return original(*args, **kwargs)
        finally:
            completed = True

    monkeypatch.setattr(app.state.web.task_control_gateway, "lookup", slow_lookup)

    async def receive() -> dict[str, object]:
        return {"type": "http.request", "body": body.model_dump_json().encode(), "more_body": False}

    async def run() -> None:
        nonlocal beats
        req = Request({"type": "http", "app": app, "method": "POST", "path": "/api/v1/tasks/controls/lookup", "headers": []}, receive)
        task = asyncio.create_task(_original(req, "alice", body, resume=False))
        try:
            while not completed:
                if entered:
                    beats += 1
                await asyncio.sleep(.001)
            assert (await task).status == "not_found"
        finally:
            if not task.done():
                await task

    asyncio.run(run())
    assert beats > 0
