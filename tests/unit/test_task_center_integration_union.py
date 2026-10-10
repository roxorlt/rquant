"""Accepted M4 and M13 contracts share the original five integration seams."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from pydantic import ValidationError


def _private_settings() -> dict[str, object]:
    return {
        "serving_root": Path("/private/tmp/rq-union-serving"),
        "ingress_socket_path": Path("/private/tmp/rq-union-ingress/web.sock"),
        "proxy_proof_file": Path("/private/tmp/rq-union-proof/proof"),
        "paper_portfolio_users": {"alice"},
        "screen_query_users": {"alice"},
        "screen_query_socket_path": Path("/private/tmp/rq-union-screen/query.sock"),
        "screen_query_service_uid": os.geteuid() + 1,
        "screen_query_shared_gid": os.getegid(),
        "task_control_enabled": True,
        "task_unit_run_users": {"alice"},
        "task_scheduling_admin_users": {"admin"},
        "task_control_socket_path": Path("/private/tmp/rq-union-task/control.sock"),
        "task_control_service_uid": os.geteuid() + 2,
        "task_control_web_group_gid": os.getegid(),
    }


@pytest.mark.parametrize(
    "field",
    ["paper_portfolio_users", "screen_query_users", "task_unit_run_users", "task_scheduling_admin_users"],
)
def test_union_exact_roles_retain_all_four_validators(field: str) -> None:
    from rquant.web.settings import WebSettings

    settings = WebSettings.model_validate(_private_settings())
    assert settings.paper_portfolio_users == settings.screen_query_users == settings.task_unit_run_users == frozenset({"alice"})
    assert settings.task_scheduling_admin_users == frozenset({"admin"})
    for invalid in ({"alice;id"}, {str(index) for index in range(17)}):
        with pytest.raises(ValidationError, match="bounded list of exact user names"):
            WebSettings.model_validate(_private_settings() | {field: invalid})


def test_union_screen_and_task_endpoints_require_distinct_private_parents() -> None:
    from rquant.web.settings import WebSettings

    old = WebSettings(serving_root=Path("/private/tmp/rq-union-serving"))
    assert old.screen_query_socket_path is None and old.task_control_socket_path is None
    assert not old.task_control_enabled
    good = WebSettings.model_validate(_private_settings())
    assert good.screen_query_socket_path.parent != good.task_control_socket_path.parent
    for altered in (
        {"task_control_socket_path": Path("/private/tmp/rq-union-screen/control.sock")},
        {"screen_query_socket_path": Path("/private/tmp/rq-union-task/query.sock")},
    ):
        with pytest.raises(ValidationError, match="separate"):
            WebSettings.model_validate(_private_settings() | altered)


@pytest.mark.parametrize("factory_name", ["build_page_control_service", "build_page_control_service_with_dependencies"])
def test_union_original_journal_recovers_both_private_families(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, factory_name: str
) -> None:
    from rquant import page_control_service
    from rquant.page_control import PageControlCommandConflictError, PageControlOutbox, parse_page_control_command
    from tests.unit.test_screen_query_history import command, executed
    from tests.unit.test_task_control_admission import NOW, REQUEST, control_service, request

    tmp_path.chmod(0o700)
    PageControlOutbox(tmp_path / "control.db").path.chmod(0o600)
    old_control, backend, source, _executor, calls = control_service(tmp_path, monkeypatch)
    task_original = request()
    control = getattr(page_control_service, factory_name)(
        outbox_path=old_control.outbox.path,
        data_dir=tmp_path / "data", log_dir=tmp_path / "logs",
        allowed_lab_export_roots=(), load_default_lab_backend=False,
        task_control_backend=backend, screen_query_executor=lambda _: executed(),
        screen_query_cursor_key=b"x" * 32, clock=lambda: NOW,
    )
    assert control.outbox is backend.journal.outbox
    assert control.consumer.screen_query_history.outbox is control.outbox
    screen_original = command("joint-private-screen")
    screen_receipt = control._submit_trusted_screen_query(screen_original, authenticated_actor_id="alice")
    assert screen_receipt.status.value == "succeeded"
    assert control.consumer.screen_query_history.detail("alice", screen_original.command_id).total == 21
    assert control.consumer.screen_query_history.detail("bob", screen_original.command_id) is None
    task_receipt = control._submit_trusted_task_control(task_original, authenticated_actor_id="alice", verified_metadata_identity=backend.journal.identity())
    accepted, _receipt = control.outbox.lookup_task_control_command(task_original, authenticated_actor_id="alice")
    assert backend.journal.run_effect(accepted).stage == "unknown" and calls == [REQUEST]
    for private in (screen_original.model_dump(mode="json"), accepted.model_dump(mode="json")):
        with pytest.raises(ValueError, match="trusted|private|protected"):
            parse_page_control_command(private)
    control.consumer.screen_query_executor = lambda _: pytest.fail("completed screen execution repeated")
    monkeypatch.setattr(source, "read", lambda **_: pytest.fail("old task retry read newer source"))
    assert control._resume_trusted_screen_query(screen_original, authenticated_actor_id="alice") == screen_receipt
    assert control._resume_trusted_task_control(task_original, authenticated_actor_id="alice") == task_receipt
    assert calls == [REQUEST]
    with pytest.raises(PageControlCommandConflictError):
        control._submit_trusted_screen_query(command(REQUEST), authenticated_actor_id="alice")
    assert control.outbox.receipt(REQUEST) == task_receipt


def test_union_web_keeps_both_gateways_routes_private_headers_and_caps(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.screen.query_admission import ScreenQueryPrivateClient, dispatch_screen_query_action
    from rquant.web.app import create_app
    from rquant.web.models.screen_history import ScreenQueryAction, ScreenQueryReadData
    from rquant.web.settings import WebSettings
    from tests.support.web_proxy_identity import ProofTestClient
    from tests.unit.test_screen_query_history import service
    from tests.unit.test_web_task_center_controls import setup

    old_app, _control, _backend, _source, _calls, task_body, clock = setup(tmp_path, monkeypatch)
    private = tmp_path / "screen"
    private.mkdir(mode=0o700)
    screen_control, _history = service(private)
    settings = WebSettings.model_validate(old_app.state.web.settings.model_dump() | {
        "screen_query_users": {"alice"},
        "screen_query_socket_path": Path("/private/tmp/rq-union-screen/query.sock"),
        "screen_query_service_uid": os.geteuid() + 1, "screen_query_shared_gid": os.getegid(),
    })
    screen_client = ScreenQueryPrivateClient(settings.screen_query_socket_path, expected_service_uid=settings.screen_query_service_uid, shared_gid=settings.screen_query_shared_gid)

    def private_read(action: ScreenQueryAction, *, authenticated_actor_id: str) -> ScreenQueryReadData:
        return dispatch_screen_query_action(screen_control, authenticated_actor_id=authenticated_actor_id, allowed_users=frozenset({"alice"}), action=action)

    monkeypatch.setattr(screen_client, "request", private_read)
    task_gateway = old_app.state.web.task_control_gateway
    app = create_app(settings, screen_query_client=screen_client, task_control_gateway=task_gateway, clock=lambda: clock[0], background=False)
    assert app.state.web.screen_query_client is screen_client and app.state.web.task_control_gateway is task_gateway
    paths = set(app.openapi()["paths"])
    assert {"/api/v1/screen/query/history", "/api/v1/screen/query/execute", "/api/v1/screen/query/alert-draft", "/api/v1/monitor/condition-rules", "/api/v1/tasks/control-capabilities", "/api/v1/tasks/scheduling/commands", "/api/v1/strategy-templates", "/api/v1/paper-portfolios"}.issubset(paths)
    with ProofTestClient(app, headers={"x-rquant-user": "alice"}) as client:
        screen_response = client.get("/api/v1/screen/query/history")
        assert screen_response.status_code == 200 and screen_response.headers["Cache-Control"] == "no-store"
        assert screen_response.json()["history"]["items"] == []
        tasks = client.get("/api/v1/tasks/control-capabilities")
        assert tasks.status_code == 200 and tasks.json()["units"][0]["unit"] == task_body.unit
        write_headers = {"x-rquant-csrf": "1", "Content-Type": "application/json"}
        assert client.post("/api/v1/screen/query/execute", content=b" " * (64 * 1024 + 1), headers=write_headers).status_code == 413
        assert client.post(f"/api/v1/tasks/units/{task_body.unit}/run", content=b" " * 4097, headers=write_headers).status_code == 413
        assert client.post("/api/v1/tasks/scheduling/commands", content=b" " * 1025, headers=write_headers).status_code == 413


def test_union_serving_retains_original_owners_and_new_exact_task_material() -> None:
    from rquant.ops_status_serving import ops_status_source_result
    from rquant.serving_read_models import PAGE_PROJECTION_CONTRACTS, ServingProjectionInput, require_projection_owner_budget
    from tests.unit.test_task_center_projection import _task_sample

    assert {"condition_alert_rule_state", "condition_alert_runtime_event", "intraday_screen_source", "intraday_feature_snapshot", "intraday_kline", "paper_portfolio_state", "paper_portfolio_material", "strategy_definition", "strategy_template_source", "experiment_private_attempt", "experiment_private_family", "experiment_private_window"}.issubset(PAGE_PROJECTION_CONTRACTS)
    expected = {"ops_task_cpu": ("ops_status", 5, 32 * 1024), "ops_task_runs": ("ops_status", 32, 64 * 1024), "lab_scheduler_control": ("lab_jobs", 1, 16 * 1024)}
    for name, (owner, rows, budget) in expected.items():
        contract = PAGE_PROJECTION_CONTRACTS[name]
        assert (contract.owner_dataset_id, contract.max_rows, contract.max_bytes) == (owner, rows, budget)
    task = _task_sample()
    payload = ops_status_source_result(task).payload
    projections = tuple(ServingProjectionInput.bind(table, owner_dataset_id="ops_status", owner_generation_id="a" * 64) for table in payload.projections)
    require_projection_owner_budget(projections)
    cpu = next(table for table in projections if table.table_name == "ops_task_cpu")
    assert cpu.rows[2]["percent"] == "40.0"
    with pytest.raises(ValueError, match="owner"):
        ServingProjectionInput.model_validate(cpu.model_dump() | {"owner_dataset_id": "paper_accounts"})
