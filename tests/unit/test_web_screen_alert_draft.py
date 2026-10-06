from __future__ import annotations

import importlib
from datetime import timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from tests.unit.test_screen_query_history import NOW, OWNER, command, service


def test_draft_is_owner_private_idempotent_immutable_and_expires(tmp_path: Path) -> None:
    draft_module = importlib.import_module("rquant.screen.alert_draft")
    control, history = service(tmp_path)
    control._submit_trusted_screen_query(command(), authenticated_actor_id=OWNER)
    execution = history.detail(OWNER, "run-1")
    request = draft_module.ScreenAlertDraftRequest(
        command_id="draft-1",
        execution_id=execution.execution_id,
        command_hash=execution.command_hash,
    )
    draft = draft_module.create_screen_alert_draft(
        history, owner_id=OWNER, request=request, now=NOW
    )
    assert draft.preferred_scope.kind == "market"
    assert draft.conditions == execution.definition.conditions
    assert draft.origin.result_digest == execution.artifact_sha256
    assert draft.capabilities.consumer_state == "awaiting_consumer"
    assert (
        draft_module.create_screen_alert_draft(
            history, owner_id=OWNER, request=request, now=NOW + timedelta(seconds=1)
        )
        == draft
    )
    assert (
        draft_module.read_screen_alert_draft(
            history, owner_id="bob", draft_id=draft.draft_id, now=NOW
        )
        is None
    )
    assert (
        draft_module.read_screen_alert_draft(
            history, owner_id=OWNER, draft_id=draft.draft_id, now=NOW + timedelta(hours=24)
        )
        is None
    )
    with pytest.raises(ValueError):
        draft_module.create_screen_alert_draft(
            history,
            owner_id=OWNER,
            request=request.model_copy(update={"command_hash": "0" * 64}),
            now=NOW,
        )
    for extra in ({"owner_id": "bob"}, {"conditions": []}, {"total": 21}, {"outcome": "succeeded"}):
        with pytest.raises(ValidationError):
            draft_module.ScreenAlertDraftRequest(**request.model_dump(), **extra)


def test_unconfirmed_or_foreign_execution_cannot_make_a_draft(tmp_path: Path) -> None:
    draft_module = importlib.import_module("rquant.screen.alert_draft")
    control, history = service(tmp_path)
    control._submit_trusted_screen_query(command(), authenticated_actor_id=OWNER)
    execution = history.detail(OWNER, "run-1")
    request = draft_module.ScreenAlertDraftRequest(
        command_id="draft-1",
        execution_id=execution.execution_id,
        command_hash=execution.command_hash,
    )
    for owner, identifier in (("bob", "run-1"), (OWNER, "missing")):
        with pytest.raises(ValueError):
            draft_module.create_screen_alert_draft(
                history,
                owner_id=owner,
                request=request.model_copy(update={"execution_id": identifier}),
                now=NOW,
            )


def test_committed_draft_precedes_capacity_and_keeps_exact_owner_payload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from types import SimpleNamespace
    from rquant.screen.alert_draft import ScreenAlertDraftRequest, create_screen_alert_draft

    control, history = service(tmp_path)
    control._submit_trusted_screen_query(command(), authenticated_actor_id=OWNER)
    execution = history.detail(OWNER, "run-1")
    original = ScreenAlertDraftRequest(
        command_id="draft-1", execution_id="run-1", command_hash=execution.command_hash
    )
    draft = create_screen_alert_draft(history, owner_id=OWNER, request=original, now=NOW)
    monkeypatch.setattr(
        "rquant.screen.query_history.os.statvfs",
        lambda _: SimpleNamespace(f_bavail=0, f_frsize=4096),
    )
    assert (
        create_screen_alert_draft(
            history, owner_id=OWNER, request=original, now=NOW + timedelta(seconds=1)
        )
        == draft
    )
    with pytest.raises(ValueError, match="command changed"):
        create_screen_alert_draft(
            history,
            owner_id=OWNER,
            request=original.model_copy(update={"command_hash": "0" * 64}),
            now=NOW,
        )
    with pytest.raises(ValueError):
        create_screen_alert_draft(history, owner_id="bob", request=original, now=NOW)
    with pytest.raises(ValueError, match="capacity unavailable"):
        create_screen_alert_draft(
            history,
            owner_id=OWNER,
            request=original.model_copy(update={"command_id": "new-draft"}),
            now=NOW,
        )
    with control.outbox._connect() as connection:
        assert connection.execute("SELECT count(*) FROM screen_alert_draft").fetchone()[0] == 1


def test_draft_routes_use_actual_private_admission_and_no_store(tmp_path: Path) -> None:
    from types import SimpleNamespace

    from fastapi import HTTPException, Response
    from starlette.requests import Request

    from rquant.screen.alert_draft import ScreenAlertDraftRequest
    from rquant.screen.query_admission import dispatch_screen_query_action
    from rquant.web.routes.screen_alert_draft import create_alert_draft, get_alert_draft, router

    control, history = service(tmp_path)
    control._submit_trusted_screen_query(command(), authenticated_actor_id=OWNER)
    execution = history.detail(OWNER, "run-1")

    class Client:
        def request(self, action: object, *, authenticated_actor_id: str) -> object:
            return dispatch_screen_query_action(
                control,
                authenticated_actor_id=authenticated_actor_id,
                allowed_users=frozenset({OWNER, "bob"}),
                action=action,
            )

    request = Request(
        {
            "type": "http",
            "headers": [],
            "app": SimpleNamespace(
                state=SimpleNamespace(
                    web=SimpleNamespace(
                        screen_query_client=Client(),
                        settings=SimpleNamespace(screen_query_users=frozenset({OWNER, "bob"})),
                    )
                )
            ),
        }
    )
    response = Response()
    saved = create_alert_draft(
        request,
        response,
        ScreenAlertDraftRequest(
            command_id="draft-1", execution_id="run-1", command_hash=execution.command_hash
        ),
        OWNER,
        None,
    )
    assert response.headers["Cache-Control"] == "no-store"
    assert saved.alert_draft.capabilities.consumer_state == "awaiting_consumer"
    read = get_alert_draft(request, Response(), saved.alert_draft.draft_id, OWNER)
    assert read.owner_scope_tag == saved.owner_scope_tag and read.alert_draft == saved.alert_draft
    with pytest.raises(HTTPException) as missing:
        get_alert_draft(request, Response(), saved.alert_draft.draft_id, "bob")
    assert missing.value.status_code == 404
    for route in router.routes:
        dependencies = {item.call.__name__ for item in route.dependant.dependencies}
        assert "require_current_user" in dependencies
        if "POST" in route.methods:
            assert "require_csrf" in dependencies


def test_draft_private_validation_does_not_echo_proof(tmp_path: Path) -> None:
    from fastapi.exceptions import RequestValidationError
    from starlette.requests import Request

    from rquant.web.app import create_app
    from rquant.web.settings import WebSettings

    app = create_app(WebSettings(serving_root=tmp_path / "serving"), background=False)
    assert "/api/v1/screen/query/alert-draft" in app.openapi()["paths"]
    error = RequestValidationError(
        [
            {
                "type": "extra_forbidden",
                "loc": ("body", "total"),
                "msg": "extra",
                "input": "PRIVATE_PROOF",
            }
        ]
    )
    request = Request({"type": "http", "path": "/api/v1/screen/query/alert-draft", "headers": []})
    coroutine = app.exception_handlers[RequestValidationError](request, error)
    try:
        coroutine.send(None)
    except StopIteration as done:
        response = done.value
    else:
        coroutine.close()
        pytest.fail("validation handler unexpectedly needs runtime")
    assert response.status_code == 422 and b"PRIVATE_PROOF" not in response.body
    assert response.headers["Cache-Control"] == "no-store"
