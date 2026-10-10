"""The public study body selects facts; the original private owner supplies identity."""

from __future__ import annotations

import json
from pathlib import Path
from uuid import UUID

import pytest
from pydantic import ValidationError

from rquant.web.models import minute_backtests as api
from rquant.web.models import collaboration as private_api
from tests.unit.test_minute_backtest_study_control import control, study


def public_body() -> dict[str, object]:
    original = study()
    body = original.request.model_dump(mode="json", exclude_computed_fields=True)
    del body["request_id"]
    del body["owner_id"]
    body["command_id"] = original.command_id
    body["protocol"] = body.pop("formal_protocol")
    return body


def test_public_study_preserves_every_original_control_and_injects_the_actor() -> None:
    value = api.MinuteStudyCreateRequest.model_validate(public_body())
    command = value.to_command(authenticated_actor_id="researcher")
    assert command == study()
    assert command.request.model_dump(mode="json") == study().request.model_dump(mode="json")
    assert api.MinuteStudyCreateRequest.model_validate_json(value.model_dump_json()) == value
    assert "owner_id" not in value.model_dump()
    assert value.parameters.parameters.paper.stop_loss_pct == study().request.parameters.parameters.paper.stop_loss_pct


@pytest.mark.parametrize("field", ["actor_id", "owner_id", "path", "head", "trusted", "prepared", "plan"])
def test_public_study_rejects_browser_authority(field: str) -> None:
    with pytest.raises(ValidationError):
        api.MinuteStudyCreateRequest.model_validate(public_body() | {field: "not-authority"})


def test_public_study_reuses_original_mode_and_complete_search_validation() -> None:
    body = public_body() | {"mode": "random"}
    with pytest.raises(ValidationError):
        api.MinuteStudyCreateRequest.model_validate(body)
    body["search"] = {"base": body["parameters"], "axes": [{"path": "max_hold_days", "values": [1, 2]}],
        "mode": "random", "seed": 17, "requested_trials": 2}
    value = api.MinuteStudyCreateRequest.model_validate(body)
    assert value.to_command(authenticated_actor_id="researcher").request.search.seed == 17
    with pytest.raises(ValidationError):
        api.MinuteStudyCreateRequest.model_validate(body | {"random_seed": 18})


def test_private_study_query_reads_actual_signed_parent_without_preparing(tmp_path: Path) -> None:
    service = control(tmp_path)
    command = study()
    proof = service.collaboration.issue_authorization(command.actor_id, command.model_dump(mode="json"))
    service.submit_authorized(command, proof)
    before = service.outbox.original_command_bytes(command.command_id)
    query = private_api.MinuteStudyJournalQuery(command_id=UUID(command.command_id))
    request = private_api.CollaborationPrivateRequest(schema_version=1, operation="study_journal",
        authenticated_actor_id=command.actor_id, study_query=query)
    value = service.collaboration_request(request)
    assert value.command == command
    assert value.status == "pending" and value.admission_json is None and value.result is None
    assert service.outbox.original_command_bytes(command.command_id) == before
    assert service.outbox.effect(command.command_id) is None
    with pytest.raises((PermissionError, LookupError)):
        service.collaboration_request(request.model_copy(update={"authenticated_actor_id": "viewer"}))


def test_private_study_query_rejects_unknown_operation_fields() -> None:
    query = private_api.MinuteStudyJournalQuery(command_id=UUID(study().command_id))
    with pytest.raises(ValidationError):
        private_api.CollaborationPrivateRequest(schema_version=1, operation="me",
            authenticated_actor_id="researcher", study_query=query)


def test_study_routes_have_real_typed_response_models() -> None:
    from fastapi import FastAPI
    from rquant.web.minute_backtest_routes import router

    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    paths = app.openapi()["paths"]
    prefix = "/api/v1/backtests/minute-runtime/studies"
    assert {"get", "post"}.issubset(paths[prefix])
    for suffix in ("/capabilities", "/{command_id}", "/{command_id}/heatmap"):
        assert "get" in paths[prefix + suffix]
    schema = app.openapi()["components"]["schemas"]["MinuteStudyCreateRequest"]
    assert not {"owner_id", "actor_id", "prepared", "path", "head"}.intersection(schema["properties"])
    assert "search" in schema["properties"] and "walk_forward" in schema["properties"]
    assert "private_marker" not in json.dumps(app.openapi())
