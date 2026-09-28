"""Research job controls keep the Serving hint separate from Lab authority."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from rquant.lab_jobs import CommandAvailability, JobStatus
from rquant.web.settings import WebSettings
from tests.support.web_proxy_identity import (
    PROXY_HEADERS,
    create_private_test_app,
)
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture
from tests.unit.test_web_tasks import _job

JOB_ID = str(UUID(int=1))
COMMAND_ID = str(UUID(int=2))
REQUESTED_AT = "2026-09-24T07:30:00Z"


def _body(**changes: object) -> dict[str, object]:
    return {
        "command_id": COMMAND_ID,
        "requested_at": REQUESTED_AT,
        "job_id": JOB_ID,
        "action": "pause",
        "expected_version": 7,
        **changes,
    }


def _job_with_actions():
    record = _job(
        1,
        status=JobStatus.RUNNING,
        updated_at=FIXTURE_BUILT_AT - timedelta(minutes=1),
    )
    return record.model_copy(
        update={
            "summary": record.summary.model_copy(
                update={
                    "version": 7,
                    "command_availability": CommandAvailability(
                        pause=True, resume=False, cancel=True, retry=False
                    ),
                }
            )
        }
    )


def _client_app(root: Path, **kwargs: object):
    return create_private_test_app(
        WebSettings(
            serving_root=root,
            ingress_socket_path=root.parent / "private-web.sock",
            lab_control_users=frozenset({"researcher"}),
        ),
        clock=lambda: FIXTURE_BUILT_AT + timedelta(seconds=30),
        background=False,
        **kwargs,
    )


def test_lab_control_users_are_separate_and_require_private_ingress(tmp_path: Path) -> None:
    assert WebSettings(serving_root=tmp_path).lab_control_users == frozenset()
    with pytest.raises(ValidationError):
        WebSettings(serving_root=tmp_path, lab_control_users=frozenset({"researcher"}))
    with pytest.raises(ValidationError):
        WebSettings(
            serving_root=tmp_path,
            ingress_socket_path=tmp_path / "private.sock",
            lab_control_users=frozenset({"researcher", "bad name"}),
        )


def test_published_actions_only_reach_exact_operator(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline", lab_jobs=(_job_with_actions(),))
    with TestClient(_client_app(root), headers=PROXY_HEADERS) as client:
        for viewer, expected in (("researcher", ["pause", "cancel"]), ("other", None)):
            response = client.get("/api/v1/tasks/overview", headers={"x-rquant-user": viewer})
            assert response.status_code == 200, response.text
            row = response.json()["data"]["research"]["items"][0]
            assert row["available_actions"] == expected
            assert row["job_version"] == (7 if expected else None)


def test_live_control_capability_reflects_exact_current_operator(tmp_path: Path) -> None:
    with TestClient(_client_app(tmp_path / "serving"), headers=PROXY_HEADERS) as client:
        for viewer, allowed in (("researcher", True), ("other", False)):
            response = client.get(
                "/api/v1/tasks/jobs/control-capabilities",
                headers={"x-rquant-user": viewer},
            )
            assert response.status_code == 200
            assert response.json() == {"can_control": allowed}


def test_old_serving_generation_stays_read_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tests.support import web_serving_fixture as fixture

    build = fixture.build_serving_read_models

    def old_tables(source: object) -> dict[str, object]:
        tables = dict(build(source))
        tables["lab_jobs"] = tables["lab_jobs"].drop(
            columns=["job_version", "can_pause", "can_resume", "can_cancel", "can_retry"]
        )
        return tables

    monkeypatch.setattr(fixture, "build_serving_read_models", old_tables)
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline", lab_jobs=(_job_with_actions(),))
    with TestClient(_client_app(root), headers=PROXY_HEADERS) as client:
        response = client.get("/api/v1/tasks/overview", headers={"x-rquant-user": "researcher"})
    assert response.status_code == 200, response.text
    row = response.json()["data"]["research"]["items"][0]
    assert row["job_version"] is None
    assert row["available_actions"] is None


def test_control_admission_rejects_untrusted_requests_before_page_control(
    tmp_path: Path,
) -> None:
    calls: list[dict[str, object]] = []

    def transport(payload: dict[str, object]) -> dict[str, object]:
        calls.append(payload)
        raise AssertionError("not admitted")

    root = tmp_path / "serving"
    app = _client_app(root, lab_control_command_transport=transport)
    with TestClient(app, headers=PROXY_HEADERS) as client:
        route = "/api/v1/tasks/jobs/commands"
        assert client.post(route, json=_body()).status_code == 401
        assert (
            client.post(
                route, json=_body(), headers={"x-rquant-user": "other", "x-rquant-csrf": "1"}
            ).status_code
            == 403
        )
        assert (
            client.post(route, json=_body(), headers={"x-rquant-user": "researcher"}).status_code
            == 403
        )
        assert (
            client.post(
                route,
                json=_body(action="submit"),
                headers={"x-rquant-user": "researcher", "x-rquant-csrf": "1"},
            ).status_code
            == 422
        )
        assert (
            client.post(
                route,
                json=_body(reason="browser controls reason"),
                headers={"x-rquant-user": "researcher", "x-rquant-csrf": "1"},
            ).status_code
            == 422
        )
        assert (
            client.post(
                route,
                content=b"{" + b" " * 4096 + b"}",
                headers={
                    "x-rquant-user": "researcher",
                    "x-rquant-csrf": "1",
                    "content-type": "application/json",
                },
            ).status_code
            == 413
        )
    assert calls == []


def test_control_submission_checks_nested_result_and_stable_interaction(
    tmp_path: Path,
) -> None:
    calls: list[dict[str, object]] = []
    expected_request_id = str(
        uuid5(
            NAMESPACE_URL,
            f"rquant.lab-job-center.interaction:web.lab-control:researcher:{JOB_ID}:pause:7",
        )
    )

    def transport(payload: dict[str, object]) -> dict[str, object]:
        calls.append(payload)
        return {
            "command_id": COMMAND_ID,
            "status": "succeeded",
            "enqueued_at": REQUESTED_AT,
            "completed_at": REQUESTED_AT,
            "result": {
                "result": "submitted",
                "request_id": expected_request_id,
                "command_type": "pause",
                "job_id": JOB_ID,
                "expected_version": 7,
                "spool": {
                    "path": "/private/tmp/test-spool",
                    "state": "pending",
                    "device": 1,
                    "inode": 2,
                    "content_hash": "a" * 64,
                },
            },
        }

    root = tmp_path / "serving"
    with TestClient(
        _client_app(root, lab_control_command_transport=transport), headers=PROXY_HEADERS
    ) as client:
        response = client.post(
            "/api/v1/tasks/jobs/commands",
            json=_body(),
            headers={"x-rquant-user": "researcher", "x-rquant-csrf": "1"},
        )
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "submitted"
    assert response.json()["message"] == "已提交，等待状态更新。"
    assert calls == [
        {
            "kind": "submit_lab_command",
            "command_id": COMMAND_ID,
            "requested_at": REQUESTED_AT,
            "command": {
                "command_type": "pause",
                "job_id": JOB_ID,
                "expected_version": 7,
                "reason": "网页暂停研究任务",
            },
            "interaction_key": f"web.lab-control:researcher:{JOB_ID}:pause:7",
        }
    ]


@pytest.mark.parametrize(
    ("inner", "expected_status"),
    [
        (
            {
                "result": "stale",
                "expected_version": 7,
                "authoritative_version": 8,
                "authoritative_status": "running",
            },
            409,
        ),
        (
            {
                "result": "unavailable",
                "command_type": "pause",
                "authoritative_version": 7,
                "authoritative_status": "checkpointed",
            },
            409,
        ),
        (
            {
                "result": "submitted",
                "command_type": "resume",
                "expected_version": 7,
                "spool": {
                    "path": "/private/tmp/spool",
                    "state": "pending",
                    "device": 1,
                    "inode": 2,
                    "content_hash": "a" * 64,
                },
            },
            502,
        ),
    ],
)
def test_authority_conflicts_and_mismatched_result_are_not_success(
    tmp_path: Path, inner: dict[str, object], expected_status: int
) -> None:
    expected_request_id = str(
        uuid5(
            NAMESPACE_URL,
            f"rquant.lab-job-center.interaction:web.lab-control:researcher:{JOB_ID}:pause:7",
        )
    )

    def transport(_payload: dict[str, object]) -> dict[str, object]:
        return {
            "command_id": COMMAND_ID,
            "status": "succeeded",
            "enqueued_at": REQUESTED_AT,
            "completed_at": REQUESTED_AT,
            "result": {"request_id": expected_request_id, "job_id": JOB_ID, **inner},
        }

    with TestClient(
        _client_app(tmp_path / "serving", lab_control_command_transport=transport),
        headers=PROXY_HEADERS,
    ) as client:
        response = client.post(
            "/api/v1/tasks/jobs/commands",
            json=_body(),
            headers={"x-rquant-user": "researcher", "x-rquant-csrf": "1"},
        )
    assert response.status_code == expected_status
    assert response.json().get("status") != "submitted"


@pytest.mark.parametrize("wire_status", ["pending", "processing", "ambiguous", "failed"])
def test_unfinished_outer_receipt_never_claims_task_state(tmp_path: Path, wire_status: str) -> None:
    def transport(_payload: dict[str, object]) -> dict[str, object]:
        return {
            "command_id": COMMAND_ID,
            "status": wire_status,
            "enqueued_at": REQUESTED_AT,
        }

    with TestClient(
        _client_app(tmp_path / "serving", lab_control_command_transport=transport),
        headers=PROXY_HEADERS,
    ) as client:
        response = client.post(
            "/api/v1/tasks/jobs/commands",
            json=_body(),
            headers={"x-rquant-user": "researcher", "x-rquant-csrf": "1"},
        )
    assert response.status_code == 200
    assert response.json()["status"] in {"pending", "processing", "unknown"}
    assert "已暂停" not in response.text
