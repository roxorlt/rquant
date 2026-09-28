"""The browser can queue a read-only plan only through a bounded control command."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from rquant.page_control import SubmitBackfillPlan, parse_page_control_command
from rquant.web.backfill_plan_command_gateway import (
    BackfillPlanCommandConflictError,
    BackfillPlanCommandGateway,
    BackfillPlanCommandUnavailableError,
)
from rquant.web.settings import WebSettings
from tests.support.web_proxy_identity import ProofTestClient as TestClient
from tests.support.web_proxy_identity import create_private_test_app as create_app

NOW = datetime(2026, 9, 27, 5, 0, tzinfo=UTC)
HEADERS = {
    "x-rquant-user": "researcher",
    "x-rquant-csrf": "1",
    "origin": "http://testserver",
}
PATH = "/api/v1/data/backfill-plans/commands"
TASK_ID = "a" * 32


def _body(command_id: str = "plan-original") -> dict[str, str]:
    return {
        "command_id": command_id,
        "requested_at": NOW.isoformat(),
        "audit_start": "2024-09-01",
        "completed_through": "2025-04-30",
    }


def _wire(body: dict[str, object], *, status: str = "succeeded", result: object = None):
    return {
        "command_id": body["command_id"],
        "status": status,
        "enqueued_at": body["requested_at"],
        "completed_at": body["requested_at"] if status == "succeeded" else None,
        "result": {"outcome": "task_queued", "task_id": TASK_ID} if result is None else result,
        "error": None,
    }


def _app(root: Path, transport=None):
    return create_app(
        WebSettings(serving_root=root),
        clock=lambda: NOW,
        background=False,
        backfill_plan_command_transport=transport,
    )


def test_success_queues_same_typed_command_with_server_actor_on_retry(tmp_path: Path) -> None:
    received: list[dict[str, object]] = []

    def transport(payload: dict[str, object]) -> dict[str, object]:
        received.append(payload)
        command = parse_page_control_command(payload)
        assert isinstance(command, SubmitBackfillPlan)
        return _wire(payload)

    with TestClient(_app(tmp_path / "serving", transport)) as client:
        first = client.post(PATH, json=_body(), headers=HEADERS)
        second = client.post(PATH, json=_body(), headers=HEADERS)

    assert first.status_code == second.status_code == 200
    assert (
        first.json()
        == second.json()
        == {
            "command_id": "plan-original",
            "status": "queued",
            "task_id": TASK_ID,
            "message": "已排队，等待生成",
        }
    )
    assert received == [received[0], received[0]]
    assert received[0] == {
        "kind": "submit_backfill_plan",
        **{**_body(), "requested_at": "2026-09-27T05:00:00Z"},
        "actor_id": "researcher",
    }


@pytest.mark.parametrize(
    ("status", "message"),
    [
        ("pending", "已受理，等待处理"),
        ("processing", "正在处理"),
        ("failed", "请求失败，请检查后重新发起。"),
        ("ambiguous", "状态待确认，请使用原请求重试。"),
    ],
)
def test_non_success_never_claims_queued(tmp_path: Path, status: str, message: str) -> None:
    def transport(payload: dict[str, object]) -> dict[str, object]:
        return {**_wire(payload, status=status), "error": "/secret/raw-system-error"}

    with TestClient(_app(tmp_path / "serving", transport)) as client:
        response = client.post(PATH, json=_body(), headers=HEADERS)

    assert response.status_code == 200
    assert response.json() == {
        "command_id": "plan-original",
        "status": status,
        "task_id": None,
        "message": message,
    }
    assert "/secret" not in response.text
    if status == "failed":
        assert "原请求重试" not in response.text


@pytest.mark.parametrize(
    "receipt_change",
    [
        {"result": {"outcome": "plan_generated", "task_id": TASK_ID}},
        {"result": {"outcome": "task_queued", "task_id": "bad"}},
        {"result": {"outcome": "task_queued", "task_id": TASK_ID, "plan_hash": "x"}},
        {"command_id": "another-command"},
        {"status": "succeeded", "result": "task_queued"},
        {"enqueued_at": "not-a-time"},
    ],
)
def test_unverifiable_success_is_not_reported_as_queued(
    tmp_path: Path, receipt_change: dict[str, object]
) -> None:
    with TestClient(
        _app(tmp_path / "serving", lambda payload: {**_wire(payload), **receipt_change})
    ) as client:
        response = client.post(PATH, json=_body(), headers=HEADERS)

    assert response.status_code == 502
    assert "已排队" not in response.text
    assert "原请求重试" in response.json()["detail"]


@pytest.mark.parametrize(
    ("headers", "body", "expected"),
    [
        ({"x-rquant-csrf": "1", "origin": "http://testserver"}, _body(), 401),
        ({"x-rquant-user": "researcher", "origin": "http://testserver"}, _body(), 403),
        ({**HEADERS, "origin": "https://other.test"}, _body(), 403),
        (HEADERS, {**_body(), "actor_id": "admin"}, 422),
        (HEADERS, {**_body(), "snapshot_path": "/secret/source.duckdb"}, 422),
        (HEADERS, {**_body(), "audit_start": "2025-05-01"}, 422),
        (HEADERS, {**_body(), "completed_through": "2035-12-31"}, 422),
        (HEADERS, {**_body(), "command_id": "x" * 129}, 422),
        (HEADERS, {**_body(), "command_id": ""}, 422),
    ],
)
def test_bad_requests_are_rejected_before_page_control(
    tmp_path: Path, headers: dict[str, str], body: dict[str, str], expected: int
) -> None:
    def must_not_submit(_payload: dict[str, object]) -> dict[str, object]:
        pytest.fail("bad request reached PageControl")

    with TestClient(_app(tmp_path / "serving", must_not_submit)) as client:
        response = client.post(PATH, json=body, headers=headers)

    assert response.status_code == expected
    assert "traceback" not in response.text.lower()


def test_oversize_and_non_json_are_rejected_before_page_control(tmp_path: Path) -> None:
    def must_not_submit(_payload: dict[str, object]) -> dict[str, object]:
        pytest.fail("bad request reached PageControl")

    with TestClient(_app(tmp_path / "serving", must_not_submit)) as client:
        oversized = client.post(
            PATH,
            content=json.dumps({**_body(), "padding": "x" * 5_000}),
            headers={**HEADERS, "content-type": "application/json"},
        )
        wrong_type = client.post(
            PATH,
            content=json.dumps(_body()),
            headers={**HEADERS, "content-type": "text/plain"},
        )
    assert oversized.status_code == 413
    assert wrong_type.status_code == 415


def test_connection_loss_keeps_original_command_retryable(tmp_path: Path) -> None:
    submitted: list[dict[str, object]] = []

    def transport(payload: dict[str, object]) -> dict[str, object]:
        submitted.append(payload)
        if len(submitted) == 1:
            raise TimeoutError("response lost after PageControl accepted command")
        return _wire(payload)

    with TestClient(_app(tmp_path / "serving", transport)) as client:
        lost = client.post(PATH, json=_body(), headers=HEADERS)
        retried = client.post(PATH, json=_body(), headers=HEADERS)

    assert lost.status_code == 503
    assert "原请求重试" in lost.json()["detail"]
    assert retried.status_code == 200
    assert retried.json()["status"] == "queued"
    assert submitted == [submitted[0], submitted[0]]


def test_page_control_conflict_and_unavailable_are_distinct(tmp_path: Path) -> None:
    def conflict(_payload: dict[str, object]) -> dict[str, object]:
        raise BackfillPlanCommandConflictError("different actor for existing command")

    def unavailable(_payload: dict[str, object]) -> dict[str, object]:
        raise OSError("connection refused")

    def unclassified(_payload: dict[str, object]) -> dict[str, object]:
        raise ValueError("database failed after command effect")

    with TestClient(_app(tmp_path / "conflict", conflict)) as client:
        rejected = client.post(PATH, json=_body(), headers=HEADERS)
    with TestClient(_app(tmp_path / "unavailable", unavailable)) as client:
        offline = client.post(PATH, json=_body(), headers=HEADERS)
    with TestClient(_app(tmp_path / "unclassified", unclassified)) as client:
        unknown = client.post(PATH, json=_body(), headers=HEADERS)

    assert rejected.status_code == 409
    assert offline.status_code == 503
    assert unknown.status_code == 503
    assert "different actor" not in rejected.text
    assert "connection refused" not in offline.text
    assert "database failed" not in unknown.text
    assert "原请求重试" in unknown.json()["detail"]


@pytest.mark.parametrize("upstream_status", [400, 409])
def test_unclassified_page_control_http_error_preserves_original_command(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, upstream_status: int
) -> None:
    submitted: list[dict[str, object]] = []

    class FakeResponse:
        status = upstream_status

    class FakeConnection:
        def __init__(self, host: str, port: int, *, timeout: float) -> None:
            assert (host, port, timeout) == ("127.0.0.1", 8767, 1.0)

        def request(self, method: str, path: str, *, body: bytes, headers: dict[str, str]) -> None:
            assert (method, path, headers["Content-Type"]) == (
                "POST",
                "/v1/commands",
                "application/json",
            )
            submitted.append(json.loads(body))

        def getresponse(self) -> FakeResponse:
            return FakeResponse()

        def close(self) -> None:
            pass

    monkeypatch.setattr(
        "rquant.web.backfill_plan_command_gateway.http.client.HTTPConnection", FakeConnection
    )
    with TestClient(_app(tmp_path / "serving")) as client:
        first = client.post(PATH, json=_body(), headers=HEADERS)
        retried = client.post(PATH, json=_body(), headers=HEADERS)

    assert first.status_code == retried.status_code == 503
    assert first.json() == retried.json()
    assert "状态待确认" in first.json()["detail"]
    assert "原请求重试" in first.json()["detail"]
    assert "冲突" not in first.text
    assert submitted == [submitted[0], submitted[0]]


def test_fixed_loopback_transport_never_follows_redirect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[object, ...]] = []

    class FakeResponse:
        status = 302

        def getheader(self, name: str, default: str = "") -> str:
            return "http://outside.test/steal" if name == "Location" else default

    class FakeConnection:
        def __init__(self, host: str, port: int, *, timeout: float) -> None:
            calls.append(("connect", host, port, timeout))

        def request(self, method: str, path: str, *, body: bytes, headers: dict[str, str]) -> None:
            calls.append(("request", method, path, json.loads(body), headers["Content-Type"]))

        def getresponse(self) -> FakeResponse:
            return FakeResponse()

        def close(self) -> None:
            calls.append(("close",))

    monkeypatch.setenv("HTTP_PROXY", "http://outside.test:1234")
    monkeypatch.setattr(
        "rquant.web.backfill_plan_command_gateway.http.client.HTTPConnection", FakeConnection
    )
    with pytest.raises(BackfillPlanCommandUnavailableError):
        BackfillPlanCommandGateway()._post({"kind": "submit_backfill_plan"})
    assert calls[0] == ("connect", "127.0.0.1", 8767, 1.0)
    assert calls[1][0:3] == ("request", "POST", "/v1/commands")
    assert calls[-1] == ("close",)
    assert "outside.test" not in str(calls)


def test_gateway_rejects_nonloopback_command_target() -> None:
    with pytest.raises(ValueError):
        BackfillPlanCommandGateway(endpoint="http://localhost:8767/v1/commands")
