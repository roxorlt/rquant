"""A browser can request an audit only through an authenticated control command."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from rquant.page_control import SubmitDataAuditReport, parse_page_control_command
from rquant.web.app import create_app
from rquant.web.data_audit_report_command_gateway import (
    AuditReportCommandConflictError,
    AuditReportCommandGateway,
    AuditReportCommandUnavailableError,
)
from rquant.web.settings import WebSettings

NOW = datetime(2026, 9, 27, 5, 0, tzinfo=UTC)
HEADERS = {
    "x-rquant-user": "researcher",
    "x-rquant-csrf": "1",
    "origin": "http://testserver",
}
PATH = "/api/v1/data/audit-report/commands"
TASK_ID = "a" * 32


def _body(command_id: str = "audit-original") -> dict[str, str]:
    return {
        "command_id": command_id,
        "requested_at": NOW.isoformat(),
        "audit_start": "2024-09-01",
        "observed_through": "2025-04-30",
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
        audit_report_command_transport=transport,
    )


def test_success_queues_typed_command_with_server_actor_and_reuses_id(tmp_path: Path) -> None:
    received: list[dict[str, object]] = []

    def transport(payload: dict[str, object]) -> dict[str, object]:
        received.append(payload)
        assert isinstance(parse_page_control_command(payload), SubmitDataAuditReport)
        return _wire(payload)

    with TestClient(_app(tmp_path / "serving", transport)) as client:
        first = client.post(PATH, json=_body(), headers=HEADERS)
        recovered = client.post(PATH, json=_body(), headers=HEADERS)

    assert first.status_code == recovered.status_code == 200
    assert first.json() == recovered.json() == {
        "command_id": "audit-original",
        "status": "queued",
        "task_id": TASK_ID,
        "message": "已排队，等待生成",
    }
    assert received == [received[0], received[0]]
    assert received[0] == {
        "kind": "submit_data_audit_report",
        **{**_body(), "requested_at": "2026-09-27T05:00:00Z"},
        "actor_id": "researcher",
    }


@pytest.mark.parametrize(
    ("status", "message"),
    [
        ("pending", "状态待确认，请使用原请求重试。"),
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
        "command_id": "audit-original",
        "status": status,
        "task_id": None,
        "message": message,
    }
    assert "/secret" not in response.text


@pytest.mark.parametrize(
    "receipt_change",
    [
        {"result": {"outcome": "report_generated", "task_id": TASK_ID}},
        {"result": {"outcome": "task_queued", "task_id": "bad"}},
        {"result": {"outcome": "task_queued", "task_id": TASK_ID, "report_hash": "x"}},
        {"command_id": "another-command"},
        {"status": "succeeded", "result": "task_queued"},
        {"completed_at": None},
        {"error": "contradictory success"},
        {"enqueued_at": "not-a-time"},
    ],
)
def test_unverifiable_success_requires_original_retry(
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
        (HEADERS, {**_body(), "replica_generation_id": "fake"}, 422),
        (HEADERS, {**_body(), "collection_completed_through": None}, 422),
        (HEADERS, {**_body(), "audit_start": "2025-05-01"}, 422),
        (HEADERS, {**_body(), "observed_through": "2035-12-31"}, 422),
        (HEADERS, {**_body(), "command_id": "x" * 129}, 422),
        (HEADERS, {**_body(), "command_id": ""}, 422),
    ],
)
def test_bad_requests_never_reach_page_control(
    tmp_path: Path, headers: dict[str, str], body: dict[str, object], expected: int
) -> None:
    def must_not_submit(_payload: dict[str, object]) -> dict[str, object]:
        pytest.fail("bad request reached PageControl")

    with TestClient(_app(tmp_path / "serving", must_not_submit)) as client:
        response = client.post(PATH, json=body, headers=headers)

    assert response.status_code == expected
    assert "traceback" not in response.text.lower()


def test_oversize_and_non_json_never_reach_page_control(tmp_path: Path) -> None:
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


def test_timeout_and_unclassified_exception_preserve_original_command(tmp_path: Path) -> None:
    received: list[dict[str, object]] = []

    def transport(payload: dict[str, object]) -> dict[str, object]:
        received.append(payload)
        if len(received) == 1:
            raise TimeoutError("response lost after admission")
        if len(received) == 2:
            raise RuntimeError("/secret/unclassified error")
        return _wire(payload)

    with TestClient(_app(tmp_path / "serving", transport)) as client:
        timed_out = client.post(PATH, json=_body(), headers=HEADERS)
        unknown = client.post(PATH, json=_body(), headers=HEADERS)
        recovered = client.post(PATH, json=_body(), headers=HEADERS)

    assert timed_out.status_code == unknown.status_code == 503
    assert timed_out.json() == unknown.json()
    assert "原请求重试" in timed_out.json()["detail"]
    assert "/secret" not in unknown.text
    assert recovered.status_code == 200
    assert recovered.json()["status"] == "queued"
    assert received == [received[0], received[0], received[0]]


def test_conflict_is_distinct_from_unknown_failure(tmp_path: Path) -> None:
    def conflict(_payload: dict[str, object]) -> dict[str, object]:
        raise AuditReportCommandConflictError("different actor for existing command")

    with TestClient(_app(tmp_path / "serving", conflict)) as client:
        response = client.post(PATH, json=_body(), headers=HEADERS)

    assert response.status_code == 409
    assert "different actor" not in response.text


@pytest.mark.parametrize("upstream_status", [400, 409])
def test_unclassified_page_control_http_error_preserves_original_command(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, upstream_status: int
) -> None:
    received: list[dict[str, object]] = []

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
            received.append(json.loads(body))

        def getresponse(self) -> FakeResponse:
            return FakeResponse()

        def close(self) -> None:
            pass

    monkeypatch.setattr(
        "rquant.web.data_audit_report_command_gateway.http.client.HTTPConnection", FakeConnection
    )
    with TestClient(_app(tmp_path / "serving")) as client:
        first = client.post(PATH, json=_body(), headers=HEADERS)
        recovered = client.post(PATH, json=_body(), headers=HEADERS)

    assert first.status_code == recovered.status_code == 503
    assert first.json() == recovered.json()
    assert "原请求重试" in first.json()["detail"]
    assert received == [received[0], received[0]]


def test_invalid_page_control_http_receipt_is_not_queued(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class FakeResponse:
        status = 200

        def getheader(self, _name: str, _default: str = "") -> str:
            return "text/plain"

    class FakeConnection:
        def __init__(self, _host: str, _port: int, *, timeout: float) -> None:
            assert timeout == 1.0

        def request(
            self, _method: str, _path: str, *, body: bytes, headers: dict[str, str]
        ) -> None:
            assert json.loads(body)["command_id"] == "audit-original"
            assert headers["Content-Type"] == "application/json"

        def getresponse(self) -> FakeResponse:
            return FakeResponse()

        def close(self) -> None:
            pass

    monkeypatch.setattr(
        "rquant.web.data_audit_report_command_gateway.http.client.HTTPConnection", FakeConnection
    )
    with TestClient(_app(tmp_path / "serving")) as client:
        response = client.post(PATH, json=_body(), headers=HEADERS)

    assert response.status_code == 502
    assert "已排队" not in response.text
    assert "原请求重试" in response.json()["detail"]


def test_gateway_never_follows_redirect_or_accepts_other_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[object, ...]] = []

    class FakeResponse:
        status = 302

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
        "rquant.web.data_audit_report_command_gateway.http.client.HTTPConnection", FakeConnection
    )
    with pytest.raises(AuditReportCommandUnavailableError):
        AuditReportCommandGateway()._post({"kind": "submit_data_audit_report"})
    assert calls[0] == ("connect", "127.0.0.1", 8767, 1.0)
    assert calls[1][:3] == ("request", "POST", "/v1/commands")
    assert calls[-1] == ("close",)
    assert "outside.test" not in str(calls)
    with pytest.raises(ValueError):
        AuditReportCommandGateway(endpoint="http://localhost:8767/v1/commands")
