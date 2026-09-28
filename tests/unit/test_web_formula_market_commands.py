"""Formula market admission is a typed same-origin command, never an in-Web run."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from rquant.page_control import SubmitFormulaMarketRun, parse_page_control_command
from rquant.web.formula_market_command_gateway import (
    FormulaMarketCommandConflictError,
    FormulaMarketCommandGateway,
)
from rquant.web.settings import WebSettings
from tests.support.web_proxy_identity import ProofTestClient as TestClient
from tests.support.web_proxy_identity import create_private_test_app as create_app

NOW = datetime(2026, 9, 28, 5, tzinfo=UTC)
HEADERS = {
    "x-rquant-user": "researcher",
    "x-rquant-csrf": "1",
    "origin": "http://testserver",
}
PATH = "/api/v1/screen/tdx/market/commands"
TASK_ID = "a" * 32


def _body(command_id: str = "market-command-0001") -> dict[str, str]:
    return {
        "command_id": command_id,
        "requested_at": NOW.isoformat(),
        "trade_date": "2026-04-15",
        "formula": "CLOSE>2",
    }


def _wire(payload: dict[str, object], *, status: str = "succeeded") -> dict[str, object]:
    return {
        "command_id": payload["command_id"],
        "status": status,
        "enqueued_at": payload["requested_at"],
        "completed_at": payload["requested_at"] if status == "succeeded" else None,
        "result": {"outcome": "task_queued", "task_id": TASK_ID},
        "error": None,
    }


def _app(root: Path, transport=None):
    return create_app(
        WebSettings(serving_root=root),
        clock=lambda: NOW,
        background=False,
        formula_market_command_transport=transport,
    )


def test_success_forwards_only_typed_input_with_server_actor(tmp_path: Path) -> None:
    received: list[dict[str, object]] = []

    def transport(payload: dict[str, object]) -> dict[str, object]:
        received.append(payload)
        assert isinstance(parse_page_control_command(payload), SubmitFormulaMarketRun)
        return _wire(payload)

    with TestClient(_app(tmp_path / "serving", transport)) as client:
        response = client.post(PATH, json=_body(), headers=HEADERS)

    assert response.status_code == 200
    assert response.json() == {
        "command_id": "market-command-0001",
        "status": "queued",
        "task_id": TASK_ID,
        "message": "已提交，等待选股结果。",
    }
    assert received == [{
        "kind": "submit_formula_market_run",
        **{**_body(), "requested_at": "2026-09-28T05:00:00Z"},
        "actor_id": "researcher",
    }]


@pytest.mark.parametrize(
    ("headers", "body", "expected"),
    [
        ({"x-rquant-csrf": "1", "origin": "http://testserver"}, _body(), 401),
        ({"x-rquant-user": "researcher", "origin": "http://testserver"}, _body(), 403),
        ({**HEADERS, "origin": "https://other.test"}, _body(), 403),
        (HEADERS, {**_body(), "actor_id": "admin"}, 422),
        (HEADERS, {**_body(), "universe_root": "/secret/list"}, 422),
        (HEADERS, {**_body(), "expected_projection_identity": "a" * 64}, 422),
        (HEADERS, {**_body(), "trade_date": "not-a-date"}, 422),
        (HEADERS, {**_body(), "formula": ""}, 422),
        (HEADERS, {**_body(), "formula": "x" * 4097}, 413),
    ],
)
def test_bad_browser_input_never_reaches_control(
    tmp_path: Path, headers: dict[str, str], body: dict[str, object], expected: int,
) -> None:
    def forbidden(_payload: dict[str, object]) -> dict[str, object]:
        pytest.fail("bad request reached PageControl")

    with TestClient(_app(tmp_path / "serving", forbidden)) as client:
        response = client.post(PATH, json=body, headers=headers)
    assert response.status_code == expected
    assert "traceback" not in response.text.lower()


def test_non_json_oversize_and_non_success_never_claim_queued(tmp_path: Path) -> None:
    def pending(payload: dict[str, object]) -> dict[str, object]:
        return {**_wire(payload, status="ambiguous"), "error": "/secret/error"}

    with TestClient(_app(tmp_path / "serving", pending)) as client:
        ambiguous = client.post(PATH, json=_body(), headers=HEADERS)
        oversized = client.post(
            PATH,
            content=json.dumps({**_body(), "padding": "x" * 9_000}),
            headers={**HEADERS, "content-type": "application/json"},
        )
        wrong_type = client.post(
            PATH,
            content=json.dumps(_body()),
            headers={**HEADERS, "content-type": "text/plain"},
        )
    assert ambiguous.status_code == 200
    assert ambiguous.json()["status"] == "ambiguous"
    assert ambiguous.json()["task_id"] is None
    assert "/secret" not in ambiguous.text
    assert (oversized.status_code, wrong_type.status_code) == (413, 415)


def test_command_conflict_and_transport_uncertainty_are_distinct(tmp_path: Path) -> None:
    def conflict(_payload: dict[str, object]) -> dict[str, object]:
        raise FormulaMarketCommandConflictError("different command")

    with TestClient(_app(tmp_path / "serving", conflict)) as client:
        response = client.post(PATH, json=_body(), headers=HEADERS)
    assert response.status_code == 409
    assert response.json()["status"] == "conflict"
    assert response.json()["task_id"] is None
    assert "different command" not in response.text

    def timeout(_payload: dict[str, object]) -> dict[str, object]:
        raise TimeoutError("lost after enqueue")

    with TestClient(_app(tmp_path / "serving", timeout)) as client:
        unknown = client.post(PATH, json=_body(), headers=HEADERS)
    assert unknown.status_code == 503
    assert "原请求重试" in unknown.json()["detail"]


def test_active_task_conflict_is_typed_and_does_not_claim_queued(tmp_path: Path) -> None:
    def active(payload: dict[str, object]) -> dict[str, object]:
        return {
            **_wire(payload),
            "result": {"outcome": "task_conflict", "reason": "task_active"},
        }

    with TestClient(_app(tmp_path / "serving", active)) as client:
        response = client.post(PATH, json=_body(), headers=HEADERS)
    assert response.status_code == 409
    assert response.json() == {
        "command_id": "market-command-0001",
        "status": "conflict",
        "task_id": None,
        "message": "已有选股任务，完成后再试。",
    }


def test_invalid_success_receipt_never_claims_queued(tmp_path: Path) -> None:
    with TestClient(
        _app(tmp_path / "serving", lambda payload: {
            **_wire(payload), "result": {"outcome": "task_queued", "task_id": "bad"}
        })
    ) as client:
        response = client.post(PATH, json=_body(), headers=HEADERS)
    assert response.status_code == 502
    assert "原请求重试" in response.json()["detail"]


def test_gateway_fixed_loopback_treats_409_as_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str]] = []

    class FakeResponse:
        status = 409

    class FakeConnection:
        def __init__(self, host: str, port: int, *, timeout: float) -> None:
            assert (host, port, timeout) == ("127.0.0.1", 8767, 1.0)

        def request(
            self, method: str, path: str, *, body: bytes, headers: dict[str, str]
        ) -> None:
            calls.append((method, path))
            assert json.loads(body)["kind"] == "submit_formula_market_run"
            assert headers["Content-Type"] == "application/json"

        def getresponse(self) -> FakeResponse:
            return FakeResponse()

        def close(self) -> None:
            pass

    monkeypatch.setattr(
        "rquant.web.formula_market_command_gateway.http.client.HTTPConnection", FakeConnection
    )
    with pytest.raises(FormulaMarketCommandConflictError):
        FormulaMarketCommandGateway().submit({"kind": "submit_formula_market_run"})
    assert calls == [("POST", "/v1/commands")]
    with pytest.raises(ValueError):
        FormulaMarketCommandGateway(endpoint="http://localhost:8767/v1/commands")
