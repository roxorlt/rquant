"""Formula pool saving crosses Web only as a typed, authenticated PageControl command."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from rquant.page_control import SaveFormulaPoolV1, parse_page_control_command
from rquant.web.formula_market_command_gateway import FormulaMarketCommandConflictError
from rquant.web.settings import WebSettings
from tests.support.web_proxy_identity import ProofTestClient as TestClient
from tests.support.web_proxy_identity import create_private_test_app as create_app

NOW = datetime(2026, 9, 28, 5, tzinfo=UTC)
PATH = "/api/v1/pools/formula/commands"
HEADERS = {
    "x-rquant-user": "researcher",
    "x-rquant-csrf": "1",
    "origin": "http://testserver",
}
TASK_ID = "a" * 32
VERSION = "b" * 64


def _body(command_id: str = "save-formula-pool-1") -> dict[str, object]:
    return {
        "command_id": command_id,
        "requested_at": NOW.isoformat(),
        "base_name": "research",
        "display_name": "研究池",
        "task_id": TASK_ID,
        "expected_version": None,
    }


def _wire(payload: dict[str, object], status: str = "succeeded") -> dict[str, object]:
    return {
        "command_id": payload["command_id"],
        "status": status,
        "enqueued_at": payload["requested_at"],
        "completed_at": payload["requested_at"] if status in {"succeeded", "failed"} else None,
        "result": {"pool_name": "user/research", "version": VERSION}
        if status == "succeeded"
        else None,
        "error": None,
    }


def _app(root: Path, transport=None):
    return create_app(
        WebSettings(serving_root=root),
        clock=lambda: NOW,
        background=False,
        formula_market_command_transport=transport,
    )


def test_success_forwards_only_typed_identifiers_with_server_actor_and_exact_retry(
    tmp_path: Path,
) -> None:
    received: list[dict[str, object]] = []

    def transport(payload: dict[str, object]) -> dict[str, object]:
        received.append(payload)
        assert isinstance(parse_page_control_command(payload), SaveFormulaPoolV1)
        return _wire(payload)

    with TestClient(_app(tmp_path / "serving", transport)) as client:
        first = client.post(PATH, json=_body(), headers=HEADERS)
        retry = client.post(PATH, json=_body(), headers=HEADERS)

    expected = {
        "command_id": "save-formula-pool-1",
        "status": "succeeded",
        "pool_name": "user/research",
        "version": VERSION,
        "message": "公式池已保存。",
    }
    assert first.status_code == retry.status_code == 200
    assert first.json() == retry.json() == expected
    forwarded = {
        "kind": "save_formula_pool_v1",
        **(_body() | {"requested_at": "2026-09-28T05:00:00Z"}),
        "actor_id": "researcher",
    }
    assert received == [forwarded, forwarded]


@pytest.mark.parametrize(
    ("headers", "body", "expected"),
    [
        ({"x-rquant-csrf": "1", "origin": "http://testserver"}, _body(), 401),
        ({"x-rquant-user": "researcher", "origin": "http://testserver"}, _body(), 403),
        ({**HEADERS, "origin": "https://other.test"}, _body(), 403),
        (HEADERS, {**_body(), "actor_id": "admin"}, 422),
        (HEADERS, {**_body(), "formula": "CLOSE>2"}, 422),
        (HEADERS, {**_body(), "match_codes": ["600001.SH"]}, 422),
        (HEADERS, {**_body(), "artifact_directory": "/private/path"}, 422),
        (HEADERS, {**_body(), "expected_version": VERSION}, 422),
        (HEADERS, {key: value for key, value in _body().items() if key != "expected_version"}, 422),
        (HEADERS, {**_body(), "task_id": "bad"}, 422),
        (HEADERS, {**_body(), "base_name": "../escape"}, 422),
        (HEADERS, {**_body(), "display_name": " "}, 422),
        (HEADERS, {**_body(), "requested_at": "2026-09-28T05:00:00"}, 422),
    ],
)
def test_bad_browser_input_never_reaches_page_control(
    tmp_path: Path,
    headers: dict[str, str],
    body: dict[str, object],
    expected: int,
) -> None:
    def forbidden(_payload: dict[str, object]) -> dict[str, object]:
        pytest.fail("bad request reached PageControl")

    with TestClient(_app(tmp_path / "serving", forbidden)) as client:
        response = client.post(PATH, json=body, headers=headers)
    assert response.status_code == expected
    assert "/private/path" not in response.text


def test_request_size_and_content_type_are_bounded_before_validation(tmp_path: Path) -> None:
    def forbidden(_payload: dict[str, object]) -> dict[str, object]:
        pytest.fail("bad request reached PageControl")

    with TestClient(_app(tmp_path / "serving", forbidden)) as client:
        oversized = client.post(
            PATH,
            content=json.dumps(_body() | {"padding": "x" * 9_000}),
            headers=HEADERS | {"content-type": "application/json"},
        )
        wrong_type = client.post(
            PATH,
            content=json.dumps(_body()),
            headers=HEADERS | {"content-type": "text/plain"},
        )
    assert (oversized.status_code, wrong_type.status_code) == (413, 415)


@pytest.mark.parametrize("status", ["pending", "processing", "ambiguous", "failed"])
def test_non_success_never_claims_a_saved_pool(
    tmp_path: Path,
    status: str,
) -> None:
    def transport(payload: dict[str, object]) -> dict[str, object]:
        return _wire(payload, status) | {"error": "/private/error"}

    with TestClient(_app(tmp_path / "serving", transport)) as client:
        response = client.post(PATH, json=_body(), headers=HEADERS)
    assert response.status_code == 200
    assert response.json()["status"] == status
    assert response.json()["pool_name"] is None
    assert response.json()["version"] is None
    assert "/private/error" not in response.text
    if status != "failed":
        assert "原请求" in response.json()["message"]


@pytest.mark.parametrize(
    "changes",
    [
        {"result": None},
        {"result": {"pool_name": "user/another", "version": VERSION}},
        {"result": {"pool_name": "user/research", "version": "bad"}},
        {"result": {"pool_name": "user/research", "version": VERSION, "path": "/private/path"}},
        {"completed_at": None},
        {"error": "/private/error"},
        {"command_id": "different-command"},
    ],
)
def test_malformed_success_receipt_cannot_claim_a_saved_pool(
    tmp_path: Path,
    changes: dict[str, object],
) -> None:
    with TestClient(_app(tmp_path / "serving", lambda payload: _wire(payload) | changes)) as client:
        response = client.post(PATH, json=_body(), headers=HEADERS)
    assert response.status_code == 502
    assert "原请求" in response.json()["detail"]
    assert "/private" not in response.text


def test_conflict_and_unknown_transport_result_keep_distinct_public_status(
    tmp_path: Path,
) -> None:
    def conflict(_payload: dict[str, object]) -> dict[str, object]:
        raise FormulaMarketCommandConflictError("other actor or command")

    with TestClient(_app(tmp_path / "serving", conflict)) as client:
        response = client.post(PATH, json=_body(), headers=HEADERS)
    assert response.status_code == 409
    assert response.json()["status"] == "conflict"
    assert response.json()["pool_name"] is None
    assert "other actor" not in response.text

    def timeout(_payload: dict[str, object]) -> dict[str, object]:
        raise TimeoutError("response lost after save")

    with TestClient(_app(tmp_path / "serving", timeout)) as client:
        unknown = client.post(PATH, json=_body(), headers=HEADERS)
    assert unknown.status_code == 503
    assert "原请求" in unknown.json()["detail"]
    assert "response lost" not in unknown.text
