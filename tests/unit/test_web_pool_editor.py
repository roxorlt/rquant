"""Pool editing crosses one Serving generation and a bounded PageControl command gateway."""

from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from rquant.page_control import (
    PageControlConsumer,
    PageControlOutbox,
    PageControlService,
    PageControlStatus,
    SaveCanvas,
    parse_page_control_command,
)
from rquant.pool_definition_projection import build_pool_definition_rows
from rquant.serving_read_models import ServingProjectionPayload
from rquant.web.app import create_app
from rquant.web.pool_editor_gateway import PoolCommandGateway, PoolCommandUnavailableError
from rquant.web.settings import WebSettings
from tests.canvas_ed25519_support import create_canvas_ed25519_test_authority
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture

NOW = FIXTURE_BUILT_AT + timedelta(seconds=30)
HEADERS = {
    "x-rquant-user": "researcher",
    "x-rquant-csrf": "1",
    "origin": "http://testserver",
}


def _projection(name: str, rows: list[dict[str, object]]) -> ServingProjectionPayload:
    return ServingProjectionPayload(
        table_name=name,
        available_at=FIXTURE_BUILT_AT - timedelta(seconds=30),
        rows=tuple(rows),
    )


def _canvas_row(refs: list[str] | None = None, *, version: str = "b" * 64) -> dict[str, object]:
    return {
        "name": "观察",
        "description": "日终观察",
        "pool_refs_json": json.dumps(refs or [], ensure_ascii=False),
        "created_at": "2026-09-24T07:00:00Z",
        "updated_at": "2026-09-24T07:00:00Z",
        "source": "page_control",
        "command_id": "canvas-seed",
        "command_hash": "c" * 64,
        "source_identity_hash": "d" * 64,
        "record_hash": "e" * 64,
        "version_hash": version,
    }


def _user_row(*, version: str = "a" * 64, can_edit: bool = True) -> dict[str, object]:
    return {
        "pool_name": "user/样本池",
        "display_name": "样本池",
        "description": "日终观察",
        "source_kind": "user",
        "state": "available",
        "reason": None,
        "version": version,
        "command_id": "save-seed",
        "command_hash": "f" * 64,
        "depends_on": "n-shape-pool1",
        "delay_mode": "exact",
        "delay_days": 2,
        "rules_json": json.dumps([{"name": "not_st", "args": {}}]),
        "include_columns_json": json.dumps(["CLOSE[0]"]),
        "can_edit": can_edit,
    }


def _app(
    root: Path,
    *,
    user_rows: list[dict[str, object]] | None = None,
    canvas_rows: list[dict[str, object]] | None = None,
    transport=None,
):
    projections = (
        _projection(
            "pool_definition",
            list(build_pool_definition_rows({}, {}, root_path="/synthetic")) + (user_rows or []),
        ),
        _projection("canvas_definition", [_canvas_row()] if canvas_rows is None else canvas_rows),
    )
    build_web_fixture(root, "baseline", signal_projections=projections)
    return create_app(
        WebSettings(serving_root=root, stale_after_seconds=600),
        clock=lambda: NOW,
        background=False,
        pool_command_transport=transport,
    )


def _save_body(command_id: str, *, expected_version: str | None = None) -> dict[str, object]:
    return {
        "kind": "save_user_pool_v2",
        "command_id": command_id,
        "requested_at": NOW.isoformat(),
        "base_name": "样本池",
        "display_name": "样本池",
        "description": "日终观察",
        "rule_calls": [{"name": "not_st", "args": {}}],
        "include_columns": ["CLOSE[0]"],
        "depends_on": None,
        "delay_days": 0,
        "expected_version": expected_version,
    }


def _attach_body(command_id: str, version: str) -> dict[str, object]:
    return {
        "kind": "add_pool_to_canvas",
        "command_id": command_id,
        "requested_at": NOW.isoformat(),
        "canvas_name": "观察",
        "pool_name": "user/样本池",
        "expected_pool_version": version,
    }


def _service(root: Path) -> PageControlService:
    authority = create_canvas_ed25519_test_authority(root / "keys")
    outbox = PageControlOutbox(root / "control.sqlite3")
    return PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=root / "data",
            log_dir=root / "logs",
            clock=lambda: NOW,
            canvas_publication_signer=authority.signer,
            canvas_publication_keyring=authority.keyring,
        ),
    )


def test_editor_reads_registered_user_rules_and_verified_canvas_from_one_generation(
    tmp_path: Path,
) -> None:
    app = _app(tmp_path / "serving", user_rows=[_user_row()])
    with TestClient(app) as client:
        response = client.get("/api/v1/pools/editor")
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["state"] == "ready"
    assert data["pools"] == [
        {
            "key": "user/样本池",
            "display_name": "样本池",
            "description": "日终观察",
            "version": "a" * 64,
            "depends_on": "n-shape-pool1",
            "delay_days": 2,
            "rule_calls": [{"name": "not_st", "args": {}}],
            "include_columns": ["CLOSE[0]"],
        }
    ]
    assert data["canvases"] == [
        {"name": "观察", "description": "日终观察", "version": "b" * 64, "pool_refs": []}
    ]
    assert response.json()["serving"]["generation_id"] is not None


def test_editor_hides_noneditable_or_corrupt_rows_and_old_generation_cannot_write(
    tmp_path: Path,
) -> None:
    app = _app(
        tmp_path / "serving",
        user_rows=[_user_row(can_edit=False)],
        canvas_rows=[_canvas_row(version="bad")],
        transport=lambda _body: pytest.fail("must not reach PageControl"),
    )
    with TestClient(app) as client:
        data = client.get("/api/v1/pools/editor").json()["data"]
        assert data["pools"] == []
        assert data["canvases"] == []
        denied = client.post(
            "/api/v1/pools/editor/commands", json=_save_body("update"), headers=HEADERS
        )
    assert denied.status_code == 409

    old_root = tmp_path / "old"
    build_web_fixture(old_root, "baseline")
    old = create_app(WebSettings(serving_root=old_root), clock=lambda: NOW, background=False)
    with TestClient(old) as client:
        response = client.post(
            "/api/v1/pools/editor/commands", json=_save_body("new"), headers=HEADERS
        )
    assert response.status_code == 409


def test_editor_does_not_offer_rule_with_unregistered_parameters(tmp_path: Path) -> None:
    row = _user_row()
    row["rules_json"] = json.dumps([{"name": "not_st", "args": {"unexpected": 1}}])
    app = _app(tmp_path / "serving", user_rows=[row])
    with TestClient(app) as client:
        data = client.get("/api/v1/pools/editor").json()["data"]
    assert data["pools"] == []


def test_new_save_and_attach_resume_original_body_after_lost_response(tmp_path: Path) -> None:
    service = _service(tmp_path / "control")
    assert (
        service.submit(SaveCanvas(command_id="canvas-seed", requested_at=NOW, name="观察")).status
        is PageControlStatus.SUCCEEDED
    )
    lost = False

    def transport(body: dict[str, object]) -> dict[str, object]:
        nonlocal lost
        receipt = service.submit(parse_page_control_command(body)).model_dump(mode="json")
        if body["kind"] == "add_pool_to_canvas" and not lost:
            lost = True
            raise TimeoutError("synthetic response loss")
        return receipt

    app = _app(tmp_path / "serving", transport=transport)
    with TestClient(app) as client:
        saved = client.post(
            "/api/v1/pools/editor/commands", json=_save_body("save-new"), headers=HEADERS
        )
        assert saved.status_code == 200
        assert saved.json()["status"] == "succeeded"
        version = saved.json()["pool_version"]
        attach_body = _attach_body("attach-new", version)
        lost_response = client.post(
            "/api/v1/pools/editor/commands", json=attach_body, headers=HEADERS
        )
        assert lost_response.status_code == 503
        continued = client.post("/api/v1/pools/editor/commands", json=attach_body, headers=HEADERS)
    assert continued.status_code == 200
    assert continued.json()["status"] == "succeeded"
    assert continued.json()["canvas_name"] == "观察"
    assert service.outbox.receipt("save-new").status is PageControlStatus.SUCCEEDED
    canvas = json.loads((tmp_path / "control" / "data" / "canvases" / "观察.json").read_text())
    assert canvas["pool_refs"] == ["user/样本池"]


def test_failed_attach_uses_new_command_without_repeating_successful_save(tmp_path: Path) -> None:
    service = _service(tmp_path / "control")
    assert (
        service.submit(SaveCanvas(command_id="canvas-seed", requested_at=NOW, name="观察")).status
        is PageControlStatus.SUCCEEDED
    )
    app = _app(
        tmp_path / "serving",
        transport=lambda body: service.submit(parse_page_control_command(body)).model_dump(
            mode="json"
        ),
    )
    with TestClient(app) as client:
        saved = client.post(
            "/api/v1/pools/editor/commands", json=_save_body("save-new"), headers=HEADERS
        )
        version = saved.json()["pool_version"]
        wrong = client.post(
            "/api/v1/pools/editor/commands",
            json=_attach_body("attach-wrong", "a" * 64),
            headers=HEADERS,
        )
        assert wrong.status_code == 200
        assert wrong.json()["status"] == "failed"
        assert "data" not in wrong.json().get("message", "")
        retried = client.post(
            "/api/v1/pools/editor/commands",
            json=_attach_body("attach-right", version),
            headers=HEADERS,
        )
    assert retried.json()["status"] == "succeeded"
    assert service.outbox.receipt("save-new").status is PageControlStatus.SUCCEEDED
    assert service.outbox.receipt("attach-wrong").status is PageControlStatus.FAILED


def test_update_uses_published_version_and_stale_update_preserves_file(tmp_path: Path) -> None:
    service = _service(tmp_path / "control")
    original = service.submit(parse_page_control_command(_save_body("save-original")))
    assert isinstance(original.result, dict)
    original_version = original.result["version"]
    app = _app(
        tmp_path / "serving",
        user_rows=[_user_row(version=original_version)],
        transport=lambda body: service.submit(parse_page_control_command(body)).model_dump(
            mode="json"
        ),
    )
    update = {
        **_save_body("save-update", expected_version=original_version),
        "display_name": "新样本池",
    }
    with TestClient(app) as client:
        response = client.post("/api/v1/pools/editor/commands", json=update, headers=HEADERS)
        assert response.status_code == 200
        assert response.json()["status"] == "succeeded"
        new_version = response.json()["pool_version"]
        stale = client.post(
            "/api/v1/pools/editor/commands",
            json={**update, "command_id": "save-stale"},
            headers=HEADERS,
        )
    assert stale.json()["status"] == "failed"
    assert "规则已变化" in stale.json()["message"]
    assert new_version != original_version
    pool_path = tmp_path / "control" / "data" / "user_presets" / "样本池.json"
    assert json.loads(pool_path.read_text())["display_name"] == "新样本池"


@pytest.mark.parametrize(
    ("status", "error", "message"),
    [
        ("pending", None, "已受理"),
        ("processing", None, "正在处理"),
        (
            "failed",
            "ValueError: page control command requested_at exceeds allowed future clock skew",
            "设备时间",
        ),
        ("ambiguous", "internal/private/path", "状态待确认"),
    ],
)
def test_five_state_receipt_copy_never_exposes_internal_errors(
    tmp_path: Path, status: str, error: str | None, message: str
) -> None:
    app = _app(
        tmp_path / "serving",
        transport=lambda body: {
            "command_id": body["command_id"],
            "status": status,
            "enqueued_at": body["requested_at"],
            "result": None,
            "error": error,
        },
    )
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/pools/editor/commands", json=_save_body("save"), headers=HEADERS
        )
    assert response.status_code == 200
    assert response.json()["status"] == status
    assert message in response.json()["message"]
    assert "internal/private" not in response.text


def test_successful_attach_receipt_must_match_canvas_and_pool(tmp_path: Path) -> None:
    app = _app(
        tmp_path / "serving",
        transport=lambda body: {
            "command_id": body["command_id"],
            "status": "succeeded",
            "enqueued_at": body["requested_at"],
            "result": {
                "canvas_name": "其他画布",
                "pool_name": body["pool_name"],
                "pool_version": body["expected_pool_version"],
                "publication_receipt_id": "b" * 64,
            },
        },
    )
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/pools/editor/commands", json=_attach_body("attach", "a" * 64), headers=HEADERS
        )
    assert response.status_code == 502
    assert "其他画布" not in response.text


def test_fixed_loopback_transport_ignores_proxy_environment_and_never_follows_redirect(
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
    monkeypatch.setenv("HTTPS_PROXY", "http://outside.test:1234")
    monkeypatch.setattr("rquant.web.pool_editor_gateway.http.client.HTTPConnection", FakeConnection)
    gateway = PoolCommandGateway()
    with pytest.raises(PoolCommandUnavailableError):
        gateway._post(_save_body("redirect"))
    assert calls[0] == ("connect", "127.0.0.1", 8767, 1.0)
    assert calls[1][0:3] == ("request", "POST", "/v1/commands")
    assert calls[-1] == ("close",)
    assert "outside.test" not in str(calls)


def test_nonloopback_command_target_is_rejected_before_any_transport() -> None:
    with pytest.raises(ValidationError):
        WebSettings(
            serving_root=Path("/private/tmp/synthetic"),
            page_control_url="http://outside.test/v1/commands",
        )
    with pytest.raises(ValueError):
        PoolCommandGateway(endpoint="http://localhost:8767/v1/commands")


@pytest.mark.parametrize(
    ("headers", "body", "expected"),
    [
        ({"x-rquant-csrf": "1", "origin": "http://testserver"}, _save_body("x"), 401),
        ({"x-rquant-user": "researcher", "origin": "http://testserver"}, _save_body("x"), 403),
        ({**HEADERS, "origin": "https://elsewhere.test"}, _save_body("x"), 403),
        (HEADERS, {**_save_body("x"), "base_name": "../bad"}, 422),
        (HEADERS, {**_save_body("x"), "description": "a" * 40_000}, 413),
    ],
)
def test_write_rejects_unauthorized_or_unbounded_input(
    tmp_path: Path, headers: dict[str, str], body: dict[str, object], expected: int
) -> None:
    app = _app(tmp_path / "serving", transport=lambda _body: pytest.fail("must not submit"))
    with TestClient(app) as client:
        response = client.post("/api/v1/pools/editor/commands", json=body, headers=headers)
    assert response.status_code == expected


def test_malformed_success_receipt_cannot_claim_a_saved_pool(tmp_path: Path) -> None:
    app = _app(
        tmp_path / "serving",
        transport=lambda body: {
            "command_id": body["command_id"],
            "status": "succeeded",
            "enqueued_at": body["requested_at"],
            "result": {"version": "not-a-version"},
        },
    )
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/pools/editor/commands", json=_save_body("save"), headers=HEADERS
        )
    assert response.status_code == 502
    assert "not-a-version" not in response.text


def test_write_rejects_non_json_content_type_before_submission(tmp_path: Path) -> None:
    app = _app(tmp_path / "serving", transport=lambda _body: pytest.fail("must not submit"))
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/pools/editor/commands",
            content=json.dumps(_save_body("wrong-type")),
            headers={**HEADERS, "content-type": "text/plain"},
        )
    assert response.status_code == 415
