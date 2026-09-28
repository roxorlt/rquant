"""Pool editing crosses one Serving generation and a bounded PageControl command gateway."""

from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

import duckdb
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
from rquant.web.models.pool_editor import SavePoolCommand, SaveRankedPoolCommand
from rquant.web.pool_editor_gateway import PoolCommandGateway, PoolCommandUnavailableError
from rquant.web.pool_editor_read import read_pool_editor
from rquant.web.routes.pool_editor import _failed_message
from rquant.web.serving import BorrowedGeneration
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
        "ranking_json": None,
        "can_edit": can_edit,
    }


def _app(
    root: Path,
    *,
    user_rows: list[dict[str, object]] | None = None,
    builtin_rows: list[dict[str, object]] | None = None,
    canvas_rows: list[dict[str, object]] | None = None,
    transport=None,
):
    projections = (
        _projection(
            "pool_definition",
            (
                list(build_pool_definition_rows({}, {}, root_path="/synthetic"))
                if builtin_rows is None
                else builtin_rows
            )
            + (user_rows or []),
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


def _ranked_save_body(command_id: str, *, expected_version: str | None = None) -> dict[str, object]:
    return {
        **_save_body(command_id, expected_version=expected_version),
        "kind": "save_user_pool_v3",
        "ranking": {
            "conditions": [
                {"metric": "RETURN_20D_PCT[0]", "ascending": False, "weight": 60},
                {"metric": "TURNOVER_RATE[0]", "ascending": False, "weight": 40},
            ],
            "top_n": 20,
        },
    }


def test_pool_editor_rejects_unpublished_fundamental_rules(tmp_path: Path) -> None:
    app = _app(tmp_path / "serving")
    body = _save_body("fundamental-blocked")
    body["rule_calls"] = [{"name": "gt", "args": {"left": "PE_TTM[0]", "right": 9}}]
    with TestClient(app) as client:
        rejected_rule = client.post("/api/v1/pools/editor/commands", json=body, headers=HEADERS)
        body["rule_calls"] = [{"name": "not_st", "args": {}}]
        body["include_columns"] = ["ROE[0]"]
        rejected_column = client.post("/api/v1/pools/editor/commands", json=body, headers=HEADERS)
    assert rejected_rule.status_code == rejected_column.status_code == 422
    assert "暂不可用" in rejected_rule.json()["detail"]


def _attach_body(command_id: str, version: str) -> dict[str, object]:
    return {
        "kind": "add_pool_to_canvas",
        "command_id": command_id,
        "requested_at": NOW.isoformat(),
        "canvas_name": "观察",
        "pool_name": "user/样本池",
        "expected_pool_version": version,
    }


def _create_body(command_id: str, *, name: str = "新画布") -> dict[str, object]:
    return {
        "kind": "create_canvas",
        "command_id": command_id,
        "requested_at": NOW.isoformat(),
        "name": name,
        "description": "日终观察",
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
            "ranking": None,
            "save_kind": "save_user_pool_v2",
        }
    ]
    assert data["canvases"] == [
        {
            "name": "观察",
            "description": "日终观察",
            "version": "b" * 64,
            "pool_refs": [],
            "command_id": "canvas-seed",
            "record_hash": "e" * 64,
        }
    ]
    assert data["canvas_create_available"] is True
    assert response.json()["serving"]["generation_id"] is not None


def test_ranked_pool_readback_keeps_plan_and_rejects_v2_overwrite(tmp_path: Path) -> None:
    row = _user_row()
    ranking = _ranked_save_body("ranked-read")["ranking"]
    row["ranking_json"] = json.dumps(ranking)
    submitted: list[dict[str, object]] = []

    def transport(body: dict[str, object]) -> dict[str, object]:
        submitted.append(body)
        return {
            "command_id": body["command_id"],
            "status": "succeeded",
            "enqueued_at": body["requested_at"],
            "result": {"version": "b" * 64},
        }

    app = _app(
        tmp_path / "serving",
        user_rows=[row],
        transport=transport,
    )
    with TestClient(app) as client:
        data = client.get("/api/v1/pools/editor").json()["data"]
        refused = client.post(
            "/api/v1/pools/editor/commands",
            json=_save_body("erase-ranking", expected_version="a" * 64),
            headers=HEADERS,
        )
    assert data["pools"][0]["ranking"] == ranking
    assert data["pools"][0]["save_kind"] == "save_user_pool_v3"
    assert refused.status_code == 409
    assert "排名" in refused.json()["detail"]
    assert submitted == []


def test_unranked_v3_readback_keeps_v3_edit_command_and_explicit_null(tmp_path: Path) -> None:
    row = _user_row()
    row["ranking_json"] = "null"
    submitted: list[dict[str, object]] = []

    def transport(body: dict[str, object]) -> dict[str, object]:
        submitted.append(body)
        return {
            "command_id": body["command_id"],
            "status": "succeeded",
            "enqueued_at": body["requested_at"],
            "result": {"version": "b" * 64},
        }

    app = _app(tmp_path / "serving", user_rows=[row], transport=transport)
    with TestClient(app) as client:
        pool = client.get("/api/v1/pools/editor").json()["data"]["pools"][0]
        legacy_update = client.post(
            "/api/v1/pools/editor/commands",
            json=_save_body("legacy-update", expected_version="a" * 64),
            headers=HEADERS,
        )
        v3_update = client.post(
            "/api/v1/pools/editor/commands",
            json={**_ranked_save_body("v3-update", expected_version="a" * 64), "ranking": None},
            headers=HEADERS,
        )
    assert pool["ranking"] is None
    assert pool["save_kind"] == "save_user_pool_v3"
    assert legacy_update.status_code == 409
    assert v3_update.status_code == 200
    assert len(submitted) == 1
    assert submitted[0]["kind"] == "save_user_pool_v3"
    assert "ranking" in submitted[0] and submitted[0]["ranking"] is None


def test_v3_save_requires_explicit_ranking_field_even_when_empty(tmp_path: Path) -> None:
    body = _ranked_save_body("missing-ranking")
    body.pop("ranking")
    app = _app(tmp_path / "serving", transport=lambda _body: pytest.fail("must not submit"))
    with TestClient(app) as client:
        response = client.post("/api/v1/pools/editor/commands", json=body, headers=HEADERS)
    assert response.status_code == 422


def test_old_serving_pool_definition_without_ranking_column_remains_editable() -> None:
    row = _user_row()
    columns = (
        "pool_name",
        "display_name",
        "description",
        "source_kind",
        "state",
        "version",
        "depends_on",
        "delay_mode",
        "delay_days",
        "rules_json",
        "include_columns_json",
        "can_edit",
    )
    connection = duckdb.connect(":memory:")
    try:
        connection.execute(
            "CREATE TABLE projection_status (table_name VARCHAR, available BOOLEAN, "
            "row_count INTEGER, available_at TIMESTAMPTZ)"
        )
        connection.execute(
            "INSERT INTO projection_status VALUES ('pool_definition', true, 1, NULL)"
        )
        connection.execute(
            "CREATE TABLE pool_definition (pool_name VARCHAR, display_name VARCHAR, "
            "description VARCHAR, source_kind VARCHAR, state VARCHAR, version VARCHAR, "
            "depends_on VARCHAR, delay_mode VARCHAR, delay_days INTEGER, rules_json VARCHAR, "
            "include_columns_json VARCHAR, can_edit BOOLEAN)"
        )
        connection.execute(
            "INSERT INTO pool_definition VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            tuple(row[name] for name in columns),
        )
        snapshot = read_pool_editor(BorrowedGeneration(None, None, connection.cursor(), None))
    finally:
        connection.close()
    assert snapshot.data.state == "ready"
    assert len(snapshot.data.pools) == 1
    assert snapshot.data.pools[0].ranking is None


def test_invalid_published_ranking_is_not_editable(tmp_path: Path) -> None:
    bad = _user_row()
    bad["ranking_json"] = json.dumps(
        {"conditions": [{"metric": "OTHER", "ascending": False, "weight": 100}], "top_n": 10}
    )
    app = _app(
        tmp_path / "serving",
        user_rows=[bad],
        transport=lambda _body: pytest.fail("invalid pool must not be edited"),
    )
    with TestClient(app) as client:
        data = client.get("/api/v1/pools/editor").json()["data"]
        refused = client.post(
            "/api/v1/pools/editor/commands",
            json=_ranked_save_body("bad-ranking", expected_version="a" * 64),
            headers=HEADERS,
        )
    assert data["pools"] == []
    assert refused.status_code == 409


@pytest.mark.parametrize(
    "ranking",
    [
        {"conditions": [], "top_n": 10},
        {"conditions": [{"metric": "OTHER", "ascending": False, "weight": 100}], "top_n": 10},
        {
            "conditions": [{"metric": "RETURN_20D_PCT[0]", "ascending": False, "weight": 100}],
            "top_n": 0,
        },
        {
            "conditions": [{"metric": "RETURN_20D_PCT[0]", "ascending": False, "weight": -1}],
            "top_n": 10,
        },
        {
            "conditions": [{"metric": "RETURN_20D_PCT[0]", "ascending": False, "weight": 0}],
            "top_n": 10,
        },
        {
            "conditions": [{"metric": "RETURN_20D_PCT[0]", "ascending": "false", "weight": 100}],
            "top_n": 10,
        },
    ],
)
def test_ranked_save_rejects_invalid_plan_before_submission(
    tmp_path: Path, ranking: object
) -> None:
    body = _ranked_save_body("invalid-ranking")
    body["ranking"] = ranking
    app = _app(tmp_path / "serving", transport=lambda _body: pytest.fail("must not submit"))
    with TestClient(app) as client:
        response = client.post("/api/v1/pools/editor/commands", json=body, headers=HEADERS)
    assert response.status_code == 422


def test_ranked_save_uses_same_auth_csrf_and_typed_receipt(tmp_path: Path) -> None:
    submitted: list[dict[str, object]] = []

    def transport(body: dict[str, object]) -> dict[str, object]:
        submitted.append(body)
        return {
            "command_id": body["command_id"],
            "status": "succeeded",
            "enqueued_at": body["requested_at"],
            "result": {"version": "b" * 64},
        }

    app = _app(tmp_path / "serving", transport=transport)
    body = _ranked_save_body("ranked-create")
    with TestClient(app) as client:
        unauthenticated = client.post(
            "/api/v1/pools/editor/commands",
            json=body,
            headers={key: value for key, value in HEADERS.items() if key != "x-rquant-user"},
        )
        no_csrf = client.post(
            "/api/v1/pools/editor/commands",
            json=body,
            headers={"x-rquant-user": "researcher"},
        )
        accepted = client.post("/api/v1/pools/editor/commands", json=body, headers=HEADERS)
    assert unauthenticated.status_code == 401
    assert no_csrf.status_code == 403
    assert accepted.status_code == 200
    assert accepted.json()["pool_version"] == "b" * 64
    assert accepted.json()["message"] == "池子已保存"
    assert len(submitted) == 1
    assert submitted[0]["kind"] == "save_user_pool_v3"
    assert submitted[0]["ranking"] == body["ranking"]


def test_v3_name_conflict_guides_new_pool_to_rename_but_edit_to_refresh() -> None:
    error = "ValueError: pool version conflict: definition changed since it was read"
    new_pool = SaveRankedPoolCommand.model_validate(_ranked_save_body("new-pool"))
    editing = SaveRankedPoolCommand.model_validate(
        _ranked_save_body("edit-pool", expected_version="a" * 64)
    )
    legacy_new = SavePoolCommand.model_validate(_save_body("legacy-new"))

    assert _failed_message(error, new_pool) == "池子名称已被使用，请换一个名称。"
    assert _failed_message(error, editing) == "规则已变化，请刷新后重试。"
    assert _failed_message(error, legacy_new) == "规则已变化，请刷新后重试。"


def test_create_canvas_lost_response_keeps_identity_and_waits_for_serving(tmp_path: Path) -> None:
    service = _service(tmp_path / "control")
    lost = False

    def transport(body: dict[str, object]) -> dict[str, object]:
        nonlocal lost
        receipt = service.submit(parse_page_control_command(body)).model_dump(mode="json")
        if body["kind"] == "create_canvas" and not lost:
            lost = True
            raise TimeoutError("synthetic response loss")
        return receipt

    app = _app(tmp_path / "serving", canvas_rows=[], transport=transport)
    body = _create_body("create-new")
    with TestClient(app) as client:
        first = client.post("/api/v1/pools/editor/commands", json=body, headers=HEADERS)
        pending = client.get("/api/v1/pools/editor").json()["data"]
        resumed = client.post("/api/v1/pools/editor/commands", json=body, headers=HEADERS)
    assert first.status_code == 503
    assert pending["canvases"] == []
    assert resumed.status_code == 200
    assert resumed.json()["status"] == "succeeded"
    assert resumed.json()["canvas_name"] == "新画布"
    assert len(resumed.json()["canvas_record_hash"]) == 64
    assert "已可用" not in resumed.json()["message"]
    assert service.outbox.receipt("create-new").status is PageControlStatus.SUCCEEDED
    record = json.loads((tmp_path / "control" / "data" / "canvases" / "新画布.json").read_text())
    assert record["pool_refs"] == []
    assert record["record_hash"] == resumed.json()["canvas_record_hash"]


def test_create_canvas_rejects_stale_serving_name_without_overwrite(tmp_path: Path) -> None:
    service = _service(tmp_path / "control")
    existing = service.submit(SaveCanvas(command_id="seed", requested_at=NOW, name="观察"))
    assert existing.status is PageControlStatus.SUCCEEDED
    path = tmp_path / "control" / "data" / "canvases" / "观察.json"
    original = path.read_bytes()
    app = _app(
        tmp_path / "serving",
        canvas_rows=[],
        transport=lambda body: service.submit(parse_page_control_command(body)).model_dump(
            mode="json"
        ),
    )
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/pools/editor/commands",
            json=_create_body("create-stale", name="观察"),
            headers=HEADERS,
        )
    assert response.status_code == 200
    assert response.json()["status"] == "failed"
    assert "已被使用" in response.json()["message"]
    assert "canvas" not in response.json()["message"]
    assert path.read_bytes() == original


def test_create_canvas_requires_current_canvas_projection(tmp_path: Path) -> None:
    root = tmp_path / "old"
    build_web_fixture(
        root,
        "baseline",
        signal_projections=(
            _projection(
                "pool_definition",
                list(build_pool_definition_rows({}, {}, root_path="/synthetic")),
            ),
        ),
    )
    app = create_app(
        WebSettings(serving_root=root),
        clock=lambda: NOW,
        background=False,
        pool_command_transport=lambda _body: pytest.fail("must not submit"),
    )
    with TestClient(app) as client:
        data = client.get("/api/v1/pools/editor").json()["data"]
        denied = client.post(
            "/api/v1/pools/editor/commands", json=_create_body("create-old"), headers=HEADERS
        )
    assert data["canvas_create_available"] is False
    assert denied.status_code == 409
    assert "画布" in denied.json()["detail"]

    older_root = tmp_path / "older"
    build_web_fixture(older_root, "baseline")
    older = create_app(
        WebSettings(serving_root=older_root),
        clock=lambda: NOW,
        background=False,
        pool_command_transport=lambda _body: pytest.fail("must not submit"),
    )
    with TestClient(older) as client:
        response = client.post(
            "/api/v1/pools/editor/commands", json=_create_body("create-older"), headers=HEADERS
        )
    assert response.status_code == 409
    assert "画布" in response.json()["detail"]


def test_editor_exposes_bounded_builtin_copy_sources_with_original_semantics(
    tmp_path: Path,
) -> None:
    app = _app(tmp_path / "serving")
    with TestClient(app) as client:
        response = client.get("/api/v1/pools/editor")
    assert response.status_code == 200
    sources = {source["key"]: source for source in response.json()["data"]["copy_sources"]}
    assert set(sources) == {"n-shape-pool1", "n-shape-pool2"}
    first = sources["n-shape-pool1"]
    assert first["display_name"] == "N 形态一池"
    assert first["description"] == "昨首板、安全过滤与下影线"
    assert len(first["version"]) == 64
    assert first["depends_on"] is None
    assert first["delay_mode"] == "none"
    assert first["delay_days"] == 0
    assert first["copyable"] is True
    assert first["copy_block_reason"] is None
    assert first["ranking"] is None
    assert {"name": "circ_mv_lt", "args": {"threshold_yi": 150}} in first["rule_calls"]
    assert "CIRC_MV[0]" in first["include_columns"]

    second = sources["n-shape-pool2"]
    assert second["depends_on"] == "n-shape-pool1"
    assert second["delay_mode"] == "legacy_window"
    assert second["delay_days"] == 2
    assert second["copyable"] is False
    assert second["copy_block_reason"] == "旧版时间窗口与精确延后日不同，暂不能无损复制。"
    assert {"name": "lt", "args": {"left": "BODY_UPPER[0]", "right": "BODY_UPPER[1]"}} in second[
        "rule_calls"
    ]


def test_editor_copy_source_uses_published_rules_and_hides_invalid_registered_args(
    tmp_path: Path,
) -> None:
    rows = list(build_pool_definition_rows({}, {}, root_path="/synthetic"))
    changed = next(row for row in rows if row["pool_name"] == "n-shape-pool1")
    changed["version"] = "c" * 64
    changed["rules_json"] = json.dumps([{"name": "circ_mv_lt", "args": {"threshold_yi": 42}}])
    app = _app(tmp_path / "published", builtin_rows=rows)
    with TestClient(app) as client:
        data = client.get("/api/v1/pools/editor").json()["data"]
    first = next(source for source in data["copy_sources"] if source["key"] == "n-shape-pool1")
    assert first["version"] == "c" * 64
    assert first["rule_calls"] == [{"name": "circ_mv_lt", "args": {"threshold_yi": 42}}]

    changed["rules_json"] = json.dumps([{"name": "circ_mv_lt", "args": {"unknown": 42}}])
    invalid_app = _app(tmp_path / "invalid", builtin_rows=rows)
    with TestClient(invalid_app) as client:
        data = client.get("/api/v1/pools/editor").json()["data"]
    assert [source["key"] for source in data["copy_sources"]] == ["n-shape-pool2"]


def test_editor_empty_builtin_rules_cannot_be_copied_or_saved_directly(tmp_path: Path) -> None:
    rows = list(build_pool_definition_rows({}, {}, root_path="/synthetic"))
    first = next(row for row in rows if row["pool_name"] == "n-shape-pool1")
    first["rules_json"] = "[]"
    app = _app(
        tmp_path / "serving",
        builtin_rows=rows,
        transport=lambda _body: pytest.fail("builtin must not reach PageControl"),
    )
    with TestClient(app) as client:
        data = client.get("/api/v1/pools/editor").json()["data"]
        denied = client.post(
            "/api/v1/pools/editor/commands",
            json={**_save_body("builtin-direct"), "base_name": "n-shape-pool1"},
            headers=HEADERS,
        )
    source = next(source for source in data["copy_sources"] if source["key"] == "n-shape-pool1")
    assert source["copyable"] is False
    assert source["copy_block_reason"] == "没有可复制的选股条件。"
    assert denied.status_code == 409


def test_old_generation_has_no_copy_sources(tmp_path: Path) -> None:
    root = tmp_path / "old"
    build_web_fixture(root, "baseline")
    app = create_app(WebSettings(serving_root=root), clock=lambda: NOW, background=False)
    with TestClient(app) as client:
        data = client.get("/api/v1/pools/editor").json()["data"]
    assert data["state"] == "unavailable"
    assert data["copy_sources"] == []


@pytest.mark.parametrize("count", [65, 256])
def test_editor_keeps_canvas_visible_through_authoritative_ref_limit(
    tmp_path: Path, count: int
) -> None:
    refs = [f"user/pool-{index:03d}" for index in range(count)]
    app = _app(tmp_path / "serving", canvas_rows=[_canvas_row(refs)])
    with TestClient(app) as client:
        response = client.get("/api/v1/pools/editor")
    assert response.status_code == 200
    assert response.json()["data"]["canvases"][0]["pool_refs"] == refs


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
        assert data["canvas_create_available"] is False
        denied = client.post(
            "/api/v1/pools/editor/commands", json=_save_body("update"), headers=HEADERS
        )
        create_denied = client.post(
            "/api/v1/pools/editor/commands",
            json=_create_body("create-on-corrupt"),
            headers=HEADERS,
        )
    assert denied.status_code == 409
    assert create_denied.status_code == 409

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


def test_create_canvas_success_requires_verifiable_publication_identity(tmp_path: Path) -> None:
    app = _app(
        tmp_path / "serving",
        transport=lambda body: {
            "command_id": body["command_id"],
            "status": "succeeded",
            "enqueued_at": body["requested_at"],
            "result": {
                "path": "/private/internal/canvas.json",
                "record_hash": "bad",
                "publication_receipt_id": "b" * 64,
            },
        },
    )
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/pools/editor/commands", json=_create_body("create-bad"), headers=HEADERS
        )
    assert response.status_code == 502
    assert "/private/internal" not in response.text


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
        ({"x-rquant-csrf": "1", "origin": "http://testserver"}, _create_body("x"), 401),
        ({"x-rquant-user": "researcher", "origin": "http://testserver"}, _create_body("x"), 403),
        ({**HEADERS, "origin": "https://elsewhere.test"}, _create_body("x"), 403),
        (HEADERS, _create_body("x", name="../bad"), 422),
        (HEADERS, {**_create_body("x"), "pool_refs": ["user/样本池"]}, 422),
        (HEADERS, {**_create_body("x"), "description": "a" * 1_025}, 422),
        (HEADERS, {**_create_body("x"), "description": "a" * 40_000}, 413),
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


@pytest.mark.parametrize("body", [_save_body("wrong-type"), _create_body("wrong-type")])
def test_write_rejects_non_json_content_type_before_submission(
    tmp_path: Path, body: dict[str, object]
) -> None:
    app = _app(tmp_path / "serving", transport=lambda _body: pytest.fail("must not submit"))
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/pools/editor/commands",
            content=json.dumps(body),
            headers={**HEADERS, "content-type": "text/plain"},
        )
    assert response.status_code == 415
