"""Read-only rule catalog and screening over a synthetic Serving generation."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from rquant.llm.registry import REGISTRY
from rquant.web.app import create_app
from rquant.web.settings import WebSettings
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture


@pytest.fixture(scope="module")
def serving_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("web-screen") / "serving"
    build_web_fixture(root, "baseline")
    return root


def _client(root: Path, *, age: timedelta = timedelta(seconds=30)) -> TestClient:
    return TestClient(
        create_app(
            WebSettings(serving_root=root, stale_after_seconds=1e9),
            clock=lambda: FIXTURE_BUILT_AT + age,
            background=False,
        )
    )


def _run(
    client: TestClient,
    *,
    conditions: list[dict],
    trade_date: str = "2026-09-24",
    page_size: int = 5,
    cursor: str | None = None,
):
    return client.post(
        "/api/v1/screen/run",
        json={
            "trade_date": trade_date,
            "conditions": conditions,
            "page_size": page_size,
            "cursor": cursor,
        },
        headers={"X-Rquant-Csrf": "1"},
    )


def test_catalog_exposes_all_registered_rules_with_plain_chinese_labels(
    serving_root: Path,
) -> None:
    with _client(serving_root) as client:
        response = client.get("/api/v1/screen/blocks")

    assert response.status_code == 200, response.text
    body = response.json()
    blocks = body["data"]["blocks"]
    assert {item["key"] for item in blocks} == {item.name for item in REGISTRY}
    assert len(blocks) == 26
    assert all(item["label"] and item["category_label"] for item in blocks)
    assert all(param["label"] for item in blocks for param in item["parameters"])
    assert body["data"]["dates"] == ["2026-09-24"]
    assert body["data"]["available"] is True


def test_run_uses_registry_rules_with_cumulative_counts_and_bounded_pages(
    serving_root: Path,
) -> None:
    conditions = [
        {"key": "not_st", "args": {}},
        {"key": "circ_mv_lt", "args": {"threshold_yi": 100}},
    ]
    with _client(serving_root) as client:
        first = _run(client, conditions=conditions)
        assert first.status_code == 200, first.text
        data = first.json()["data"]
        assert (data["status"], data["base_count"], data["total"]) == ("ready", 30, 18)
        assert [(step["label"], step["count"]) for step in data["steps"]] == [
            ("排除 ST", 27),
            ("流通市值低于", 18),
        ]
        assert [row["ts_code"] for row in data["rows"]] == [
            "600001.SH", "600002.SH", "600003.SH", "600004.SH", "600005.SH"
        ]
        assert data["next_cursor"]
        second = _run(client, conditions=conditions, cursor=data["next_cursor"])

    assert second.status_code == 200, second.text
    next_data = second.json()["data"]
    assert next_data["total"] == 18
    assert [row["ts_code"] for row in next_data["rows"]] == [
        "600006.SH", "600007.SH", "600008.SH", "600009.SH", "600011.SH"
    ]


def test_empty_result_and_date_without_data_have_different_states(serving_root: Path) -> None:
    with _client(serving_root) as client:
        empty = _run(client, conditions=[{"key": "circ_mv_lt", "args": {"threshold_yi": 0.1}}])
        missing_date = _run(
            client,
            conditions=[{"key": "not_st", "args": {}}],
            trade_date="2026-09-23",
        )

    assert empty.status_code == 200, empty.text
    assert empty.json()["data"]["status"] == "ready"
    assert empty.json()["data"]["total"] == 0
    assert empty.json()["data"]["rows"] == []
    assert missing_date.status_code == 200, missing_date.text
    assert missing_date.json()["data"]["status"] == "no_date"
    assert missing_date.json()["data"]["total"] is None


def test_invalid_or_unavailable_rule_is_explained_without_technical_field_names(
    serving_root: Path,
) -> None:
    with _client(serving_root) as client:
        invalid = _run(client, conditions=[{"key": "circ_mv_lt", "args": {"threshold_yi": -1}}])
        unsupported = _run(client, conditions=[{"key": "first_limit_up", "args": {"offset": 1}}])
        unknown_field = _run(
            client,
            conditions=[{"key": "gt", "args": {"left": "name", "right": 1}}],
        )

    assert invalid.status_code == 422
    assert "检查" in invalid.json()["detail"]
    assert unsupported.status_code == 422
    assert "当前数据" in unsupported.json()["detail"]
    assert "[1]" not in unsupported.json()["detail"]
    assert "IS_FIRST_LIMIT_UP" not in unsupported.json()["detail"]
    assert unknown_field.status_code == 422
    assert "条件目录" in unknown_field.json()["detail"]


def test_next_page_requires_a_rerun_after_generation_changes(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline")
    conditions = [{"key": "not_st", "args": {}}]
    with _client(root, age=timedelta(minutes=2)) as client:
        first = _run(client, conditions=conditions)
        assert first.status_code == 200, first.text
        cursor = first.json()["data"]["next_cursor"]
        assert cursor
        build_web_fixture(root, "baseline", sequence=1)
        client.app.state.web.tracker.refresh()
        stale_page = _run(client, conditions=conditions, cursor=cursor)

    assert stale_page.status_code == 409
    assert stale_page.json()["detail"] == "数据已更新，请重新筛选。"


def test_cursor_cannot_fall_through_to_a_different_date_or_plan(serving_root: Path) -> None:
    conditions = [{"key": "not_st", "args": {}}]
    with _client(serving_root) as client:
        first = _run(client, conditions=conditions)
        cursor = first.json()["data"]["next_cursor"]
        changed = _run(client, conditions=conditions, trade_date="2026-09-23", cursor=cursor)
        changed_plan = _run(
            client,
            conditions=[{"key": "circ_mv_lt", "args": {"threshold_yi": 100}}],
            cursor=cursor,
        )
    assert changed.status_code == 409
    assert changed.json()["detail"] == "数据已更新，请重新筛选。"
    assert changed_plan.status_code == 409
    assert changed_plan.json()["detail"] == "数据已更新，请重新筛选。"


def test_screening_refuses_oversized_requests_and_busy_worker(serving_root: Path) -> None:
    with _client(serving_root) as client:
        oversized = _run(
            client,
            conditions=[{"key": "circ_mv_lt", "args": {"threshold_yi": 100, "note": "x" * 9000}}],
        )
        too_many_rules = _run(client, conditions=[{"key": "not_st", "args": {}}] * 27)
        too_large_page = _run(
            client,
            conditions=[{"key": "not_st", "args": {}}],
            page_size=101,
        )
        gate = client.app.state.web.screen_gate
        assert gate.acquire(blocking=False)
        try:
            busy = _run(client, conditions=[{"key": "not_st", "args": {}}])
        finally:
            gate.release()
    assert oversized.status_code == 413
    assert too_many_rules.status_code == 422
    assert too_large_page.status_code == 422
    assert busy.status_code == 429


def test_missing_published_data_is_not_mistaken_for_zero_hits(tmp_path: Path) -> None:
    with _client(tmp_path / "absent") as client:
        catalog = client.get("/api/v1/screen/blocks")
        result = _run(client, conditions=[{"key": "not_st", "args": {}}])

    assert catalog.json()["data"]["available"] is False
    assert catalog.json()["data"]["dates"] == []
    assert result.status_code == 200, result.text
    assert result.json()["data"]["status"] == "unavailable"
    assert result.json()["data"]["total"] is None
