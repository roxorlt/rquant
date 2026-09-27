"""Backfill proposal API reads one Serving generation and never implies execution."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from rquant.backfill_plan_artifact import load_daily_bar_backfill_plan
from rquant.backfill_plan_projection import project_backfill_plans
from rquant.serving_read_models import ServingProjectionPayload
from rquant.web.app import create_app
from rquant.web.settings import WebSettings
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture
from tests.unit.test_backfill_plan_artifact import _publish, _snapshot


def _app(root: Path, *, now: datetime | None = None) -> FastAPI:
    return create_app(
        WebSettings(serving_root=root),
        clock=lambda: now or FIXTURE_BUILT_AT + timedelta(seconds=20),
        background=False,
    )


def _projections(tmp_path: Path, *, count: int = 1) -> tuple[ServingProjectionPayload, ...]:
    if count:
        snapshot = _snapshot(tmp_path)
        directory = tmp_path / "plans"
        plans = [
            (
                load_daily_bar_backfill_plan(
                    _publish(snapshot, directory, evidence_code_revision=f"revision-{number}")
                ),
                FIXTURE_BUILT_AT - timedelta(minutes=number + 1),
            )
            for number in range(count)
        ]
        plans.reverse()
    else:
        plans = []
    return project_backfill_plans(
        plans,
        available_at=FIXTURE_BUILT_AT - timedelta(minutes=1),
    )


def test_published_plan_list_and_detail_keep_all_dates_and_unverified_claims(
    tmp_path: Path,
) -> None:
    root = tmp_path / "serving"
    projections = _projections(tmp_path)
    build_web_fixture(root, "baseline", backfill_plan_projections=projections)
    index = next(item for item in projections if item.table_name == "backfill_plan_index")
    hash_value = index.rows[0]["plan_hash"]

    with TestClient(_app(root)) as client:
        listing = client.get("/api/v1/data/backfill-plans")
        detail = client.get(f"/api/v1/data/backfill-plans/{hash_value}")

    assert listing.status_code == 200
    data = listing.json()["data"]
    assert data["source_state"] == "ready"
    assert data["total"] == 1
    assert data["next_cursor"] is None
    assert data["items"][0]["plan_hash"] == hash_value
    assert data["items"][0]["executable"] is False
    assert data["progress"] == {
        "availability": "unavailable",
        "task_id": None,
        "message": "任务进度尚未提供",
        "logs": [],
    }
    assert detail.status_code == 200
    plan = detail.json()["data"]["plan"]
    assert plan["plan_hash"] == hash_value
    assert len(plan["missing_dates"]) == data["items"][0]["missing_day_count"]
    assert sum(month["missing_open_days"] for month in plan["monthly"]) == len(
        plan["missing_dates"]
    )
    assert plan["estimate"]["quota_status"] == "unverified"
    assert plan["source"]["identity_verified"] is False
    assert plan["source"]["collection_complete_verified"] is False
    assert plan["executable"] is False
    assert listing.headers["X-Rquant-Generation"] == listing.json()["serving"]["generation_id"]


def test_no_plan_unpublished_and_unavailable_are_distinct(tmp_path: Path) -> None:
    empty = tmp_path / "empty"
    build_web_fixture(empty, "baseline", backfill_plan_projections=_projections(tmp_path, count=0))
    unpublished = tmp_path / "unpublished"
    build_web_fixture(unpublished, "baseline")

    with TestClient(_app(empty)) as client:
        empty_response = client.get("/api/v1/data/backfill-plans")
        empty_detail = client.get("/api/v1/data/backfill-plans/" + "0" * 64)
    with TestClient(_app(unpublished)) as client:
        unpublished_response = client.get("/api/v1/data/backfill-plans")
    with TestClient(_app(tmp_path / "missing")) as client:
        unavailable_response = client.get("/api/v1/data/backfill-plans")

    assert empty_response.json()["data"]["source_state"] == "empty"
    assert empty_response.json()["data"]["total"] == 0
    assert empty_detail.json()["data"]["source_state"] == "empty"
    assert empty_detail.json()["data"]["plan"] is None
    assert unpublished_response.json()["data"]["source_state"] == "not_published"
    assert unpublished_response.json()["data"]["total"] is None
    assert unavailable_response.json()["data"]["source_state"] == "unavailable"
    assert unavailable_response.json()["data"]["total"] is None


def test_page_cursor_is_bound_to_one_generation(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    projections = _projections(tmp_path, count=3)
    first = build_web_fixture(root, "baseline", backfill_plan_projections=projections)
    with TestClient(_app(root)) as client:
        first_page = client.get("/api/v1/data/backfill-plans", params={"page_size": 1})
        cursor = first_page.json()["data"]["next_cursor"]
        second_page = client.get(
            "/api/v1/data/backfill-plans",
            params={"page_size": 1, "generation": first.generation_id, "cursor": cursor},
        )
        missing_generation = client.get(
            "/api/v1/data/backfill-plans", params={"page_size": 1, "cursor": cursor}
        )
        third_page = client.get(
            "/api/v1/data/backfill-plans",
            params={
                "page_size": 1,
                "generation": first.generation_id,
                "cursor": second_page.json()["data"]["next_cursor"],
            },
        )
        unknown_cursor = client.get(
            "/api/v1/data/backfill-plans",
            params={"page_size": 1, "generation": first.generation_id, "cursor": "0" * 64},
        )
        build_web_fixture(root, "baseline", sequence=1, backfill_plan_projections=projections)
        client.app.state.web.tracker.refresh()
        changed = client.get(
            "/api/v1/data/backfill-plans",
            params={"page_size": 1, "generation": first.generation_id, "cursor": cursor},
        )

    assert first_page.status_code == second_page.status_code == third_page.status_code == 200
    assert first_page.json()["data"]["items"][0]["plan_hash"] != second_page.json()["data"][
        "items"
    ][0]["plan_hash"]
    assert third_page.json()["data"]["next_cursor"] is None
    assert len(
        {
            first_page.json()["data"]["items"][0]["plan_hash"],
            second_page.json()["data"]["items"][0]["plan_hash"],
            third_page.json()["data"]["items"][0]["plan_hash"],
        }
    ) == 3
    assert unknown_cursor.status_code == 409
    assert missing_generation.status_code == changed.status_code == 409


def test_old_serving_generation_is_labeled_stale_without_claiming_current_plan(
    tmp_path: Path,
) -> None:
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline", backfill_plan_projections=_projections(tmp_path))
    with TestClient(_app(root, now=FIXTURE_BUILT_AT + timedelta(days=2))) as client:
        response = client.get("/api/v1/data/backfill-plans")
    assert response.status_code == 200
    assert response.json()["serving"]["state"] == "stale"
    assert response.json()["data"]["source_state"] == "ready"
    assert response.json()["data"]["items"][0]["executable"] is False


def test_ninth_plan_detail_is_read_from_same_published_generation(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    projections = _projections(tmp_path, count=9)
    build_web_fixture(root, "baseline", backfill_plan_projections=projections)
    index = next(item for item in projections if item.table_name == "backfill_plan_index")
    oldest_hash = index.rows[-1]["plan_hash"]

    with TestClient(_app(root)) as client:
        page = client.get("/api/v1/data/backfill-plans", params={"page_size": 9})
        generation = page.json()["serving"]["generation_id"]
        detail = client.get(
            f"/api/v1/data/backfill-plans/{oldest_hash}", params={"generation": generation}
        )
        changed = client.get(
            f"/api/v1/data/backfill-plans/{oldest_hash}",
            params={"generation": "0" * 64},
        )
        missing = client.get(
            f"/api/v1/data/backfill-plans/{'0' * 64}", params={"generation": generation}
        )

    assert page.status_code == 200
    assert len(page.json()["data"]["items"]) == 9
    assert detail.status_code == 200
    assert detail.json()["data"]["plan"]["plan_hash"] == oldest_hash
    assert detail.json()["data"]["plan"]["missing_dates"]
    assert changed.status_code == 409
    assert missing.status_code == 404


def test_partial_plan_projection_fails_closed(tmp_path: Path) -> None:
    projections = tuple(
        item for item in _projections(tmp_path) if item.table_name != "backfill_plan_preview"
    )
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline", backfill_plan_projections=projections)
    with TestClient(_app(root)) as client:
        response = client.get("/api/v1/data/backfill-plans")
    assert response.status_code == 503
    assert "hash" not in response.text
