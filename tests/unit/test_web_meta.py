"""``GET /api/v1/meta``: the envelope in its four states, and the market phase."""

from __future__ import annotations

import os
import stat
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from rquant.web.app import create_app
from rquant.web.market import MarketPhase, market_phase
from rquant.web.settings import WebSettings
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture


def _client(root: Path, now: datetime) -> TestClient:
    app = create_app(WebSettings(serving_root=root), clock=lambda: now, background=False)
    return TestClient(app)


def _meta(root: Path, now: datetime, **headers: str) -> dict:
    with _client(root, now) as client:
        response = client.get("/api/v1/meta", headers=headers)
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    return response.json()


def test_ready_generation_carries_marker_watermarks_projections_and_phase(
    tmp_path: Path,
) -> None:
    root = tmp_path / "serving"
    manifest = build_web_fixture(root, "baseline")
    now = FIXTURE_BUILT_AT + timedelta(seconds=30)

    with _client(root, now) as client:
        response = client.get("/api/v1/meta")
    body = response.json()

    assert response.headers["x-rquant-generation"] == manifest.generation_id
    assert body["serving"] == {
        "generation_id": manifest.generation_id,
        "built_at": "2026-09-24T07:31:00Z",
        "state": "ready",
        "detail": "serving generation verified",
    }
    data = body["data"]
    assert data["generation"]["generation_id"] == manifest.generation_id
    assert data["generation"]["age_seconds"] == 30.0
    assert data["generation"]["producer_commit"] == manifest.producer_commit
    assert {item["dataset_id"] for item in data["datasets"]} >= {"signals", "runtime_health"}
    assert all(item["status"] == "fresh" for item in data["datasets"])
    projections = {item["table_name"]: item for item in data["projections"]}
    assert projections["dashboard_summary"]["available"] is True
    assert projections["market_snapshot"]["available"] is False
    assert projections["market_snapshot"]["reason"] == "projection_not_published"
    # 2026-09-24 15:31:30 in Shanghai, a weekday in the synthetic calendar.
    assert data["market"] == {
        "trade_date": "2026-09-24",
        "phase": "after_close",
        "phase_label": "收盘",
        "is_trading_day": True,
    }
    assert data["viewer"] is None


def test_old_generation_is_stale(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline")

    body = _meta(root, FIXTURE_BUILT_AT + timedelta(minutes=11))

    assert body["serving"]["state"] == "stale"
    assert body["serving"]["detail"] == (
        "serving generation stale: built_at exceeded freshness budget"
    )


def test_degraded_watermarks_make_the_generation_degraded(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    build_web_fixture(root, "degraded")

    body = _meta(root, FIXTURE_BUILT_AT + timedelta(seconds=30))

    assert body["serving"]["state"] == "degraded"
    assert "runtime_health:degraded:" in body["serving"]["detail"]
    assert "lab_jobs:unavailable:" in body["serving"]["detail"]
    projections = {item["table_name"]: item for item in body["data"]["projections"]}
    assert projections["dashboard_summary"]["available"] is False


def test_missing_serving_root_is_unavailable_without_creating_it(tmp_path: Path) -> None:
    root = tmp_path / "absent"

    body = _meta(root, FIXTURE_BUILT_AT)

    assert body["serving"]["state"] == "unavailable"
    assert body["serving"]["generation_id"] is None
    assert "serving 指针不可读" in body["serving"]["detail"]
    assert body["data"]["generation"] is None
    assert body["data"]["market"]["phase"] == "unknown"
    assert not root.exists()


def test_a_broken_newer_generation_keeps_serving_the_old_one_as_degraded(
    tmp_path: Path,
) -> None:
    root = tmp_path / "serving"
    first = build_web_fixture(root, "baseline")
    now = FIXTURE_BUILT_AT + timedelta(minutes=1, seconds=30)
    app = create_app(
        WebSettings(serving_root=root, pointer_check_seconds=0.001),
        clock=lambda: now,
        background=False,
    )
    with TestClient(app) as client:
        assert client.get("/api/v1/meta").json()["serving"]["state"] == "ready"
        second = build_web_fixture(root, "baseline", sequence=1)
        database = root / "generations" / second.generation_id / "serving.duckdb"
        os.chmod(database.parent, stat.S_IRWXU)
        os.chmod(database, stat.S_IRUSR | stat.S_IWUSR)
        with database.open("r+b") as handle:
            handle.seek(4096)
            handle.write(b"\x00" * 64)
        body = client.get("/api/v1/meta").json()

    assert body["serving"]["generation_id"] == first.generation_id
    assert body["serving"]["state"] == "degraded"
    assert second.generation_id[:8] in body["serving"]["detail"]


def test_viewer_comes_from_the_nginx_user_header_and_is_validated(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline")
    now = FIXTURE_BUILT_AT + timedelta(seconds=30)

    assert _meta(root, now, **{"X-Rquant-User": "liutong"})["data"]["viewer"] == "liutong"
    assert _meta(root, now, **{"X-Rquant-User": "bad user;"})["data"]["viewer"] is None


def test_unknown_paths_and_docs_are_not_served(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline")
    with _client(root, FIXTURE_BUILT_AT) as client:
        for path in ("/docs", "/redoc", "/openapi.json", "/api/v1/nope", "/"):
            assert client.get(path).status_code == 404


def _shanghai(hour: int, minute: int) -> datetime:
    return datetime(2026, 9, 24, hour, minute, tzinfo=UTC) - timedelta(hours=8)


@pytest.mark.parametrize(
    ("hour", "minute", "phase"),
    (
        (9, 14, MarketPhase.PRE_OPEN),
        (9, 15, MarketPhase.CALL_AUCTION),
        (9, 29, MarketPhase.CALL_AUCTION),
        (9, 30, MarketPhase.CONTINUOUS),
        (11, 29, MarketPhase.CONTINUOUS),
        (11, 30, MarketPhase.NOON_BREAK),
        (12, 59, MarketPhase.NOON_BREAK),
        (13, 0, MarketPhase.CONTINUOUS),
        (14, 56, MarketPhase.CONTINUOUS),
        (14, 57, MarketPhase.CLOSING_AUCTION),
        (15, 0, MarketPhase.AFTER_CLOSE),
    ),
)
def test_market_phase_boundaries_in_the_shanghai_clock(
    hour: int, minute: int, phase: MarketPhase
) -> None:
    assert market_phase(_shanghai(hour, minute), True) is phase


def test_market_phase_without_a_calendar_answer() -> None:
    assert market_phase(_shanghai(10, 0), False) is MarketPhase.NON_TRADING_DAY
    assert market_phase(_shanghai(10, 0), None) is MarketPhase.UNKNOWN
