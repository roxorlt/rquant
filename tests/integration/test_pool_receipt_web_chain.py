"""A verified zero-hit run reaches the pool page through a real Serving generation."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from fastapi.testclient import TestClient

from rquant.web.app import create_app
from rquant.web.settings import WebSettings
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture
from tests.unit.test_pool_result_publication import (
    NOW,
    TODAY,
    _database,
    _insert_receipt,
    _projections,
)

_POOL_PROJECTIONS = (
    "pool_definition",
    "screen_run_receipt",
    "canvas_latest_trade_date",
    "canvas_hit",
    "canvas_diagnostic",
    "screen_bounds",
)


def test_verified_zero_hit_reaches_pool_page_from_one_generation(tmp_path: Path) -> None:
    replica = tmp_path / "replica.duckdb"
    _database(replica)
    _insert_receipt(replica, day=TODAY)
    projected = _projections(replica, NOW)

    serving_root = tmp_path / "serving"
    build_web_fixture(
        serving_root,
        "baseline",
        signal_projections=tuple(projected[name] for name in _POOL_PROJECTIONS),
    )
    app = create_app(
        WebSettings(serving_root=serving_root, stale_after_seconds=10_000_000),
        clock=lambda: FIXTURE_BUILT_AT + timedelta(seconds=30),
        background=False,
    )
    with TestClient(app) as client:
        response = client.get("/api/v1/pools")

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["latest_trade_date"] == TODAY.isoformat()
    pool = next(item for item in data["pools"] if item["key"] == "n-shape-pool1")
    assert pool["state"] == "current"
    assert pool["member_count"] == 0
    assert pool["members"] == []
    assert pool["steps"] == []
    assert pool["definition"]["state"] == "available"
    assert pool["result"]["state"] == "current_rules"
    assert pool["result"]["trade_date"] == TODAY.isoformat()
    assert pool["result"]["hit_count"] == 0
