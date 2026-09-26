"""Legacy-shaped operational events reach the web timeline through Serving."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from fastapi.testclient import TestClient

from rquant.web.app import create_app
from rquant.web.settings import WebSettings
from tests.support.monitor_timeline_replay import build_monitor_timeline_replay
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT


def test_legacy_event_files_reach_one_web_timeline_generation(tmp_path: Path) -> None:
    serving_root = tmp_path / "serving"
    build_monitor_timeline_replay(serving_root)
    app = create_app(
        WebSettings(serving_root=serving_root),
        clock=lambda: FIXTURE_BUILT_AT + timedelta(seconds=30),
        background=False,
    )
    with TestClient(app) as client:
        response = client.get("/api/v1/monitor/timeline", params={"page_size": 20})
        assert response.status_code == 200, response.text
        data = response.json()["data"]
        assert data["total"] == 4
        assert data["source_note"] is None
        assert [(row["kind"], row["at"]) for row in data["items"][:2]] == [
            ("monitor", "2026-09-24T02:05:00Z"),
            ("surge", "2026-09-24T01:52:00Z"),
        ]
        assert data["items"][0]["price"] == 12.34
        assert data["items"][1]["price"] == 11.25
        assert data["items"][1]["pct_chg"] == 3.15
        assert "receipts" not in data["items"][0]
        assert "receipts" not in data["items"][1]
