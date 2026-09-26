"""Legacy-shaped operational events reach the web timeline through Serving."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from rquant.web.app import create_app
from rquant.web.settings import WebSettings
from tests.support.monitor_timeline_replay import build_monitor_timeline_replay
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT


def test_legacy_event_files_reach_one_web_timeline_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant import runtime_builder_signal
    from rquant.runtime_serving_snapshot import ServingSnapshotAssembler

    signal_publications = 0
    serving_assemblies = 0
    original_publish = runtime_builder_signal._publish_signal_authority
    original_assemble = ServingSnapshotAssembler.assemble

    def track_publish(*args: object, **kwargs: object) -> tuple[str, int]:
        nonlocal signal_publications
        signal_publications += 1
        return original_publish(*args, **kwargs)

    def track_assemble(self: ServingSnapshotAssembler, as_of: datetime) -> object:
        nonlocal serving_assemblies
        serving_assemblies += 1
        return original_assemble(self, as_of)

    monkeypatch.setattr(runtime_builder_signal, "_publish_signal_authority", track_publish)
    monkeypatch.setattr(ServingSnapshotAssembler, "assemble", track_assemble)
    serving_root = tmp_path / "serving"
    build_monitor_timeline_replay(serving_root)
    assert signal_publications == 1
    assert serving_assemblies == 1
    app = create_app(
        WebSettings(serving_root=serving_root),
        clock=lambda: FIXTURE_BUILT_AT + timedelta(seconds=30),
        background=False,
    )
    with TestClient(app) as client:
        response = client.get("/api/v1/monitor/timeline", params={"page_size": 20})
        assert response.status_code == 200, response.text
        data = response.json()["data"]
        assert data["total"] == 3
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
        signal = data["items"][2]
        assert signal["kind"] == "signal"
        assert signal["at"] == "2026-09-24T01:47:00Z"
        assert signal["receipts"][0]["status_label"] == "待发送"


@pytest.mark.parametrize("source_failure", ("missing", "read_error"))
def test_unreadable_monitor_source_stays_partial_after_new_serving_assembly(
    tmp_path: Path, source_failure: str
) -> None:
    serving_root = tmp_path / "serving"
    build_monitor_timeline_replay(serving_root, source_failure=source_failure)
    assert len(tuple((serving_root / "generations").iterdir())) >= 2
    app = create_app(
        WebSettings(serving_root=serving_root),
        clock=lambda: FIXTURE_BUILT_AT + timedelta(minutes=1, seconds=30),
        background=False,
    )
    with TestClient(app) as client:
        response = client.get("/api/v1/monitor/timeline")
        assert response.status_code == 200, response.text
        data = response.json()["data"]
        assert [item["kind"] for item in data["items"]] == ["surge", "signal"]
        assert "盯盘触发记录" in data["source_note"]
        assert "仅显示已有记录" in data["source_note"]
