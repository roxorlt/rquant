"""Published channel submission facts are independent from the alert timeline."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from rquant.web.settings import WebSettings
from tests.support.web_proxy_identity import ResearcherTestClient as TestClient
from tests.support.web_proxy_identity import create_private_test_app as create_app
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture


def _app(root: Path):
    return create_app(
        WebSettings(serving_root=root),
        clock=lambda: FIXTURE_BUILT_AT + timedelta(seconds=30),
        background=False,
    )


def _published(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    rows: list[dict[str, Any]] | None,
    state: str | None = "complete",
    skipped: int = 0,
) -> Path:
    from tests.support import web_serving_fixture as fixture

    original = fixture._projections

    def with_notifications(scenario, *, built_at, generations):
        projections = list(original(scenario, built_at=built_at, generations=generations))
        available_at = built_at - timedelta(seconds=30)
        if rows is not None:
            projections.append(
                fixture._projection(
                    "legacy_notification",
                    rows,
                    owner="signals",
                    generations=generations,
                    available_at=available_at,
                )
            )
        if state is not None:
            projections.append(
                fixture._projection(
                    "legacy_notification_status",
                    [{"snapshot_key": "current", "state": state, "skipped": skipped}],
                    owner="signals",
                    generations=generations,
                    available_at=available_at,
                )
            )
        return tuple(projections)

    monkeypatch.setattr(fixture, "_projections", with_notifications)
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline")
    return root


def _row(key: str, at: str, channel: str, submitted: bool) -> dict[str, Any]:
    return {
        "record_key": key * 64,
        "sent_at": at,
        "scene_label": "价位提醒",
        "channel_label": channel,
        "submitted": submitted,
    }


def test_channels_count_shanghai_today_and_inclusive_seven_days(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _published(
        tmp_path,
        monkeypatch,
        rows=[
            _row("1", "2026-09-24T02:06:00Z", "PushDeer", True),
            _row("2", "2026-09-24T02:07:00Z", "PushDeer", False),
            _row("3", "2026-09-17T16:00:00Z", "PushDeer", True),
            _row("4", "2026-09-17T15:59:59Z", "PushPlus", True),
            _row("5", "2026-09-24T00:00:00Z", "PushPlus", False),
        ],
    )
    with TestClient(_app(root)) as client:
        response = client.get("/api/v1/monitor/channels")
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["data"]["state"] == "ready"
        channels = {item["channel"]: item for item in body["data"]["channels"]}
        assert set(channels) == {"pushdeer", "pushplus"}
        assert channels["pushdeer"] == {
            "channel": "pushdeer",
            "channel_label": "PushDeer",
            "today_submitted": 1,
            "seven_day_attempts": 3,
            "seven_day_submitted": 2,
            "seven_day_success_pct": pytest.approx(66.7),
            "last_success_at": "2026-09-24T02:06:00Z",
        }
        assert channels["pushplus"]["today_submitted"] == 0
        assert channels["pushplus"]["seven_day_attempts"] == 1
        assert channels["pushplus"]["seven_day_submitted"] == 0
        assert channels["pushplus"]["seven_day_success_pct"] == 0
        assert channels["pushplus"]["last_success_at"] == "2026-09-17T15:59:59Z"
        assert "SECRET" not in response.text


def test_complete_source_with_no_submissions_has_real_zero_and_unknown_rate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _published(tmp_path, monkeypatch, rows=[])
    with TestClient(_app(root)) as client:
        data = client.get("/api/v1/monitor/channels").json()["data"]
        assert data["state"] == "ready"
        assert len(data["channels"]) == 2
        assert all(
            item["today_submitted"] == 0
            and item["seven_day_attempts"] == 0
            and item["seven_day_success_pct"] is None
            and item["last_success_at"] is None
            for item in data["channels"]
        )


@pytest.mark.parametrize(
    ("rows", "state", "skipped"),
    [
        (None, None, 0),
        (None, "complete", 0),
        ([], "unavailable", 0),
        ([_row("1", "2026-09-24T02:06:00Z", "PushDeer", True)], "partial", 1),
        ([_row("1", "2026-09-24T02:06:00Z", "Other", True)], "complete", 0),
    ],
)
def test_incomplete_or_bad_source_never_publishes_precise_counts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rows: list[dict[str, Any]] | None,
    state: str | None,
    skipped: int,
) -> None:
    root = _published(tmp_path, monkeypatch, rows=rows, state=state, skipped=skipped)
    with TestClient(_app(root)) as client:
        response = client.get("/api/v1/monitor/channels")
        assert response.status_code == 200, response.text
        assert response.json()["data"] == {"state": "unavailable", "channels": []}


def test_channels_survive_unpublished_signal_source_and_clear_when_serving_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.web.routes import monitor

    root = _published(
        tmp_path,
        monkeypatch,
        rows=[_row("1", "2026-09-24T02:06:00Z", "PushDeer", True)],
    )
    monkeypatch.setattr(monitor, "_source_published", lambda _borrowed: False)
    with TestClient(_app(root)) as client:
        timeline = client.get("/api/v1/monitor/timeline").json()["data"]
        assert timeline["source_state"] == "not_published"
        assert client.get("/api/v1/monitor/channels").json()["data"]["state"] == "ready"
    with TestClient(_app(tmp_path / "missing")) as client:
        data = client.get("/api/v1/monitor/channels").json()["data"]
        assert data == {"state": "unavailable", "channels": []}


def test_channels_follow_new_borrowed_serving_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rows = [_row("1", "2026-09-24T02:06:00Z", "PushDeer", True)]
    root = _published(tmp_path, monkeypatch, rows=rows)
    app = _app(root)
    with TestClient(app) as client:
        original = client.get("/api/v1/monitor/channels").json()
        assert original["data"]["channels"][0]["today_submitted"] == 1
        rows.clear()
        build_web_fixture(root, "baseline", sequence=1)
        app.state.web.tracker.refresh()
        changed = client.get("/api/v1/monitor/channels").json()
        assert changed["serving"]["generation_id"] != original["serving"]["generation_id"]
        assert all(item["today_submitted"] == 0 for item in changed["data"]["channels"])
