"""Alert acknowledgment reads and exact retry recovery through the Web boundary."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from rquant.alert_ack import stable_alert_id, stable_signal_alert_id
from rquant.page_control import AlertAcknowledgment
from rquant.serving_alert_projection import AlertAckAuthoritySnapshot
from rquant.serving_read_models import ServingProjectionPayload
from rquant.web.alert_ack_read import AlertReadModel, _Event
from rquant.web.app import create_app
from rquant.web.models.alert_ack import UnacknowledgedSummary
from rquant.web.settings import WebSettings
from tests.support.web_serving_fixture import (
    FIXTURE_BUILT_AT,
    _monitor_events,
    build_web_fixture,
)

NOW = FIXTURE_BUILT_AT + timedelta(seconds=30)
HEADERS = {
    "x-rquant-user": "researcher",
    "x-rquant-csrf": "1",
    "origin": "http://testserver",
}


def _projection(name: str, rows: list[dict[str, object]]) -> ServingProjectionPayload:
    return ServingProjectionPayload(
        table_name=name,
        available_at=FIXTURE_BUILT_AT,
        rows=tuple(rows),
    )


def _app(root: Path, *, lookup=None):
    return create_app(
        WebSettings(serving_root=root),
        clock=lambda: NOW,
        background=False,
        ack_lookup_transport=lookup,
    )


def _body() -> dict[str, str]:
    return {
        "command_id": "ack-original",
        "requested_at": NOW.isoformat(),
        "generation_id": "a" * 64,
        "alert_id": "b" * 64,
    }


def test_signal_identity_helper_only_accepts_canonical_published_id() -> None:
    assert stable_signal_alert_id("a" * 64) == stable_signal_alert_id("a" * 64)
    with pytest.raises(ValueError):
        stable_signal_alert_id("not-a-verified-signal")


def test_old_serving_generation_keeps_ack_count_unknown_on_both_pages(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline")
    with TestClient(_app(root)) as client:
        timeline = client.get("/api/v1/monitor/timeline")
        overview = client.get("/api/v1/overview")
    assert timeline.status_code == overview.status_code == 200
    for response in (timeline, overview):
        summary = response.json()["data"]["unacknowledged"]
        assert summary["count"] is None
        assert summary["count_as_of"] is None
        assert summary["state"] == "unavailable"
    for item in timeline.json()["data"]["items"]:
        if item["kind"] == "notification":
            assert "acknowledgment" not in item
        else:
            assert item["acknowledgment"]["eligible"] is False


def test_confirmed_item_is_read_without_inventing_full_window_count(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    event = _monitor_events()[0]
    alert_id = stable_alert_id("monitor_event", event)
    ack = AlertAcknowledgment(
        alert_id=alert_id,
        confirmation_id="ack-first",
        actor_id="researcher",
        confirmed_at=FIXTURE_BUILT_AT - timedelta(minutes=5),
        generation_id="c" * 64,
    )
    snapshot = AlertAckAuthoritySnapshot.create(
        activated_at=FIXTURE_BUILT_AT - timedelta(days=1), rows=[ack]
    )
    projections = [
        _projection(
            "alert_ack_state",
            [
                {
                    "snapshot_key": "current",
                    "state": "ready",
                    "activated_at": snapshot.activated_at.isoformat(),
                    "row_count": snapshot.row_count,
                    "rows_sha256": snapshot.rows_sha256,
                }
            ],
        ),
        _projection("alert_ack", [ack.model_dump(mode="json")]),
        _projection(
            "alert_event",
            [
                {
                    "source": "monitor_event",
                    "alert_id": alert_id,
                    "occurred_at": event["trigger_time"],
                    "confirmation_id": "ack-first",
                    "confirmed_at": ack.confirmed_at.isoformat(),
                    "eligible": False,
                }
            ],
        ),
        _projection(
            "alert_source_coverage",
            [
                {
                    "source": source,
                    "state": "unavailable",
                    "reason": "coverage_unverified",
                    "window_start": None,
                    "window_end": None,
                    "count_as_of": None,
                    "source_generation_id": None,
                    "high_watermark": None,
                    "row_count": 1 if source == "monitor_event" else 0,
                    "row_digest": "d" * 64,
                }
                for source in ("signal", "monitor_event", "surge_event")
            ],
        ),
        _projection(
            "alert_overview",
            [
                {
                    "snapshot_key": "current",
                    "state": "ready",
                    "unacknowledged_count": 0,
                    "count_as_of": FIXTURE_BUILT_AT.isoformat(),
                    "activated_at": snapshot.activated_at.isoformat(),
                }
            ],
        ),
    ]
    build_web_fixture(root, "baseline", signal_projections=tuple(projections))
    with TestClient(_app(root)) as client:
        timeline = client.get("/api/v1/monitor/timeline").json()["data"]
        overview = client.get("/api/v1/overview").json()["data"]
    monitor = next(item for item in timeline["items"] if item["kind"] == "monitor")
    assert monitor["acknowledgment"]["state"] == "confirmed"
    assert monitor["acknowledgment"]["confirmation_id"] == "ack-first"
    assert monitor["acknowledgment"]["eligible"] is False
    assert timeline["unacknowledged"] == overview["unacknowledged"]
    assert timeline["unacknowledged"]["count"] is None


def test_exact_ack_retry_recovers_first_confirmation_without_serving(tmp_path: Path) -> None:
    observed: list[dict[str, object]] = []

    def lookup(payload: dict[str, object]) -> dict[str, object]:
        observed.append(payload)
        return {
            "found": True,
            "receipt": {
                "command_id": payload["command_id"],
                "status": "succeeded",
                "enqueued_at": NOW.isoformat(),
                "completed_at": NOW.isoformat(),
                "result": {"confirmation_id": "ack-first"},
                "error": None,
            },
        }

    with TestClient(_app(tmp_path / "no-serving", lookup=lookup)) as client:
        response = client.post("/api/v1/monitor/ack", json=_body(), headers=HEADERS)
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "succeeded"
    assert response.json()["confirmation_id"] == "ack-first"
    assert observed == [
        {
            "kind": "ack_alert",
            **_body(),
            "requested_at": NOW.isoformat().replace("+00:00", "Z"),
            "actor_id": "researcher",
        }
    ]


def test_ack_write_requires_identity_and_same_site(tmp_path: Path) -> None:
    def forbidden_lookup(_payload: dict[str, object]) -> dict[str, object]:
        pytest.fail("unauthorized request reached PageControl")

    with TestClient(_app(tmp_path / "no-serving", lookup=forbidden_lookup)) as client:
        no_user = client.post(
            "/api/v1/monitor/ack",
            json=_body(),
            headers={"x-rquant-csrf": "1", "origin": "http://testserver"},
        )
        cross_site = client.post(
            "/api/v1/monitor/ack",
            json=_body(),
            headers={**HEADERS, "origin": "http://evil.test"},
        )
        forged_actor = client.post(
            "/api/v1/monitor/ack",
            json={**_body(), "actor_id": "mallory"},
            headers=HEADERS,
        )
    assert no_user.status_code == 401
    assert cross_site.status_code == 403
    assert forged_actor.status_code == 422


def test_new_ack_stays_closed_after_explicit_lookup_miss(tmp_path: Path) -> None:
    observed: list[dict[str, object]] = []

    def lookup(payload: dict[str, object]) -> dict[str, object]:
        observed.append(payload)
        return {"found": False}

    root = tmp_path / "serving"
    manifest = build_web_fixture(root, "baseline")
    with TestClient(_app(root, lookup=lookup)) as client:
        response = client.post(
            "/api/v1/monitor/ack",
            json={
                **_body(),
                "generation_id": manifest.generation_id,
                "alert_id": stable_alert_id("monitor_event", _monitor_events()[0]),
            },
            headers=HEADERS,
        )
    assert response.status_code == 409
    assert "暂不可用" in response.json()["detail"]
    assert len(observed) == 1


@pytest.mark.parametrize(
    ("lookup", "status"),
    [
        (lambda _payload: (_ for _ in ()).throw(OSError("secret path")), 503),
        (lambda _payload: (_ for _ in ()).throw(ValueError("different content")), 409),
        (lambda _payload: {"found": True, "receipt": {"status": "succeeded"}}, 502),
        (lambda _payload: {"found": True, "receipt": {"command_id": "wrong"}}, 502),
    ],
)
def test_ack_lookup_failure_never_falls_through_to_new_write(
    tmp_path: Path, lookup, status: int
) -> None:
    with TestClient(_app(tmp_path / "no-serving", lookup=lookup)) as client:
        response = client.post("/api/v1/monitor/ack", json=_body(), headers=HEADERS)
    assert response.status_code == status
    assert "secret path" not in response.text
    assert "different content" not in response.text


def test_original_command_is_bound_to_server_identity(tmp_path: Path) -> None:
    observed: list[str] = []

    def lookup(payload: dict[str, object]) -> dict[str, object]:
        observed.append(str(payload["actor_id"]))
        if payload["actor_id"] != "researcher":
            raise ValueError("same ID belongs to another actor")
        return {"found": False}

    with TestClient(_app(tmp_path / "no-serving", lookup=lookup)) as client:
        response = client.post(
            "/api/v1/monitor/ack",
            json=_body(),
            headers={**HEADERS, "x-rquant-user": "mallory"},
        )
    assert response.status_code == 409
    assert observed == ["mallory"]
    assert "another actor" not in response.text


def test_original_pending_receipt_does_not_claim_confirmation(tmp_path: Path) -> None:
    def lookup(payload: dict[str, object]) -> dict[str, object]:
        return {
            "found": True,
            "receipt": {
                "command_id": payload["command_id"],
                "status": "pending",
                "enqueued_at": NOW.isoformat(),
                "completed_at": None,
                "result": None,
                "error": None,
            },
        }

    with TestClient(_app(tmp_path / "no-serving", lookup=lookup)) as client:
        response = client.post("/api/v1/monitor/ack", json=_body(), headers=HEADERS)
    assert response.status_code == 200
    assert response.json()["status"] == "pending"
    assert response.json()["confirmation_id"] is None


def test_event_after_verified_cutoff_cannot_be_confirmed() -> None:
    alert_id = "b" * 64
    event = _Event(
        source="monitor_event",
        alert_id=alert_id,
        occurred_at=NOW + timedelta(seconds=1),
        confirmation_id=None,
        confirmed_at=None,
        eligible=True,
    )
    model = AlertReadModel(
        summary=UnacknowledgedSummary(state="ready", count=0, count_as_of=NOW, label="0 条待确认"),
        activated_at=NOW - timedelta(days=1),
        events={("monitor_event", alert_id): event},
        acknowledgments={},
    )
    assert model.status_for("monitor_event", alert_id).eligible is False
