"""Alert acknowledgment reads and exact retry recovery through the Web boundary."""

from __future__ import annotations

import threading
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from fastapi.testclient import TestClient

from rquant.alert_ack import (
    alert_event_at,
    alert_window_start,
    stable_alert_id,
    stable_signal_alert_id,
)
from rquant.alert_ack_admission import (
    AckAdmission,
    AckAdmissionRejectedError,
    AckAdmissionUnavailableError,
    build_ack_admission_server,
)
from rquant.page_control import AckAlert, AlertAcknowledgment, PageControlReceipt
from rquant.page_control_service import build_page_control_service
from rquant.runtime_contracts import canonical_sha256
from rquant.serving_alert_projection import AlertAckAuthoritySnapshot
from rquant.serving_read_models import ServingProjectionPayload
from rquant.web.alert_ack_read import AlertReadModel, _Event
from rquant.web.app import create_app
from rquant.web.models.alert_ack import UnacknowledgedSummary
from rquant.web.settings import WebSettings
from tests.support.web_serving_fixture import (
    FIXTURE_BUILT_AT,
    _generation_ids,
    _monitor_events,
    _signal_bundle,
    _timeline_surge_events,
    build_web_fixture,
)

NOW = FIXTURE_BUILT_AT + timedelta(seconds=30)
SHORT_TMP = "/private/tmp" if Path("/private/tmp").is_dir() else "/tmp"
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


def _app(root: Path, *, lookup=None, admission=None):
    return create_app(
        WebSettings(
            serving_root=root,
            ack_admission_socket_path=(root.parent / "private" / "ack.sock")
            if admission is not None
            else None,
            ingress_socket_path=(root.parent / "web-private" / "web.sock")
            if admission is not None
            else None,
        ),
        clock=lambda: NOW,
        background=False,
        ack_lookup_transport=lookup,
        ack_admission_client=admission,
    )


def _body() -> dict[str, str]:
    return {
        "command_id": "ack-original",
        "requested_at": NOW.isoformat(),
        "generation_id": "a" * 64,
        "alert_id": "b" * 64,
    }


def _complete_alert_projections(
    *, missing_observed_rows: bool = False, bad_monitor_generation: bool = False
) -> tuple[ServingProjectionPayload, ...]:
    activated_at = FIXTURE_BUILT_AT - timedelta(days=1)
    first = alert_window_start(count_as_of=FIXTURE_BUILT_AT, activated_at=activated_at)
    signals = [record.signal for record in _signal_bundle(FIXTURE_BUILT_AT)[0]]
    sources = {
        "signal": signals,
        "monitor_event": _monitor_events(),
        "surge_event": _timeline_surge_events("baseline"),
    }
    generation = _generation_ids("baseline", 0)["signals"]
    event_rows: list[dict[str, object]] = []
    coverage_rows: list[dict[str, object]] = []
    for source, facts in sources.items():
        rows = sorted(
            (
                {
                    "source": source,
                    "alert_id": stable_alert_id(source, fact),
                    "occurred_at": alert_event_at(source, fact).isoformat(),
                    "confirmation_id": None,
                    "confirmed_at": None,
                    "eligible": True,
                }
                for fact in facts
            ),
            key=lambda row: str(row["alert_id"]),
        )
        if missing_observed_rows:
            rows = []
        event_rows.extend(rows)
        coverage_rows.append(
            {
                "source": source,
                "state": "complete",
                "reason": None,
                "window_start": first.isoformat(),
                "window_end": FIXTURE_BUILT_AT.isoformat(),
                "count_as_of": FIXTURE_BUILT_AT.isoformat(),
                "source_generation_id": (
                    "wrong-source-generation"
                    if source == "monitor_event" and bad_monitor_generation
                    else generation
                ),
                "high_watermark": "verified-through-session",
                "row_count": len(rows),
                "row_digest": canonical_sha256(
                    {"contract": "alert-observed-rows/v1", "source": source, "rows": tuple(rows)}
                ),
            }
        )
    snapshot = AlertAckAuthoritySnapshot.create(activated_at=activated_at, rows=[])
    return (
        _projection(
            "alert_ack_state",
            [
                {
                    "snapshot_key": "current",
                    "state": "ready",
                    "activated_at": activated_at.isoformat(),
                    "row_count": snapshot.row_count,
                    "rows_sha256": snapshot.rows_sha256,
                }
            ],
        ),
        _projection("alert_ack", []),
        _projection("alert_event", event_rows),
        _projection("alert_source_coverage", coverage_rows),
        _projection(
            "alert_overview",
            [
                {
                    "snapshot_key": "current",
                    "state": "ready",
                    "unacknowledged_count": len(event_rows),
                    "count_as_of": FIXTURE_BUILT_AT.isoformat(),
                    "activated_at": activated_at.isoformat(),
                }
            ],
        ),
    )


def test_complete_projection_must_match_all_same_generation_source_rows(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    build_web_fixture(
        root,
        "baseline",
        signal_projections=_complete_alert_projections(missing_observed_rows=True),
    )
    with TestClient(_app(root)) as client:
        timeline = client.get("/api/v1/monitor/timeline").json()["data"]
        overview = client.get("/api/v1/overview").json()["data"]
    assert timeline["total"] == 4
    assert timeline["unacknowledged"] == overview["unacknowledged"]
    assert timeline["unacknowledged"]["count"] is None
    assert timeline["unacknowledged"]["state"] == "source_incomplete"
    assert all(item["acknowledgment"]["eligible"] is False for item in timeline["items"])


def test_complete_projection_with_matching_source_rows_is_usable(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline", signal_projections=_complete_alert_projections())
    with TestClient(_app(root)) as client:
        timeline = client.get("/api/v1/monitor/timeline").json()["data"]
        overview = client.get("/api/v1/overview").json()["data"]
    assert timeline["unacknowledged"] == overview["unacknowledged"]
    assert timeline["unacknowledged"]["count"] == 4
    assert timeline["unacknowledged"]["state"] == "ready"
    assert all(item["acknowledgment"]["eligible"] is True for item in timeline["items"])


def test_complete_window_digest_ignores_later_alert_projection_event(tmp_path: Path) -> None:
    cutoff = FIXTURE_BUILT_AT - timedelta(minutes=1)
    late_alert_id = "e" * 64
    projections = []
    for projection in _complete_alert_projections():
        rows = [dict(row) for row in projection.rows]
        if projection.table_name == "alert_source_coverage":
            for row in rows:
                row["window_end"] = cutoff.isoformat()
                row["count_as_of"] = cutoff.isoformat()
        elif projection.table_name == "alert_overview":
            rows[0]["count_as_of"] = cutoff.isoformat()
        elif projection.table_name == "alert_event":
            rows.append(
                {
                    "source": "signal",
                    "alert_id": late_alert_id,
                    "occurred_at": (FIXTURE_BUILT_AT - timedelta(seconds=30)).isoformat(),
                    "confirmation_id": None,
                    "confirmed_at": None,
                    "eligible": False,
                }
            )
        projections.append(_projection(projection.table_name, rows))
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline", signal_projections=tuple(projections))

    with TestClient(_app(root)) as client:
        timeline = client.get("/api/v1/monitor/timeline").json()["data"]
        overview = client.get("/api/v1/overview").json()["data"]

    assert timeline["unacknowledged"] == overview["unacknowledged"]
    assert timeline["unacknowledged"]["state"] == "ready"
    assert timeline["unacknowledged"]["count"] == 4
    assert timeline["total"] == 4


def test_complete_projection_rejects_wrong_source_generation(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    build_web_fixture(
        root,
        "baseline",
        signal_projections=_complete_alert_projections(bad_monitor_generation=True),
    )
    with TestClient(_app(root)) as client:
        data = client.get("/api/v1/monitor/timeline").json()["data"]
    assert data["unacknowledged"]["count"] is None
    assert all(item["acknowledgment"]["eligible"] is False for item in data["items"])


def test_complete_projection_rejects_missing_source_table(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tests.support import web_serving_fixture as fixture

    original = fixture._projections

    def without_monitor(*args, **kwargs):
        return tuple(
            item for item in original(*args, **kwargs) if item.table_name != "monitor_event"
        )

    monkeypatch.setattr(fixture, "_projections", without_monitor)
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline", signal_projections=_complete_alert_projections())
    with TestClient(_app(root)) as client:
        data = client.get("/api/v1/monitor/timeline").json()["data"]
    assert data["unacknowledged"]["count"] is None
    assert all(item["acknowledgment"]["eligible"] is False for item in data["items"])


def test_complete_projection_rejects_invalid_source_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tests.support import web_serving_fixture as fixture

    projections = _complete_alert_projections()
    monkeypatch.setattr(
        fixture,
        "_monitor_events",
        lambda: [{**_monitor_events()[0], "trigger_time": "2026-09-23T01:05:00Z"}],
    )
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline", signal_projections=projections)
    with TestClient(_app(root)) as client:
        data = client.get("/api/v1/monitor/timeline").json()["data"]
    assert data["unacknowledged"]["count"] is None
    assert all(item["acknowledgment"]["eligible"] is False for item in data["items"])


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


def test_oversized_ack_body_stops_before_command_lookup(tmp_path: Path) -> None:
    def forbidden_lookup(_payload: dict[str, object]) -> dict[str, object]:
        pytest.fail("oversized request reached PageControl")

    with TestClient(_app(tmp_path / "no-serving", lookup=forbidden_lookup)) as client:
        response = client.post(
            "/api/v1/monitor/ack",
            content=b"x" * 4097,
            headers={**HEADERS, "content-type": "application/json"},
        )
    assert response.status_code == 413


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


def test_new_ack_uses_current_complete_serving_and_private_admission(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    manifest = build_web_fixture(root, "baseline", signal_projections=_complete_alert_projections())
    alert_id = stable_alert_id("monitor_event", _monitor_events()[0])
    order: list[str] = []
    submitted = []

    def lookup(_payload: dict[str, object]) -> dict[str, object]:
        order.append("lookup")
        return {"found": False}

    class Admission:
        def submit(self, command: AckAlert) -> PageControlReceipt:
            order.append("admission")
            submitted.append(command)
            return _receipt(command.command_id, "pending")

    with TestClient(_app(root, lookup=lookup, admission=Admission())) as client:
        response = client.post(
            "/api/v1/monitor/ack",
            json={**_body(), "generation_id": manifest.generation_id, "alert_id": alert_id},
            headers=HEADERS,
        )
    assert response.status_code == 200, response.text
    assert response.json() == {
        "command_id": "ack-original",
        "status": "pending",
        "confirmation_id": None,
        "message": "已受理，等待处理",
    }
    assert order == ["lookup", "admission"]
    assert len(submitted) == 1
    assert submitted[0].generation_id == manifest.generation_id
    assert submitted[0].alert_id == alert_id
    assert submitted[0].actor_id == "researcher"


def test_complete_serving_does_not_enable_new_ack_without_socket(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    manifest = build_web_fixture(root, "baseline", signal_projections=_complete_alert_projections())
    with TestClient(_app(root, lookup=lambda _payload: {"found": False})) as client:
        response = client.post(
            "/api/v1/monitor/ack",
            json={
                **_body(),
                "generation_id": manifest.generation_id,
                "alert_id": stable_alert_id("monitor_event", _monitor_events()[0]),
            },
            headers=HEADERS,
        )
    assert response.status_code == 503
    assert "尚未就绪" in response.json()["detail"]


def _receipt(
    command_id: str, status: str, *, confirmation_id: str | None = None
) -> PageControlReceipt:
    return PageControlReceipt.model_validate(
        {
            "command_id": command_id,
            "status": status,
            "enqueued_at": NOW.isoformat(),
            "completed_at": NOW.isoformat() if status == "succeeded" else None,
            "result": {"confirmation_id": confirmation_id} if confirmation_id else None,
            "error": None,
        }
    )


def test_pending_original_resumes_over_private_admission_without_serving(tmp_path: Path) -> None:
    observed: list[AckAlert] = []

    def lookup(payload: dict[str, object]) -> dict[str, object]:
        return {
            "found": True,
            "receipt": _receipt(str(payload["command_id"]), "pending").model_dump(mode="json"),
        }

    class Admission:
        def submit(self, command: AckAlert) -> PageControlReceipt:
            observed.append(command)
            return _receipt(command.command_id, "succeeded", confirmation_id="ack-first")

    with TestClient(_app(tmp_path / "no-serving", lookup=lookup, admission=Admission())) as client:
        response = client.post("/api/v1/monitor/ack", json=_body(), headers=HEADERS)
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "succeeded"
    assert response.json()["confirmation_id"] == "ack-first"
    assert len(observed) == 1
    assert observed[0].command_id == _body()["command_id"]


@pytest.mark.parametrize(
    "failure",
    [AckAdmissionRejectedError("private path"), AckAdmissionUnavailableError("private path")],
)
def test_pending_original_admission_failure_returns_durable_pending(
    tmp_path: Path, failure: Exception
) -> None:
    lookups = 0

    def lookup(payload: dict[str, object]) -> dict[str, object]:
        nonlocal lookups
        lookups += 1
        return {
            "found": True,
            "receipt": _receipt(str(payload["command_id"]), "pending").model_dump(mode="json"),
        }

    class Admission:
        def submit(self, _command: AckAlert) -> PageControlReceipt:
            raise failure

    with TestClient(_app(tmp_path / "no-serving", lookup=lookup, admission=Admission())) as client:
        response = client.post("/api/v1/monitor/ack", json=_body(), headers=HEADERS)
    assert response.status_code == 200
    assert response.json()["status"] == "pending"
    assert response.json()["confirmation_id"] is None
    assert lookups == 2
    assert "private path" not in response.text


def test_pending_original_rechecks_durable_receipt_after_private_response_loss(
    tmp_path: Path,
) -> None:
    lookups = 0

    def lookup(payload: dict[str, object]) -> dict[str, object]:
        nonlocal lookups
        lookups += 1
        receipt = _receipt(
            str(payload["command_id"]),
            "pending" if lookups == 1 else "succeeded",
            confirmation_id="ack-first" if lookups == 2 else None,
        )
        return {"found": True, "receipt": receipt.model_dump(mode="json")}

    class Admission:
        def submit(self, _command: AckAlert) -> PageControlReceipt:
            raise AckAdmissionUnavailableError("response lost")

    with TestClient(_app(tmp_path / "no-serving", lookup=lookup, admission=Admission())) as client:
        response = client.post("/api/v1/monitor/ack", json=_body(), headers=HEADERS)
    assert response.status_code == 200
    assert response.json()["status"] == "succeeded"
    assert response.json()["confirmation_id"] == "ack-first"
    assert lookups == 2


def test_pending_original_needs_second_lookup_if_private_admission_fails(tmp_path: Path) -> None:
    lookups = 0

    def lookup(payload: dict[str, object]) -> dict[str, object]:
        nonlocal lookups
        lookups += 1
        if lookups == 2:
            raise OSError("private database path")
        return {
            "found": True,
            "receipt": _receipt(str(payload["command_id"]), "pending").model_dump(mode="json"),
        }

    class Admission:
        def submit(self, _command: AckAlert) -> PageControlReceipt:
            raise AckAdmissionUnavailableError("response lost")

    with TestClient(_app(tmp_path / "no-serving", lookup=lookup, admission=Admission())) as client:
        response = client.post("/api/v1/monitor/ack", json=_body(), headers=HEADERS)
    assert response.status_code == 503
    assert lookups == 2
    assert "private database path" not in response.text


def test_pending_original_rechecks_if_private_receipt_lacks_confirmation_id(
    tmp_path: Path,
) -> None:
    lookups = 0

    def lookup(payload: dict[str, object]) -> dict[str, object]:
        nonlocal lookups
        lookups += 1
        receipt = _receipt(
            str(payload["command_id"]),
            "pending" if lookups == 1 else "succeeded",
            confirmation_id="ack-first" if lookups == 2 else None,
        )
        return {"found": True, "receipt": receipt.model_dump(mode="json")}

    class Admission:
        def submit(self, command: AckAlert) -> PageControlReceipt:
            return _receipt(command.command_id, "succeeded")

    with TestClient(_app(tmp_path / "no-serving", lookup=lookup, admission=Admission())) as client:
        response = client.post("/api/v1/monitor/ack", json=_body(), headers=HEADERS)
    assert response.status_code == 200
    assert response.json()["confirmation_id"] == "ack-first"
    assert lookups == 2


def test_new_ack_rejects_old_generation_before_private_admission(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline", signal_projections=_complete_alert_projections())

    class Admission:
        def submit(self, _command: AckAlert) -> PageControlReceipt:
            pytest.fail("stale generation reached private admission")

    app = _app(root, lookup=lambda _payload: {"found": False}, admission=Admission())
    with TestClient(app) as client:
        response = client.post("/api/v1/monitor/ack", json=_body(), headers=HEADERS)
    assert response.status_code == 409
    assert "刷新" in response.json()["detail"]


def test_new_ack_rejects_incomplete_source_before_private_admission(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    manifest = build_web_fixture(root, "baseline")

    class Admission:
        def submit(self, _command: AckAlert) -> PageControlReceipt:
            pytest.fail("incomplete source reached private admission")

    app = _app(root, lookup=lambda _payload: {"found": False}, admission=Admission())
    with TestClient(app) as client:
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


def test_new_ack_rejects_unknown_alert_in_complete_source(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    manifest = build_web_fixture(root, "baseline", signal_projections=_complete_alert_projections())

    class Admission:
        def submit(self, _command: AckAlert) -> PageControlReceipt:
            pytest.fail("unknown alert reached private admission")

    app = _app(root, lookup=lambda _payload: {"found": False}, admission=Admission())
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/monitor/ack",
            json={**_body(), "generation_id": manifest.generation_id},
            headers=HEADERS,
        )
    assert response.status_code == 409


@pytest.mark.parametrize(
    ("failure", "expected_status"),
    [
        (AckAdmissionRejectedError("private path"), 409),
        (AckAdmissionUnavailableError("private path"), 503),
    ],
)
def test_private_admission_failure_closes_new_write(
    tmp_path: Path, failure: Exception, expected_status: int
) -> None:
    root = tmp_path / "serving"
    manifest = build_web_fixture(root, "baseline", signal_projections=_complete_alert_projections())

    class Admission:
        def submit(self, _command: AckAlert) -> PageControlReceipt:
            raise failure

    app = _app(root, lookup=lambda _payload: {"found": False}, admission=Admission())
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/monitor/ack",
            json={
                **_body(),
                "generation_id": manifest.generation_id,
                "alert_id": stable_alert_id("monitor_event", _monitor_events()[0]),
            },
            headers=HEADERS,
        )
    assert response.status_code == expected_status
    assert "private path" not in response.text


@pytest.mark.parametrize(
    "invalid_receipt",
    [
        _receipt("other-command", "pending"),
        _receipt("ack-original", "succeeded"),
    ],
)
def test_private_admission_invalid_receipt_does_not_claim_success(
    tmp_path: Path, invalid_receipt: PageControlReceipt
) -> None:
    root = tmp_path / "serving"
    manifest = build_web_fixture(root, "baseline", signal_projections=_complete_alert_projections())

    class Admission:
        def submit(self, _command: AckAlert) -> PageControlReceipt:
            return invalid_receipt

    app = _app(root, lookup=lambda _payload: {"found": False}, admission=Admission())
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/monitor/ack",
            json={
                **_body(),
                "generation_id": manifest.generation_id,
                "alert_id": stable_alert_id("monitor_event", _monitor_events()[0]),
            },
            headers=HEADERS,
        )
    assert response.status_code == 502
    assert "回执无法核对" in response.json()["detail"]


def test_lost_private_response_recovers_original_id_on_exact_retry(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    manifest = build_web_fixture(root, "baseline", signal_projections=_complete_alert_projections())
    submitted: list[AckAlert] = []
    saved: PageControlReceipt | None = None

    def lookup(_payload: dict[str, object]) -> dict[str, object]:
        if saved is None:
            return {"found": False}
        return {"found": True, "receipt": saved.model_dump(mode="json")}

    class Admission:
        def submit(self, command: AckAlert) -> PageControlReceipt:
            nonlocal saved
            submitted.append(command)
            saved = _receipt(command.command_id, "succeeded", confirmation_id="ack-first")
            raise AckAdmissionUnavailableError("response lost")

    body = {
        **_body(),
        "generation_id": manifest.generation_id,
        "alert_id": stable_alert_id("monitor_event", _monitor_events()[0]),
    }
    with TestClient(_app(root, lookup=lookup, admission=Admission())) as client:
        first = client.post("/api/v1/monitor/ack", json=body, headers=HEADERS)
        retry = client.post("/api/v1/monitor/ack", json=body, headers=HEADERS)
    assert first.status_code == 503
    assert retry.status_code == 200
    assert retry.json()["confirmation_id"] == "ack-first"
    assert len(submitted) == 1


def test_web_ack_uses_real_private_socket_and_recovers_same_receipt(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    manifest = build_web_fixture(root, "baseline", signal_projections=_complete_alert_projections())
    service = build_page_control_service(
        outbox_path=tmp_path / "control" / "page-control.sqlite3",
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
        allowed_lab_export_roots=(tmp_path / "exports",),
        load_default_lab_backend=False,
        clock=lambda: NOW,
    )
    service.outbox.activate_alert_ack(FIXTURE_BUILT_AT - timedelta(days=1))

    def lookup(payload: dict[str, object]) -> dict[str, object]:
        original = service.lookup_ack_command(AckAlert.model_validate(payload))
        if original is None:
            return {"found": False}
        return {"found": True, "receipt": original.model_dump(mode="json")}

    with TemporaryDirectory(prefix="rqa-", dir=SHORT_TMP) as directory:
        socket_path = Path(directory) / "ack.sock"
        server = build_ack_admission_server(
            AckAdmission(service, root, clock=lambda: NOW), socket_path=socket_path
        )
        assert server is not None
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            app = create_app(
                WebSettings(
                    serving_root=root,
                    ack_admission_socket_path=socket_path,
                    ingress_socket_path=tmp_path / "web-private" / "web.sock",
                ),
                clock=lambda: NOW,
                background=False,
                ack_lookup_transport=lookup,
            )
            body = {
                **_body(),
                "generation_id": manifest.generation_id,
                "alert_id": stable_alert_id("monitor_event", _monitor_events()[0]),
            }
            with TestClient(app) as client:
                first = client.post("/api/v1/monitor/ack", json=body, headers=HEADERS)
                retry = client.post("/api/v1/monitor/ack", json=body, headers=HEADERS)
            assert first.status_code == retry.status_code == 200
            assert first.json() == retry.json()
            assert first.json()["status"] == "succeeded"
            assert first.json()["confirmation_id"] == "ack-original"
        finally:
            server.shutdown()
            server.server_close()
            worker.join(timeout=2)


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
