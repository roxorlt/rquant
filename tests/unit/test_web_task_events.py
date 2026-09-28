"""Research task events are a gated, bounded view of one Serving generation."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from uuid import UUID

import pytest

from rquant.lab_jobs import JobStatus
from rquant.serving_read_models import ServingProjectionPayload
from rquant.web.settings import WebSettings
from tests.support.web_proxy_identity import ProofTestClient as TestClient
from tests.support.web_proxy_identity import create_proof_test_app as create_app
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture
from tests.unit.test_web_tasks import _job

TASK_ID = str(UUID(int=1))
OTHER_ID = str(UUID(int=2))
URL = f"/api/v1/tasks/jobs/{TASK_ID}/events"
ADMIN = {"X-Rquant-User": "liutong"}


def _app(root: Path, *, private: bool = True, admins: frozenset[str] = frozenset({"liutong"})):
    return create_app(
        WebSettings(
            serving_root=root,
            ingress_socket_path=(root / "ingress" / "web.sock") if private else None,
            log_admin_users=admins,
        ),
        clock=lambda: FIXTURE_BUILT_AT + timedelta(seconds=30),
        background=False,
    )


def _projections(
    *,
    state: str = "available",
    label: str = "任务已开始",
    count: int = 1,
    truncated: bool = False,
) -> tuple[ServingProjectionPayload, ...]:
    at = FIXTURE_BUILT_AT - timedelta(minutes=10 if count > 60 else 1)
    return (
        ServingProjectionPayload(
            table_name="lab_job_event_window",
            available_at=FIXTURE_BUILT_AT,
            rows=(
                {
                    "job_id": TASK_ID,
                    "job_version": 1,
                    "state": state,
                    "retained_count": count,
                    "truncated": truncated,
                },
            ),
        ),
        ServingProjectionPayload(
            table_name="lab_job_event",
            available_at=FIXTURE_BUILT_AT,
            rows=tuple(
                {
                    "job_id": TASK_ID,
                    "event_id": index + 1,
                    "job_version": 1,
                    "occurred_at": (at + timedelta(seconds=index)).isoformat(),
                    "new_status": "running",
                    "label": label,
                }
                for index in range(count)
            ),
        ),
    )


def _publish(
    root: Path,
    *,
    projections: tuple[ServingProjectionPayload, ...] = (),
    sequence: int = 0,
) -> None:
    build_web_fixture(
        root,
        "baseline",
        sequence=sequence,
        lab_jobs=(
            _job(
                1,
                status=JobStatus.RUNNING,
                updated_at=FIXTURE_BUILT_AT - timedelta(minutes=1),
            ),
        ),
        audit_report_projections=projections,
    )


@pytest.mark.parametrize(
    ("private", "admins", "headers", "status"),
    (
        (False, frozenset({"liutong"}), ADMIN, 401),
        (True, frozenset(), ADMIN, 503),
        (True, frozenset({"liutong"}), {}, 401),
        (True, frozenset({"liutong"}), {"X-Rquant-User": "other"}, 403),
    ),
)
def test_task_events_fail_closed_before_reading_data(
    tmp_path: Path,
    private: bool,
    admins: frozenset[str],
    headers: dict[str, str],
    status: int,
) -> None:
    with TestClient(_app(tmp_path / "missing", private=private, admins=admins)) as client:
        response = client.get(URL, headers=headers)
    assert response.status_code == status
    assert "lab_job" not in response.text


def test_admin_list_is_explicit_and_rejects_wildcards(tmp_path: Path) -> None:
    ingress = tmp_path / "web.sock"
    settings = WebSettings.from_env(
        {
            "RQUANT_WEB_INGRESS_SOCKET": str(ingress),
            "RQUANT_WEB_LOG_ADMIN_USERS": "liutong,mei",
        }
    )
    assert settings.log_admin_users == frozenset({"liutong", "mei"})
    assert WebSettings.from_env({}).log_admin_users == frozenset()
    for value in ("*", "liutong,", "liutong,other user"):
        with pytest.raises(ValueError):
            WebSettings.from_env({"RQUANT_WEB_LOG_ADMIN_USERS": value})


def test_overview_exposes_only_server_computed_log_capability(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root)
    with TestClient(_app(root)) as client:
        admin = client.get("/api/v1/tasks/overview", headers=ADMIN)
        other = client.get("/api/v1/tasks/overview", headers={"X-Rquant-User": "other"})
        anonymous = client.get("/api/v1/tasks/overview")
    with TestClient(_app(root, private=False)) as client:
        public = client.get("/api/v1/tasks/overview", headers=ADMIN)
    assert admin.json()["data"]["can_view_research_logs"] is True
    assert other.json()["data"]["can_view_research_logs"] is False
    assert anonymous.status_code == public.status_code == 401


def test_published_events_return_only_safe_fields(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root, projections=_projections())
    with TestClient(_app(root)) as client:
        response = client.get(URL, headers=ADMIN)
    assert response.status_code == 200
    data = response.json()
    assert data["state"] == "ready"
    assert data["generation_id"]
    assert data["updated_at"]
    assert data["truncated"] is False
    assert data["events"] == [
        {
            "event_id": 1,
            "occurred_at": (FIXTURE_BUILT_AT - timedelta(minutes=1))
            .isoformat()
            .replace("+00:00", "Z"),
            "label": "任务已开始",
            "status_label": "运行中",
        }
    ]
    for forbidden in ("reason", "request_id", "fencing", "new_status", "job_version"):
        assert forbidden not in response.text


def test_changed_generation_clears_old_drawer(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root, projections=_projections())
    app = _app(root)
    with TestClient(app) as client:
        current = client.get(URL, headers=ADMIN)
        old = current.json()["generation_id"]
        _publish(root, projections=_projections(), sequence=1)
        app.state.web.tracker.refresh()
        changed = client.get(URL, params={"generation_id": old}, headers=ADMIN)
    assert changed.status_code == 409
    assert "重新" in changed.text
    assert "任务已开始" not in changed.text


def test_old_generation_without_event_projection_is_not_published(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root)
    with TestClient(_app(root)) as client:
        response = client.get(URL, headers=ADMIN)
    assert response.status_code == 200
    assert response.json()["state"] == "not_published"
    assert response.json()["events"] == []


def test_no_serving_generation_is_unavailable_without_source_details(tmp_path: Path) -> None:
    with TestClient(_app(tmp_path / "missing")) as client:
        response = client.get(URL, headers=ADMIN)
    assert response.status_code == 200
    assert response.json()["state"] == "unavailable"
    assert response.json()["generation_id"] is None
    assert response.json()["events"] == []


def test_empty_events_and_unincluded_job_are_distinct(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root, projections=_projections(state="empty", count=0))
    with TestClient(_app(root)) as client:
        empty = client.get(URL, headers=ADMIN)
        not_included = client.get(f"/api/v1/tasks/jobs/{OTHER_ID}/events", headers=ADMIN)
    assert empty.status_code == not_included.status_code == 200
    assert empty.json()["state"] == "empty"
    assert not_included.json()["state"] == "not_included"
    assert empty.json()["events"] == not_included.json()["events"] == []


def test_truncation_is_explicit_and_events_are_newest_first(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root, projections=_projections(state="truncated", count=2, truncated=True))
    with TestClient(_app(root)) as client:
        response = client.get(URL, headers=ADMIN)
    data = response.json()
    assert data["state"] == "truncated"
    assert data["truncated"] is True
    assert [event["event_id"] for event in data["events"]] == [2, 1]
    assert "最近" in data["note"]


def test_untrusted_event_label_does_not_leave_api(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    secret = "Bearer secret-token /private/path request_id=abc"
    _publish(root, projections=_projections(label=secret))
    with TestClient(_app(root)) as client:
        response = client.get(URL, headers=ADMIN)
    assert response.status_code == 200
    assert response.json()["state"] == "unavailable"
    assert response.json()["events"] == []
    assert secret not in response.text


def test_event_window_count_mismatch_hides_entire_result(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    window, event = _projections()
    broken = ServingProjectionPayload(
        table_name="lab_job_event_window",
        available_at=FIXTURE_BUILT_AT,
        rows=({**window.rows[0], "retained_count": 2},),
    )
    _publish(root, projections=(broken, event))
    with TestClient(_app(root)) as client:
        response = client.get(URL, headers=ADMIN)
    assert response.status_code == 200
    assert response.json()["state"] == "unavailable"
    assert response.json()["events"] == []


def test_more_than_five_hundred_events_cannot_be_returned(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root, projections=_projections(state="truncated", count=501, truncated=True))
    with TestClient(_app(root)) as client:
        response = client.get(URL, headers=ADMIN)
    assert response.status_code == 200
    assert response.json()["state"] == "unavailable"
    assert response.json()["events"] == []
