"""One bounded task overview from one verified Serving generation."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant.lab_jobs import JobStatus
from rquant.ops_status import (
    STATIC_TIMER_STEMS,
    OpsResourceEvidence,
    OpsSnapshot,
    OpsUnitEvidence,
)
from rquant.ops_status_serving import ops_status_projections
from rquant.runtime_service_control import (
    RuntimeServiceHealth,
    RuntimeServicePlane,
    RuntimeServiceStatus,
)
from rquant.serving_contracts import FreshnessStatus
from rquant.serving_read_models import ServingProjectionInput
from rquant.web.market import MarketPhase
from rquant.web.models.tasks import ResourcesData, ScheduledTasksData
from rquant.web.settings import WebSettings
from rquant.web.task_overview import _timer_status
from tests.support import web_serving_fixture as fixture
from tests.support.web_proxy_identity import ResearcherTestClient as TestClient
from tests.support.web_proxy_identity import create_private_test_app as create_app
from tests.unit.test_web_tasks import _job

_BASE_DATASETS = fixture._DATASETS
_BASE_PROJECTIONS = fixture._projections
_BASE_WATERMARKS = fixture._watermarks
_BASE_RUNTIME_SERVICES = fixture._runtime_services
_SLICES = (
    "rquant.slice",
    "rquant-live.slice",
    "rquant-serving.slice",
    "rquant-research.slice",
    "rquant-maintenance.slice",
)


def _sample(at: datetime) -> OpsSnapshot:
    return OpsSnapshot(
        sampled_at=at,
        host_name="synthetic-host",
        boot_id="12345678-1234-1234-1234-123456789abc",
        manifest_digest="a" * 64,
        host_memory_total_bytes=10_000,
        host_memory_available_bytes=4_000,
        units=tuple(
            OpsUnitEvidence(
                timer=f"rquant-{stem}.timer",
                service=f"rquant-{stem}.service",
                label="每日选股" if stem == "daily" else f"定时任务{index}",
                expected_enabled=stem != "backup",
                session="market_hours" if stem == "monitor" else "all",
                resource_group="maintenance",
                timer_load_state="loaded",
                timer_unit_file_state="disabled" if stem == "backup" else "enabled",
                timer_active_state="inactive" if stem == "monitor" else "active",
                timer_sub_state="waiting",
                service_load_state="loaded",
                last_trigger_at=at - timedelta(hours=1),
                next_at=(
                    at + timedelta(seconds=10)
                    if stem == "monitor-watchdog"
                    else at + timedelta(hours=1)
                ),
                service_result="exit-code" if stem == "daily" else None,
            )
            for index, stem in enumerate(STATIC_TIMER_STEMS)
        ),
        resources=tuple(
            OpsResourceEvidence(
                slice_name=name,
                load_state="loaded",
                active_state="active",
                memory_current_bytes=1_000 if name == "rquant.slice" else 700,
                memory_peak_bytes=2_000 if name == "rquant.slice" else 900,
            )
            for name in _SLICES
        ),
    )


def _publish(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    sequence: int = 0,
    ops_available: bool = True,
    ops_partial: bool = False,
    health_available: bool = True,
    jobs: bool = False,
    built_at: datetime | None = None,
    health_rows: int | None = None,
    long_job_name: bool = False,
    ops_sample_transform: Callable[[OpsSnapshot], OpsSnapshot] | None = None,
) -> datetime:
    if built_at is not None:
        monkeypatch.setattr(
            fixture, "fixture_built_at", lambda selected: built_at + timedelta(minutes=selected)
        )
    at = fixture.fixture_built_at(sequence)
    monkeypatch.setattr(fixture, "_DATASETS", (*_BASE_DATASETS, "ops_status"))
    if health_rows is not None:

        def runtime_services(observed_at: datetime):
            base = _BASE_RUNTIME_SERVICES(observed_at)
            return base + tuple(
                RuntimeServiceHealth(
                    service_id=f"extra-role-{index}.primary.v1",
                    plane=RuntimeServicePlane.RESEARCH,
                    status=RuntimeServiceStatus.MISSING,
                    stale=True,
                    observed_at=observed_at,
                )
                for index in range(health_rows - len(base))
            )

        monkeypatch.setattr(fixture, "_runtime_services", runtime_services)

    def projections(*args: object, **kwargs: object):
        result = _BASE_PROJECTIONS(*args, **kwargs)
        if not ops_available:
            return result
        sample = _sample(at - timedelta(seconds=40))
        if ops_sample_transform is not None:
            sample = ops_sample_transform(sample)
        ops_projections = ops_status_projections(sample)
        if ops_partial:
            ops_projections = tuple(
                projection.model_copy(update={"rows": projection.rows[:-1]})
                if projection.table_name == "ops_unit_status"
                else projection
                for projection in ops_projections
            )
        return result + tuple(
            ServingProjectionInput.bind(
                projection,
                owner_dataset_id="ops_status",
                owner_generation_id=kwargs["generations"]["ops_status"],
            )
            for projection in ops_projections
        )

    def watermarks(*args: object, **kwargs: object):
        result = _BASE_WATERMARKS(*args, **kwargs)
        return tuple(
            mark.model_copy(
                update={
                    "status": FreshnessStatus.UNAVAILABLE,
                    "reason": "known unavailable",
                }
            )
            if (mark.dataset_id == "ops_status" and not ops_available)
            or (mark.dataset_id == "runtime_health" and not health_available)
            else mark
            for mark in result
        )

    monkeypatch.setattr(fixture, "_projections", projections)
    monkeypatch.setattr(fixture, "_watermarks", watermarks)
    first_job = _job(1, status=JobStatus.RUNNING, updated_at=at - timedelta(minutes=1))
    if long_job_name:
        first_job = first_job.model_copy(
            update={
                "summary": first_job.summary.model_copy(update={"strategy_name": "研究" * 5_000})
            }
        )
    fixture.build_web_fixture(
        root,
        "baseline",
        sequence=sequence,
        lab_jobs=(
            (
                first_job,
                _job(2, status=JobStatus.QUEUED, updated_at=at - timedelta(minutes=2)),
            )
            if jobs
            else ()
        ),
    )
    return at


def _app(root: Path, now: datetime):
    return create_app(WebSettings(serving_root=root), clock=lambda: now, background=False)


@pytest.mark.parametrize("service_load_state", (None, "not-found"))
def test_active_timer_without_its_installed_service_is_not_healthy(
    service_load_state: str | None,
) -> None:
    now = fixture.FIXTURE_BUILT_AT
    timer = next(
        item for item in _sample(now).units if item.timer == "rquant-monitor-watchdog.timer"
    ).model_copy(update={"service_load_state": service_load_state})

    status = _timer_status(timer, MarketPhase.CONTINUOUS, now)

    assert status.state == "crit"
    assert status.label == "异常"


@pytest.mark.parametrize("phase", (MarketPhase.NON_TRADING_DAY, MarketPhase.PRE_OPEN))
def test_failed_market_timer_remains_an_alarm_outside_market_hours(
    phase: MarketPhase,
) -> None:
    now = fixture.FIXTURE_BUILT_AT
    timer = next(
        item for item in _sample(now).units if item.timer == "rquant-monitor.timer"
    ).model_copy(update={"timer_active_state": "failed"})

    status = _timer_status(timer, phase, now)

    assert status.state == "crit"
    assert status.label == "异常"


def test_overview_uses_one_borrow_and_keeps_results_unknown_without_receipts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "serving"
    at = _publish(root, monkeypatch, jobs=True)
    app = _app(root, at + timedelta(seconds=30))
    with TestClient(app) as client:
        calls = 0
        original_borrow = app.state.web.tracker.borrow

        @contextmanager
        def counted_borrow():
            nonlocal calls
            calls += 1
            with original_borrow() as borrowed:
                yield borrowed

        monkeypatch.setattr(app.state.web.tracker, "borrow", counted_borrow)
        response = client.get("/api/v1/tasks/overview", params={"page_size": 1})
    assert response.status_code == 200, response.text
    assert calls == 1
    data = response.json()["data"]
    assert data["scheduled"]["source_state"] == "ready"
    assert data["scheduled"]["source_updated_at"] is not None
    assert data["scheduled"]["expires_at"] is not None
    assert len(data["scheduled"]["items"]) == 14
    daily = next(item for item in data["scheduled"]["items"] if item["name"] == "每日选股")
    assert daily["result_label"] == "未知"
    assert daily["duration_seconds"] is None
    assert daily["status"]["state"] == "warn"
    assert "rquant-daily.timer" not in daily["status"]["reason"]
    assert next(
        item for item in data["scheduled"]["items"] if item["timer_unit"] == "rquant-backup.timer"
    )["status"]["label"] == "未运行"
    assert next(
        item for item in data["scheduled"]["items"] if item["timer_unit"] == "rquant-monitor.timer"
    )["status"]["label"] == "已收盘"
    assert next(
        item
        for item in data["scheduled"]["items"]
        if item["timer_unit"] == "rquant-monitor-watchdog.timer"
    )["status"]["state"] == "warn"
    assert data["services"]["source_state"] == "ready"
    assert data["services"]["items"]
    assert all("." not in item["name"] for item in data["services"]["items"])
    resources = data["resources"]
    assert resources["source_state"] == "ready"
    assert resources["host_memory_total_bytes"] == 10_000
    assert resources["host_memory_available_bytes"] == 4_000
    assert resources["rquant_memory_current_bytes"] == 1_000
    assert resources["rquant_memory_peak_bytes"] == 2_000
    assert [item["memory_current_bytes"] for item in resources["groups"]] == [700] * 4
    assert resources["cpu_usage_percent"] is None
    assert resources["expires_at"] == data["scheduled"]["expires_at"]
    assert data["research"]["total"] == 2
    assert len(data["research"]["items"]) == 1
    assert data["research"]["next_cursor"]
    assert response.headers["X-Rquant-Generation"] == response.json()["serving"]["generation_id"]


def test_missing_or_inactive_slice_does_not_report_stale_memory_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def change_resources(sample: OpsSnapshot) -> OpsSnapshot:
        return sample.model_copy(
            update={
                "resources": tuple(
                    item.model_copy(update={"load_state": "not-found"})
                    if item.slice_name == "rquant.slice"
                    else item.model_copy(update={"active_state": "inactive"})
                    if item.slice_name == "rquant-live.slice"
                    else item
                    for item in sample.resources
                )
            }
        )

    root = tmp_path / "serving"
    at = _publish(root, monkeypatch, ops_sample_transform=change_resources)
    with TestClient(_app(root, at + timedelta(seconds=30))) as client:
        response = client.get("/api/v1/tasks/overview")

    assert response.status_code == 200, response.text
    resources = response.json()["data"]["resources"]
    assert resources["source_state"] == "ready"
    assert resources["rquant_memory_current_bytes"] is None
    assert resources["rquant_memory_peak_bytes"] is None
    assert resources["groups"][0]["memory_current_bytes"] is None
    assert resources["groups"][0]["memory_peak_bytes"] is None
    assert resources["groups"][1]["memory_current_bytes"] == 700
    assert resources["groups"][1]["memory_peak_bytes"] == 900


def test_overview_expires_at_full_120_seconds_even_when_serving_is_current(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "serving"
    at = _publish(root, monkeypatch)
    for age, expected in ((119.999, "ready"), (120, "unavailable")):
        with TestClient(_app(root, at - timedelta(seconds=40) + timedelta(seconds=age))) as client:
            response = client.get("/api/v1/tasks/overview")
        assert response.status_code == 200, response.text
        data = response.json()["data"]
        assert data["scheduled"]["source_state"] == expected
        assert data["resources"]["source_state"] == expected
        assert data["scheduled"]["remaining_seconds"] == data["resources"]["remaining_seconds"]
        if expected == "unavailable":
            assert data["scheduled"]["remaining_seconds"] == 0
            assert data["scheduled"]["expires_at"] is not None
            assert data["scheduled"]["items"] == []
            assert data["resources"]["rquant_memory_current_bytes"] is None
            assert data["resources"]["groups"] == []
        else:
            assert data["scheduled"]["remaining_seconds"] == pytest.approx(0.001)
            assert data["scheduled"]["expires_at"] is not None


def test_overview_missing_sources_do_not_promote_untrusted_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "serving"
    at = _publish(root, monkeypatch, ops_available=False, health_available=False)
    with TestClient(_app(root, at + timedelta(seconds=30))) as client:
        response = client.get("/api/v1/tasks/overview")
    assert response.status_code == 200, response.text
    data = response.json()["data"]
    assert data["scheduled"]["source_state"] == "unavailable"
    assert data["scheduled"]["remaining_seconds"] is None
    assert data["scheduled"]["expires_at"] is None
    assert data["scheduled"]["items"] == []
    assert data["services"]["source_state"] == "unavailable"
    assert data["services"]["items"] == []
    assert data["resources"]["source_state"] == "unavailable"
    assert data["resources"]["remaining_seconds"] is None
    assert data["resources"]["expires_at"] is None
    assert data["research"]["source_state"] == "empty"


def test_remaining_seconds_models_reject_false_ready_and_unbounded_values() -> None:
    scheduled = {
        "source_state": "ready",
        "source_label": "定时任务",
        "source_note": None,
        "source_updated_at": fixture.FIXTURE_BUILT_AT,
        "expires_at": fixture.FIXTURE_BUILT_AT + timedelta(seconds=120),
        "items": [],
    }
    for invalid in (None, 0, -0.001, 120.001, float("nan")):
        with pytest.raises(ValidationError):
            ScheduledTasksData.model_validate({**scheduled, "remaining_seconds": invalid})
    assert ScheduledTasksData.model_validate(
        {**scheduled, "remaining_seconds": 120}
    ).remaining_seconds == 120
    with pytest.raises(ValidationError):
        ScheduledTasksData.model_validate(
            {**scheduled, "source_state": "unavailable", "remaining_seconds": 1}
        )
    resource = {
        "source_state": "ready",
        "source_label": "资源使用",
        "source_note": None,
        "source_updated_at": fixture.FIXTURE_BUILT_AT,
        "expires_at": fixture.FIXTURE_BUILT_AT + timedelta(seconds=120),
        "host_memory_total_bytes": None,
        "host_memory_available_bytes": None,
        "rquant_memory_current_bytes": None,
        "rquant_memory_peak_bytes": None,
        "groups": [],
        "cpu_usage_percent": None,
        "cpu_note": "暂无可信 CPU 数据",
    }
    with pytest.raises(ValidationError):
        ResourcesData.model_validate({**resource, "remaining_seconds": None})
    assert ResourcesData.model_validate(
        {**resource, "source_state": "unavailable", "remaining_seconds": 0}
    ).remaining_seconds == 0


def test_legacy_generation_without_ops_watermark_keeps_both_ops_sections_unavailable(
    tmp_path: Path
) -> None:
    root = tmp_path / "legacy"
    fixture.build_web_fixture(root, "baseline")
    with TestClient(_app(root, fixture.FIXTURE_BUILT_AT + timedelta(seconds=30))) as client:
        response = client.get("/api/v1/tasks/overview")
    assert response.status_code == 200, response.text
    data = response.json()["data"]
    assert data["scheduled"]["source_state"] == "unavailable"
    assert data["scheduled"]["items"] == []
    assert data["resources"]["source_state"] == "unavailable"
    assert data["services"]["source_state"] == "ready"


def test_partial_ops_projection_cannot_show_a_cherry_picked_healthy_subset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "partial"
    at = _publish(root, monkeypatch, ops_partial=True)
    with TestClient(_app(root, at + timedelta(seconds=30))) as client:
        response = client.get("/api/v1/tasks/overview")
    assert response.status_code == 200, response.text
    data = response.json()["data"]
    assert data["scheduled"]["source_state"] == "unavailable"
    assert data["scheduled"]["items"] == []
    assert data["resources"]["source_state"] == "unavailable"
    assert data["services"]["source_state"] == "ready"


def test_oversize_runtime_roster_is_unavailable_instead_of_truncated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "serving"
    at = _publish(root, monkeypatch, health_rows=33)
    with TestClient(_app(root, at + timedelta(seconds=30))) as client:
        response = client.get("/api/v1/tasks/overview")
    assert response.status_code == 200, response.text
    data = response.json()["data"]
    assert data["services"]["source_state"] == "unavailable"
    assert data["services"]["items"] == []
    assert data["scheduled"]["source_state"] == "ready"


def test_overview_bounds_long_job_name_without_changing_the_existing_jobs_endpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "serving"
    at = _publish(root, monkeypatch, jobs=True, long_job_name=True)
    with TestClient(_app(root, at + timedelta(seconds=30))) as client:
        overview = client.get("/api/v1/tasks/overview", params={"page_size": 1})
        jobs = client.get("/api/v1/tasks/jobs", params={"page_size": 1})
    assert overview.status_code == jobs.status_code == 200
    assert len(overview.json()["data"]["research"]["items"][0]["strategy_name"]) == 80
    assert len(jobs.json()["data"]["items"][0]["strategy_name"]) == 10_000
    assert len(overview.content) < 32 * 1024


def test_overview_cursor_cannot_join_another_generation_or_the_jobs_endpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "serving"
    at = _publish(root, monkeypatch, jobs=True)
    app = _app(root, at + timedelta(seconds=30))
    with TestClient(app) as client:
        first = client.get("/api/v1/tasks/overview", params={"page_size": 1})
        assert first.status_code == 200, first.text
        cursor = first.json()["data"]["research"]["next_cursor"]
        assert cursor
        second = client.get(
            "/api/v1/tasks/overview", params={"page_size": 1, "cursor": cursor}
        )
        assert second.status_code == 200, second.text
        assert len(second.json()["data"]["research"]["items"]) == 1
        assert second.json()["data"]["research"]["next_cursor"] is None
        assert client.get(
            "/api/v1/tasks/jobs", params={"page_size": 1, "cursor": cursor}
        ).status_code == 409
        assert client.get(
            "/api/v1/tasks/overview", params={"page_size": 2, "cursor": cursor}
        ).status_code == 409
        assert client.get(
            "/api/v1/tasks/overview", params={"page_size": 1, "cursor": f"{cursor}x"}
        ).status_code == 409
        jobs_cursor = client.get(
            "/api/v1/tasks/jobs", params={"page_size": 1}
        ).json()["data"]["next_cursor"]
        assert jobs_cursor
        assert client.get(
            "/api/v1/tasks/overview", params={"page_size": 1, "cursor": jobs_cursor}
        ).status_code == 409
        _publish(root, monkeypatch, sequence=1, jobs=True)
        app.state.web.tracker.refresh()
        changed = client.get(
            "/api/v1/tasks/overview", params={"page_size": 1, "cursor": cursor}
        )
        assert changed.status_code == 409
        assert "重新" in changed.json()["detail"]


@pytest.mark.parametrize(
    "built_at",
    (
        datetime(2026, 9, 25, 1, 0, tzinfo=UTC),
        datetime(2026, 9, 28, 0, 30, tzinfo=UTC),
    ),
)
def test_closed_day_and_preopen_market_timer_wait_without_a_false_alarm(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, built_at: datetime
) -> None:
    root = tmp_path / "serving"
    at = _publish(root, monkeypatch, built_at=built_at)
    with TestClient(_app(root, at + timedelta(seconds=30))) as client:
        response = client.get("/api/v1/tasks/overview")
    assert response.status_code == 200, response.text
    monitor = next(
        item
        for item in response.json()["data"]["scheduled"]["items"]
        if item["timer_unit"] == "rquant-monitor.timer"
    )
    assert monitor["status"]["state"] == "waiting"
    assert monitor["status"]["label"] == "等待开盘"
