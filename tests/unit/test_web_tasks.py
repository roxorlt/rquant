"""The research queue reads only a single published Serving generation."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from uuid import UUID

import duckdb
import pytest

from rquant.lab_eta import LabEtaEstimate, LabEtaFinishWindow
from rquant.lab_jobs import CommandAvailability, JobStatus, LabJobProgress, LabJobSummary
from rquant.research_run_spec import ResearchJobType, ResourceClass
from rquant.serving_contracts import FreshnessStatus
from rquant.serving_read_models import ServingLabJobRecord
from rquant.web.settings import WebSettings
from tests.support.web_proxy_identity import ResearcherTestClient as TestClient
from tests.support.web_proxy_identity import create_private_test_app as create_app
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture


def _job(
    number: int,
    *,
    status: JobStatus,
    updated_at: datetime,
    progress: tuple[int, int] = (0, 4),
    eta_at: datetime | None = None,
) -> ServingLabJobRecord:
    job_id = UUID(int=number)
    summary = LabJobSummary(
        job_id=job_id,
        strategy_name=f"研究任务{number}",
        spec_hash="a" * 64,
        job_type=ResearchJobType.PARAMETER_SEARCH,
        resource_class=ResourceClass.STANDARD,
        status=status,
        control_intent="none",
        result_state="pending",
        version=1,
        deadline=FIXTURE_BUILT_AT + timedelta(hours=2),
        created_at=updated_at - timedelta(minutes=2),
        updated_at=updated_at,
        progress=LabJobProgress(
            total_shards=progress[1],
            terminal_shards=progress[0],
            succeeded_shards=progress[0],
            failed_shards=0,
            cancelled_shards=0,
            fraction=progress[0] / progress[1],
        ),
        command_availability=CommandAvailability(
            pause=False, resume=False, cancel=False, retry=False
        ),
    )
    eta = (
        LabEtaEstimate(
            job_id=job_id,
            status="running",
            as_of=updated_at,
            estimator="static",
            completed_telemetry_shards=progress[0],
            remaining_shards=progress[1] - progress[0],
            remaining_duration=None,
            finish_at=LabEtaFinishWindow(low=eta_at, center=eta_at, high=eta_at),
        )
        if eta_at is not None
        else None
    )
    return ServingLabJobRecord(summary=summary, eta=eta)


def _app(root: Path):
    return create_app(
        WebSettings(serving_root=root),
        clock=lambda: FIXTURE_BUILT_AT + timedelta(seconds=30),
        background=False,
    )


def test_jobs_count_and_page_in_one_generation_with_real_progress_and_eta(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    same_at = FIXTURE_BUILT_AT - timedelta(minutes=1)
    eta_at = FIXTURE_BUILT_AT + timedelta(minutes=4)
    build_web_fixture(
        root,
        "baseline",
        lab_jobs=(
            _job(2, status=JobStatus.QUEUED, updated_at=same_at),
            _job(3, status=JobStatus.FAILED, updated_at=same_at - timedelta(minutes=1)),
            _job(1, status=JobStatus.RUNNING, updated_at=same_at, progress=(1, 4), eta_at=eta_at),
        ),
    )
    with TestClient(_app(root)) as client:
        seen: list[str] = []
        cursor: str | None = None
        for _ in range(3):
            response = client.get(
                "/api/v1/tasks/jobs",
                params={"page_size": 1, **({"cursor": cursor} if cursor else {})},
            )
            assert response.status_code == 200, response.text
            body = response.json()
            data = body["data"]
            assert data["source_state"] == "ready"
            assert data["total"] == 3
            assert data["counts"] == {
                "queued": 1,
                "running": 1,
                "checkpointed": 0,
                "succeeded": 0,
                "failed": 1,
                "cancelled": 0,
                "other": 0,
            }
            assert body["serving"]["generation_id"] == response.headers["X-Rquant-Generation"]
            item = data["items"][0]
            seen.append(item["job_id"])
            assert "spec_hash" not in item
            assert "command_availability" not in item
            if len(seen) == 1:
                assert item["status"] == {
                    "state": "ok",
                    "label": "运行中",
                    "reason": "任务正在运行",
                }
                assert item["progress_fraction"] == 0.25
                assert (item["terminal_shards"], item["total_shards"]) == (1, 4)
                assert item["eta_at"] == eta_at.isoformat().replace("+00:00", "Z")
            cursor = data["next_cursor"]
        assert seen == [str(UUID(int=index)) for index in (1, 2, 3)]
        assert cursor is None


def test_unpublished_empty_and_missing_generation_are_distinct(tmp_path: Path) -> None:
    empty = tmp_path / "empty"
    unpublished = tmp_path / "unpublished"
    build_web_fixture(empty, "baseline")
    build_web_fixture(
        unpublished,
        "degraded",
        lab_jobs=(
            _job(9, status=JobStatus.RUNNING, updated_at=FIXTURE_BUILT_AT - timedelta(minutes=1)),
        ),
    )
    for root, state, total in (
        (empty, "empty", 0),
        (unpublished, "not_published", None),
        (tmp_path / "missing", "unavailable", None),
    ):
        with TestClient(_app(root)) as client:
            response = client.get("/api/v1/tasks/jobs")
            assert response.status_code == 200
            data = response.json()["data"]
            assert data["source_state"] == state
            assert data["total"] == total
            assert data["items"] == []


def test_job_cursor_rejects_tampering_page_size_and_generation_switch(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    jobs = (
        _job(1, status=JobStatus.RUNNING, updated_at=FIXTURE_BUILT_AT - timedelta(minutes=1)),
        _job(2, status=JobStatus.QUEUED, updated_at=FIXTURE_BUILT_AT - timedelta(minutes=2)),
    )
    build_web_fixture(root, "baseline", lab_jobs=jobs)
    app = _app(root)
    with TestClient(app) as client:
        first = client.get("/api/v1/tasks/jobs", params={"page_size": 1})
        cursor = first.json()["data"]["next_cursor"]
        assert cursor
        for params in (
            {"page_size": 2, "cursor": cursor},
            {"page_size": 1, "cursor": f"{cursor}x"},
        ):
            response = client.get("/api/v1/tasks/jobs", params=params)
            assert response.status_code == 409
            assert "重新" in response.json()["detail"]
        build_web_fixture(root, "baseline", sequence=1, lab_jobs=jobs)
        app.state.web.tracker.refresh()
        changed = client.get("/api/v1/tasks/jobs", params={"page_size": 1, "cursor": cursor})
        assert changed.status_code == 409
        assert "重新" in changed.json()["detail"]


def test_stale_job_watermark_is_visible_without_exposing_raw_reason(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tests.support import web_serving_fixture as fixture

    original = fixture._watermarks

    def stale(*args: object, **kwargs: object):
        marks = original(*args, **kwargs)
        return tuple(
            mark.model_copy(update={"status": FreshnessStatus.STALE, "reason": "internal-secret"})
            if mark.dataset_id == "lab_jobs"
            else mark
            for mark in marks
        )

    monkeypatch.setattr(fixture, "_watermarks", stale)
    root = tmp_path / "stale"
    build_web_fixture(
        root,
        "baseline",
        lab_jobs=(
            _job(1, status=JobStatus.RUNNING, updated_at=FIXTURE_BUILT_AT - timedelta(minutes=1)),
        ),
    )
    with TestClient(_app(root)) as client:
        response = client.get("/api/v1/tasks/jobs")
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["source_state"] == "ready"
        assert "延迟" in data["source_note"]
        assert "internal-secret" not in response.text


def test_unreadable_published_table_is_an_error_not_an_empty_queue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline")
    app = _app(root)

    class BrokenCursor:
        def execute(self, *_args: object, **_kwargs: object) -> None:
            raise duckdb.CatalogException("internal-table-name")

    with TestClient(app) as client:
        original_borrow = app.state.web.tracker.borrow

        @contextmanager
        def broken_borrow():
            with original_borrow() as borrowed:
                assert borrowed is not None
                yield replace(borrowed, cursor=BrokenCursor())

        monkeypatch.setattr(app.state.web.tracker, "borrow", broken_borrow)
        response = client.get("/api/v1/tasks/jobs")
        assert response.status_code == 503
        assert response.json()["detail"] == "研究任务数据暂时无法读取，请稍后重试。"
        assert "internal-table-name" not in response.text
