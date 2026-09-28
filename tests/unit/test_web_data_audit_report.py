"""One published daily-bar report stays truthful across the Serving/Web boundary."""

from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import duckdb
import pytest
from fastapi import FastAPI, HTTPException

from rquant.data_audit_evidence import DailyBarNullFieldSpec
from rquant.data_audit_report import AuditReportSource, build_data_audit_report
from rquant.data_audit_report_job_projection import DataAuditReportJobProgress
from rquant.data_audit_report_jobs import DataAuditReportJobEvent
from rquant.data_audit_report_projection import project_data_audit_report
from rquant.serving_read_models import ServingProjectionPayload
from rquant.storage.schema import DAILY_BAR_DDL, TRADE_CALENDAR_DDL
from rquant.web.routes.data_audit_report import _snapshot
from rquant.web.serving import GenerationTracker
from rquant.web.settings import WebSettings
from tests.support.web_proxy_identity import ResearcherTestClient as TestClient
from tests.support.web_proxy_identity import create_private_test_app as create_app
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture


def _report_projections(tmp_path: Path) -> tuple[ServingProjectionPayload, ...]:
    database = tmp_path / "daily.duckdb"
    start = date(2026, 9, 22)
    with duckdb.connect(str(database)) as connection:
        connection.execute(TRADE_CALENDAR_DDL)
        connection.execute(DAILY_BAR_DDL)
        connection.executemany(
            "INSERT INTO trade_calendar (exchange, cal_date, is_open, source, updated_at) "
            "VALUES ('SSE', ?, TRUE, 'synthetic', ?)",
            [
                (start + timedelta(days=offset), FIXTURE_BUILT_AT - timedelta(minutes=30))
                for offset in range(3)
            ],
        )
        connection.executemany(
            "INSERT INTO daily_bar (ts_code, trade_date, close, vol) VALUES (?, ?, ?, ?)",
            [
                ("600000.SH", start, None, 100.0),
                ("600000.SH", start + timedelta(days=2), 10.0, 100.0),
            ],
        )
    with duckdb.connect(str(database), read_only=True) as connection:
        report = build_data_audit_report(
            connection,
            source=AuditReportSource(
                mode="production_unverified",
                namespace="production",
                snapshot_label="synthetic-fixture",
            ),
            audit_start=start,
            observed_through=start + timedelta(days=2),
            null_fields=(
                DailyBarNullFieldSpec(
                    field_name="close", max_null_numerator=0, max_null_denominator=1
                ),
            ),
        )
    return project_data_audit_report(report, available_at=FIXTURE_BUILT_AT - timedelta(minutes=5))


def _large_report_projections(tmp_path: Path) -> tuple[ServingProjectionPayload, ...]:
    database = tmp_path / "many-daily.duckdb"
    start = date(2025, 9, 24)
    end = date(2026, 9, 24)
    days = [start + timedelta(days=offset) for offset in range((end - start).days + 1)]
    with duckdb.connect(str(database)) as connection:
        connection.execute(TRADE_CALENDAR_DDL)
        connection.execute(DAILY_BAR_DDL)
        connection.executemany(
            "INSERT INTO trade_calendar (exchange, cal_date, is_open, source, updated_at) "
            "VALUES ('SSE', ?, ?, 'synthetic', ?)",
            [(day, day.weekday() < 5, FIXTURE_BUILT_AT - timedelta(minutes=30)) for day in days],
        )
        connection.executemany(
            "INSERT INTO daily_bar (ts_code, trade_date, close, vol) VALUES (?, ?, ?, ?)",
            [("600000.SH", day, None, 100.0) for day in days if day.weekday() < 5],
        )
    with duckdb.connect(str(database), read_only=True) as connection:
        report = build_data_audit_report(
            connection,
            source=AuditReportSource(
                mode="production_unverified",
                namespace="production",
                snapshot_label="many-issues-fixture",
            ),
            audit_start=start,
            observed_through=end,
            null_fields=(
                DailyBarNullFieldSpec(
                    field_name="close", max_null_numerator=0, max_null_denominator=1
                ),
            ),
        )
    assert len(report.issues) > 256
    return project_data_audit_report(report, available_at=FIXTURE_BUILT_AT - timedelta(minutes=5))


def _app(root: Path, *, now: datetime | None = None) -> FastAPI:
    return create_app(
        WebSettings(serving_root=root),
        clock=lambda: now or FIXTURE_BUILT_AT + timedelta(seconds=20),
        background=False,
    )


def _change(
    projections: tuple[ServingProjectionPayload, ...], table: str, **changes: Any
) -> tuple[ServingProjectionPayload, ...]:
    return tuple(
        ServingProjectionPayload(
            table_name=item.table_name,
            available_at=item.available_at,
            rows=({**dict(item.rows[0]), **changes}, *item.rows[1:]),
        )
        if item.table_name == table
        else item
        for item in projections
    )


def _job_projections(
    *,
    status: str | None = None,
    report_hash: str | None = None,
    successful_task_id: str | None = None,
    availability: str | None = None,
    error_code: str | None = None,
    available_at: datetime | None = None,
    extra_events: int = 0,
) -> tuple[ServingProjectionPayload, ...]:
    published_at = available_at or FIXTURE_BUILT_AT - timedelta(minutes=5)
    task_id = "a" * 32
    created_at = FIXTURE_BUILT_AT - timedelta(minutes=10)
    updated_at = FIXTURE_BUILT_AT - timedelta(minutes=8)
    success_offset = (
        timedelta(hours=1)
        if successful_task_id is not None and successful_task_id != task_id
        else timedelta(0)
    )
    progress = DataAuditReportJobProgress(
        availability=availability or ("ready" if status else "empty"),
        latest_task_id=task_id if status else None,
        latest_status=status,
        latest_attempts=(0 if status == "queued" else 1) if status else None,
        latest_created_at=created_at if status else None,
        latest_updated_at=updated_at if status else None,
        latest_error_code=error_code,
        successful_task_id=successful_task_id,
        successful_report_hash=report_hash,
        successful_created_at=created_at - success_offset if successful_task_id else None,
        successful_updated_at=updated_at - success_offset if successful_task_id else None,
    )
    events: list[dict[str, object]] = []
    if status:
        terminal = {
            "queued": "queued",
            "running": "started",
            "succeeded": "succeeded",
            "failed": "failed",
        }[status]
        kinds = ["queued"] + ["started"] * extra_events + [terminal]
        if status == "queued":
            kinds = ["queued"]
        events = [
            DataAuditReportJobEvent(
                event_id=index + 1,
                task_id=task_id,
                event_type=kind,
                attempts=0 if kind == "queued" else 1,
                occurred_at=created_at + timedelta(seconds=index),
                error_code=error_code if kind == "failed" else None,
            ).model_dump(mode="json")
            for index, kind in enumerate(kinds)
        ]
    return (
        ServingProjectionPayload(
            table_name="audit_report_job",
            available_at=published_at,
            rows=(progress.model_dump(mode="json"),),
        ),
        ServingProjectionPayload(
            table_name="audit_report_job_event", available_at=published_at, rows=tuple(events)
        ),
    )


def test_published_report_returns_unconfirmed_facts_and_total_not_page_count(
    tmp_path: Path,
) -> None:
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline", audit_report_projections=_report_projections(tmp_path))

    with TestClient(_app(root)) as client:
        response = client.get("/api/v1/data/report")

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["source_state"] == "ready"
    overview = data["overview"]
    assert overview["collection_status"] == "collection_unconfirmed"
    assert overview["current"] is False
    assert overview["coverage_conclusion"] == "unconfirmed"
    assert overview["quality_conclusion"] == "not_fully_assessed"
    assert overview["expected_open_days"] == 3
    assert overview["covered_open_days"] == 2
    assert overview["missing_open_days"] == 1
    assert overview["quality_issue_count"] == 1
    assert overview["indexed_issue_count"] == len(data["issues"]) == 1
    assert overview["omitted_issue_count"] == 0
    assert len(data["months"]) == 1
    assert data["months"][0]["expected_open_days"] == 3
    assert data["months"][0]["covered_open_days"] == 2
    assert len(data["rules"]) == 3
    assert any(
        rule["name"] == "收盘价上下限" and rule["assessed_days"] == 0 for rule in data["rules"]
    )
    assert all(rule["unassessed_reasons"] for rule in data["rules"] if rule["unassessed_days"])
    assert data["issues"][0]["name"] == "字段空值比例"
    assert response.headers["X-Rquant-Generation"] == response.json()["serving"]["generation_id"]


def test_full_issue_total_is_kept_when_only_first_256_are_indexed(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    build_web_fixture(
        root, "baseline", audit_report_projections=_large_report_projections(tmp_path)
    )

    with TestClient(_app(root)) as client:
        response = client.get("/api/v1/data/report")

    assert response.status_code == 200
    data = response.json()["data"]
    overview = data["overview"]
    assert overview["quality_issue_count"] > 256
    assert overview["indexed_issue_count"] == len(data["issues"]) == 256
    assert overview["omitted_issue_count"] == overview["quality_issue_count"] - 256
    assert sum(rule["issue_count"] for rule in data["rules"]) == overview["quality_issue_count"]
    assert [item["number"] for item in data["issues"]] == list(range(1, 257))


def test_unpublished_report_and_generation_change_have_distinct_states(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    first = build_web_fixture(root, "baseline")
    with TestClient(_app(root, now=FIXTURE_BUILT_AT + timedelta(minutes=1, seconds=20))) as client:
        unpublished = client.get("/api/v1/data/report")
        assert unpublished.status_code == 200
        assert unpublished.json()["data"] == {
            "source_state": "not_published",
            "overview": None,
            "months": [],
            "rules": [],
            "issues": [],
            "progress": {
                "availability": "unavailable",
                "latest_task_id": None,
                "latest_status": None,
                "latest_status_label": None,
                "latest_attempts": None,
                "latest_created_at": None,
                "latest_updated_at": None,
                "latest_hint": None,
                "successful_task_id": None,
                "successful_report_hash": None,
                "successful_created_at": None,
                "successful_updated_at": None,
                "events": [],
            },
        }
        build_web_fixture(
            root,
            "baseline",
            sequence=1,
            audit_report_projections=_report_projections(tmp_path),
        )
        client.app.state.web.tracker.refresh()
        changed = client.get("/api/v1/data/report", params={"generation": first.generation_id})
        current = client.get("/api/v1/data/report")

    assert changed.status_code == 409
    assert current.status_code == 200
    assert current.json()["data"]["source_state"] == "ready"
    empty_root = tmp_path / "empty-serving"
    with TestClient(_app(empty_root)) as client:
        unavailable = client.get("/api/v1/data/report")
    assert unavailable.status_code == 200
    assert unavailable.json()["data"]["source_state"] == "unavailable"


def test_empty_queued_and_running_tasks_have_progress_without_report(tmp_path: Path) -> None:
    for status in (None, "queued", "running"):
        root = tmp_path / f"serving-{status}"
        build_web_fixture(
            root, "baseline", audit_report_projections=_job_projections(status=status)
        )
        with TestClient(_app(root)) as client:
            response = client.get("/api/v1/data/report")
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["source_state"] == "not_published"
        assert data["overview"] is None
        progress = data["progress"]
        assert progress["availability"] == ("ready" if status else "empty")
        assert progress["latest_status"] == status
        assert (
            progress["latest_status_label"]
            == {
                None: None,
                "queued": "等待审计",
                "running": "正在审计",
            }[status]
        )
        assert len(progress["events"]) == {None: 0, "queued": 1, "running": 2}[status]
        assert "/" not in str(progress)


def test_latest_failure_preserves_previous_report_but_never_calls_it_current(
    tmp_path: Path,
) -> None:
    report = _report_projections(tmp_path)
    digest = report[0].rows[0]["report_hash"]
    root = tmp_path / "serving"
    build_web_fixture(
        root,
        "baseline",
        audit_report_projections=report
        + _job_projections(
            status="failed",
            report_hash=digest,
            successful_task_id="b" * 32,
            error_code="replica_changed",
        ),
    )
    with TestClient(_app(root)) as client:
        response = client.get("/api/v1/data/report")
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["source_state"] == "ready"
    assert data["overview"]["report_hash"] == digest
    assert data["overview"]["collection_status"] == "collection_unconfirmed"
    assert data["overview"]["current"] is False
    assert data["progress"]["latest_status"] == "failed"
    assert data["progress"]["successful_report_hash"] == digest
    assert data["progress"]["successful_task_id"] != data["progress"]["latest_task_id"]
    assert data["progress"]["successful_updated_at"] != data["progress"]["latest_updated_at"]
    assert data["progress"]["latest_hint"] == "数据副本已更新，请重新发起审计。"
    assert "replica_changed" not in response.text


def test_successful_task_publishes_matching_report_without_claiming_collection_complete(
    tmp_path: Path,
) -> None:
    report = _report_projections(tmp_path)
    digest = report[0].rows[0]["report_hash"]
    root = tmp_path / "serving"
    build_web_fixture(
        root,
        "baseline",
        audit_report_projections=report
        + _job_projections(status="succeeded", report_hash=digest, successful_task_id="a" * 32),
    )
    with TestClient(_app(root)) as client:
        response = client.get("/api/v1/data/report")
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["progress"]["latest_status_label"] == "审计完成"
    assert data["progress"]["successful_report_hash"] == data["overview"]["report_hash"]
    assert data["overview"]["current"] is False
    assert data["overview"]["collection_status"] == "collection_unconfirmed"


@pytest.mark.parametrize(
    "problem",
    ["missing_event_table", "mixed_time", "wrong_hash", "wrong_event_task", "orphan_report"],
)
def test_task_projection_must_match_its_report_and_budget(tmp_path: Path, problem: str) -> None:
    report = _report_projections(tmp_path)
    digest = report[0].rows[0]["report_hash"]
    jobs = _job_projections(
        status="succeeded",
        report_hash=digest,
        successful_task_id="a" * 32,
        available_at=FIXTURE_BUILT_AT - timedelta(minutes=4) if problem == "mixed_time" else None,
    )
    if problem == "missing_event_table":
        jobs = jobs[:1]
    elif problem == "wrong_hash":
        jobs = _change(jobs, "audit_report_job", successful_report_hash="f" * 64)
    elif problem == "wrong_event_task":
        jobs = _change(jobs, "audit_report_job_event", task_id="f" * 32)
    elif problem == "orphan_report":
        jobs = _job_projections(status="running")
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline", audit_report_projections=report + jobs)
    with TestClient(_app(root)) as client:
        response = client.get("/api/v1/data/report")
    assert response.status_code == 503
    assert "replica_changed" not in response.text


def test_serving_projection_rejects_more_than_twenty_audit_events() -> None:
    with pytest.raises(ValueError, match="row budget"):
        _job_projections(status="running", extra_events=19)


def test_unpublished_task_status_with_foreign_owner_fails_closed(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline")
    tracker = GenerationTracker(root)
    try:
        with tracker.borrow() as borrowed:
            assert borrowed is not None
            job_rows = borrowed.cursor.execute(
                "SELECT table_name, available, row_count, owner_dataset_id, "
                "owner_generation_id, available_at FROM projection_status "
                "WHERE table_name IN (?, ?) ORDER BY table_name",
                ("audit_report_job", "audit_report_job_event"),
            ).fetchall()
            assert len(job_rows) == 2 and all(not row[1] for row in job_rows)
            changed = [
                (name, available, count, "signals", owner_generation, at)
                for name, available, count, _, owner_generation, at in job_rows
            ]

            def execute(query: str, params: tuple[object, ...]) -> Any:
                if len(params) == 3 and "FROM projection_status" in query:
                    return SimpleNamespace(fetchall=lambda: changed)
                return borrowed.cursor.execute(query, params)

            tampered = replace(borrowed, cursor=SimpleNamespace(execute=execute))
            with pytest.raises(HTTPException, match="审计报告暂时无法读取"):
                _snapshot(tampered)
    finally:
        tracker.close()


def test_failed_task_hides_raw_error_details_from_response(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    build_web_fixture(
        root,
        "baseline",
        audit_report_projections=_job_projections(status="failed", error_code="internal_error"),
    )
    with TestClient(_app(root)) as client:
        response = client.get("/api/v1/data/report")
    assert response.status_code == 200
    assert response.json()["data"]["progress"]["latest_hint"] == "审计未完成，请稍后重试。"
    assert "internal_error" not in response.text


def test_partial_report_projection_fails_closed(tmp_path: Path) -> None:
    projections = tuple(
        item for item in _report_projections(tmp_path) if item.table_name != "audit_report_rule"
    )
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline", audit_report_projections=projections)

    with TestClient(_app(root)) as client:
        response = client.get("/api/v1/data/report")

    assert response.status_code == 503
    assert "hash" not in response.text


def test_missing_daily_bar_cannot_be_recast_as_fully_checked_and_healthy(tmp_path: Path) -> None:
    projections: list[ServingProjectionPayload] = []
    for item in _report_projections(tmp_path):
        rows = item.rows
        if item.table_name == "audit_report_overview":
            rows = (
                {
                    **dict(rows[0]),
                    "quality_conclusion": "no_issues_observed",
                    "quality_issue_count": 0,
                    "indexed_issue_count": 0,
                    "omitted_issue_count": 0,
                    "unassessed_rule_days": 0,
                },
            )
        elif item.table_name == "audit_report_rule":
            rows = tuple(
                {
                    **dict(row),
                    "checked_days": 3,
                    "assessed_days": 3,
                    "unassessed_days": 0,
                    "first_assessed_date": "2026-09-22",
                    "last_assessed_date": "2026-09-24",
                    "assessment_complete": True,
                    "unassessed_reasons_json": "{}",
                    "issue_count": 0,
                }
                for row in rows
            )
        elif item.table_name == "audit_report_issue":
            rows = ()
        projections.append(
            ServingProjectionPayload(
                table_name=item.table_name, available_at=item.available_at, rows=rows
            )
        )
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline", audit_report_projections=tuple(projections))

    with TestClient(_app(root)) as client:
        response = client.get("/api/v1/data/report")

    assert response.status_code == 503
    assert "hash" not in response.text


@pytest.mark.parametrize(
    ("table", "change"),
    [
        ("audit_report_overview", {"missing_open_days": 0}),
        ("audit_report_overview", {"gap_count": 0}),
        ("audit_report_overview", {"current": True}),
        ("audit_report_overview", {"source_mode": "synthetic_test"}),
        ("audit_report_overview", {"indexed_issue_count": 0}),
        ("audit_report_month", {"report_hash": "f" * 64}),
        ("audit_report_month", {"covered_open_days": 3}),
        ("audit_report_rule", {"unassessed_reasons_json": "{}"}),
        (
            "audit_report_rule",
            {
                "unassessed_reasons_json": (
                    '{"close_missing":1,"limits_unavailable":1,"no_observations":1}'
                )
            },
        ),
        ("audit_report_rule", {"first_assessed_date": "2026-09-22"}),
        ("audit_report_issue", {"issue_index": 3}),
    ],
)
def test_bad_report_summary_or_row_never_looks_healthy(
    tmp_path: Path, table: str, change: dict[str, object]
) -> None:
    projections = _change(_report_projections(tmp_path), table, **change)
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline", audit_report_projections=projections)

    with TestClient(_app(root)) as client:
        response = client.get("/api/v1/data/report")

    assert response.status_code == 503
    assert "hash" not in response.text
