"""Audit API evidence crosses research metadata, Serving, and the web boundary."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from fastapi.testclient import TestClient

from rquant.data_metadata import DataAuditRun, DataAuditRunFinalization, DataQualityIssue
from rquant.serving_page_projection_source import DuckDBLabPageProjectionSource
from rquant.storage.duckdb import DuckDBStore
from rquant.web.app import create_app
from rquant.web.settings import WebSettings
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture


def _app(root: Path):
    return create_app(
        WebSettings(serving_root=root),
        clock=lambda: FIXTURE_BUILT_AT + timedelta(seconds=20),
        background=False,
    )


def test_audit_endpoints_keep_failed_attempt_separate_from_last_success(tmp_path: Path) -> None:
    research = tmp_path / "research.duckdb"
    started = FIXTURE_BUILT_AT - timedelta(minutes=10)
    issue = DataQualityIssue.detected(
        rule_id="minute-without-daily",
        dataset_id="minute_bar",
        severity="P1",
        scope_key="/private/source/secret",
        message="internal service code svc-secret at /private/source",
        evidence={"path": "/private/source"},
        observed_at=started,
    )
    with DuckDBStore(research) as store:
        store.record_data_quality_issue(issue)
        successful = store.begin_data_audit_run(
            DataAuditRun.create(
                as_of_date=date(2026, 9, 23),
                range_start=date(2026, 9, 1),
                range_end=date(2026, 9, 23),
                rule_set_version="stage1-v3",
                observed_at=started,
            )
        )
        store.finalize_data_audit_run(
            successful.audit_run_id,
            DataAuditRunFinalization(
                finding_issue_ids=(issue.issue_id,),
                p0_count=0,
                completed_at=started + timedelta(minutes=1),
            ),
        )
        failed = store.begin_data_audit_run(
            DataAuditRun.create(
                as_of_date=date(2026, 9, 24),
                range_start=date(2026, 9, 1),
                range_end=date(2026, 9, 24),
                rule_set_version="stage1-v3",
                observed_at=started + timedelta(minutes=2),
            )
        )
        store.fail_data_audit_run(
            failed.audit_run_id,
            error_message="secret failed path /private/source",
            completed_at=started + timedelta(minutes=3),
        )
    projection = DuckDBLabPageProjectionSource(research)(FIXTURE_BUILT_AT).projections
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline", lab_page_projections=projection)

    with TestClient(_app(root)) as client:
        health = client.get("/api/v1/data/health")
        assert health.status_code == 200
        assert health.json()["data"]["source_state"] == "ready"
        assert health.json()["data"]["latest_attempt"]["status"] == "failed"
        assert health.json()["data"]["latest_success"]["finding_count"] == 1
        generation = health.json()["serving"]["generation_id"]
        issues = client.get(
            "/api/v1/data/issues", params={"dataset": "minute_bar", "generation": generation}
        )
        assert issues.status_code == 200
        assert issues.json()["data"]["total_count"] == 1
        assert issues.json()["data"]["issues"][0]["name"] == "分钟线缺少日线"
        assert issues.json()["data"]["issues"][0]["status"] == "待处理"
        assert issues.json()["data"]["partial"] is False
        assert client.get("/api/v1/data/issues", params={"dataset": "unknown"}).status_code == 404
        assert (
            client.get(
                "/api/v1/data/issues", params={"dataset": "minute_bar", "generation": "old"}
            ).status_code
            == 409
        )

    payload = health.text + issues.text
    for secret in ("/private/source", "svc-secret", issue.issue_id, successful.audit_run_id):
        assert secret not in payload


def test_unpublished_audit_is_not_reported_as_healthy(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline")
    with TestClient(_app(root)) as client:
        health = client.get("/api/v1/data/health")
        issues = client.get("/api/v1/data/issues?dataset=daily_bar")
    assert health.status_code == issues.status_code == 200
    assert health.json()["data"]["source_state"] == "not_published"
    assert health.json()["data"]["latest_attempt"] is None
    assert issues.json()["data"]["issues"] == []


def test_global_finding_count_matches_selectable_dataset_issues(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline", audit=True)
    with TestClient(_app(root)) as client:
        health = client.get("/api/v1/data/health")
        minute = client.get("/api/v1/data/issues", params={"dataset": "minute_bar"})
        limit_up = client.get("/api/v1/data/issues", params={"dataset": "limit_up_pool_daily"})

    assert health.status_code == minute.status_code == limit_up.status_code == 200
    assert health.json()["data"]["latest_success"]["finding_count"] == 2
    counts = [minute.json()["data"]["total_count"], limit_up.json()["data"]["total_count"]]
    assert counts == [1, 1]
    assert sum(counts) == health.json()["data"]["latest_success"]["finding_count"]
    assert limit_up.json()["data"]["issues"][0]["name"] == "涨停池日期异常"
