"""One published daily-bar report stays truthful across the Serving/Web boundary."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from rquant.data_audit_evidence import DailyBarNullFieldSpec
from rquant.data_audit_report import AuditReportSource, build_data_audit_report
from rquant.data_audit_report_projection import project_data_audit_report
from rquant.serving_read_models import ServingProjectionPayload
from rquant.storage.schema import DAILY_BAR_DDL, TRADE_CALENDAR_DDL
from rquant.web.app import create_app
from rquant.web.settings import WebSettings
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
