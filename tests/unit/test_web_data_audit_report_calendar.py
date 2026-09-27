"""Audit date choices come only from one verified Serving calendar generation."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest
from fastapi.testclient import TestClient

from rquant.web.app import create_app
from rquant.web.routes.data_audit_report_calendar import _snapshot
from rquant.web.serving import BorrowedGeneration
from rquant.web.settings import WebSettings
from tests.support.web_serving_fixture import build_web_fixture


def _clock(day: int, hour: int, minute: int = 0) -> datetime:
    """September 2026 Shanghai wall clock."""

    return datetime(2026, 9, day, hour - 8, minute, tzinfo=UTC)


def test_calendar_only_offers_closed_sse_sessions_across_close_and_holiday(
    tmp_path: Path,
) -> None:
    root = tmp_path / "serving"
    manifest = build_web_fixture(root, "baseline")
    current = [_clock(28, 14, 59)]
    app = create_app(WebSettings(serving_root=root), clock=lambda: current[0], background=False)

    with TestClient(app) as client:
        before = client.get("/api/v1/data/audit-report/calendar")
        current[0] = _clock(28, 15)
        after = client.get("/api/v1/data/audit-report/calendar")
        current[0] = _clock(25, 10)  # Friday Mid-Autumn closure
        holiday = client.get("/api/v1/data/audit-report/calendar")
        current[0] = _clock(27, 10)  # Sunday
        weekend = client.get("/api/v1/data/audit-report/calendar")

    for response in (before, after, holiday, weekend):
        assert response.status_code == 200
        assert response.headers["x-rquant-generation"] == manifest.generation_id
        assert response.headers["cache-control"] == "no-store"
        data = response.json()["data"]
        assert data["availability"] == "ready"
        assert data["earliest_selectable_date"] == "2026-01-05"
        assert data["open_dates"] == sorted(set(data["open_dates"]))
        assert "2026-09-25" not in data["open_dates"]

    for response in (before, holiday, weekend):
        assert response.json()["data"]["latest_closed_date"] == "2026-09-24"
        assert response.json()["data"]["open_dates"][-1] == "2026-09-24"
        assert "2026-09-28" not in response.json()["data"]["open_dates"]
    assert after.json()["data"]["latest_closed_date"] == "2026-09-28"
    assert after.json()["data"]["open_dates"][-1] == "2026-09-28"


def test_calendar_outside_coverage_or_missing_generation_offers_no_dates(
    tmp_path: Path,
) -> None:
    root = tmp_path / "serving"
    with TestClient(
        create_app(WebSettings(serving_root=root), clock=lambda: _clock(24, 16), background=False)
    ) as client:
        missing = client.get("/api/v1/data/audit-report/calendar")
    assert missing.status_code == 200
    assert missing.json()["data"] == {
        "availability": "unavailable",
        "latest_closed_date": None,
        "earliest_selectable_date": None,
        "open_dates": [],
    }

    build_web_fixture(root, "baseline")
    with TestClient(
        create_app(
            WebSettings(serving_root=root),
            clock=lambda: datetime(2027, 1, 4, 2, tzinfo=UTC),
            background=False,
        )
    ) as client:
        uncovered = client.get("/api/v1/data/audit-report/calendar")
    assert uncovered.status_code == 200
    assert uncovered.json()["data"] == missing.json()["data"]


def test_calendar_pinned_generation_rejects_changed_generation(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    old = build_web_fixture(root, "baseline")
    app = create_app(
        WebSettings(serving_root=root, pointer_check_seconds=0.001),
        clock=lambda: _clock(24, 16),
        background=False,
    )
    with TestClient(app) as client:
        first = client.get(
            "/api/v1/data/audit-report/calendar", params={"generation": old.generation_id}
        )
        newer = build_web_fixture(root, "baseline", sequence=1)
        app.state.web.tracker.refresh()
        changed = client.get(
            "/api/v1/data/audit-report/calendar", params={"generation": old.generation_id}
        )
        latest = client.get(
            "/api/v1/data/audit-report/calendar", params={"generation": newer.generation_id}
        )
    assert first.status_code == 200
    assert changed.status_code == 409
    assert latest.status_code == 200
    assert latest.json()["serving"]["generation_id"] == newer.generation_id


def _borrowed_calendar(
    connection: duckdb.DuckDBPyConnection,
    *,
    rows: list[tuple[date, bool, str]],
    owner: str = "reference_slow_authority",
    published: bool = True,
    status_count: int | None = None,
) -> BorrowedGeneration:
    connection.execute(
        "CREATE TABLE projection_status (table_name VARCHAR, available BOOLEAN, "
        "row_count INTEGER, owner_dataset_id VARCHAR, owner_generation_id VARCHAR, "
        "available_at TIMESTAMPTZ)"
    )
    connection.execute(
        "CREATE TABLE trade_calendar (trade_date DATE, is_open BOOLEAN, exchange VARCHAR)"
    )
    if rows:
        connection.executemany("INSERT INTO trade_calendar VALUES (?, ?, ?)", rows)
    if published:
        connection.execute(
            "INSERT INTO projection_status VALUES ('trade_calendar', TRUE, ?, ?, 'owner-1', ?)",
            [status_count if status_count is not None else len(rows), owner, _clock(24, 15)],
        )
    manifest = SimpleNamespace(
        row_counts={"trade_calendar": len(rows)},
        watermarks=[
            SimpleNamespace(dataset_id="reference_slow_authority", generation_id="owner-1")
        ],
        built_at=_clock(24, 16),
    )
    return BorrowedGeneration(
        manifest=manifest, pointer=None, cursor=connection, fallback_detail=None
    )


def test_legacy_generation_without_a_calendar_projection_has_no_choices() -> None:
    with duckdb.connect(":memory:") as connection:
        borrowed = _borrowed_calendar(connection, rows=[], published=False)
        result = _snapshot(borrowed, now=_clock(28, 15))
    assert result.availability == "unavailable"
    assert result.latest_closed_date is None
    assert result.earliest_selectable_date is None
    assert result.open_dates == []


@pytest.mark.parametrize(
    "damage",
    ("missing_status", "wrong_owner", "foreign_exchange", "closed_row", "count_mismatch"),
)
def test_corrupt_calendar_never_offers_dates(damage: str) -> None:
    days = [
        (date(2026, 9, 23), True, "SSE"),
        (date(2026, 9, 24), True, "SSE"),
        (date(2026, 9, 28), True, "SSE"),
        (date(2026, 9, 29), True, "SSE"),
    ]
    if damage == "foreign_exchange":
        days[1] = (date(2026, 9, 24), True, "SZSE")
    if damage == "closed_row":
        days[1] = (date(2026, 9, 24), False, "SSE")
    with duckdb.connect(":memory:") as connection:
        borrowed = _borrowed_calendar(
            connection,
            rows=days,
            owner="wrong" if damage == "wrong_owner" else "reference_slow_authority",
            published=damage != "missing_status",
            status_count=3 if damage == "count_mismatch" else None,
        )
        with pytest.raises(ValueError):
            _snapshot(borrowed, now=_clock(28, 15))


def test_calendar_limits_choices_to_one_valid_audit_range() -> None:
    start = date(2014, 1, 6)
    rows = [(start + timedelta(days=7 * index), True, "SSE") for index in range(680)]
    today = date(2026, 9, 28)
    with duckdb.connect(":memory:") as connection:
        borrowed = _borrowed_calendar(connection, rows=rows)
        result = _snapshot(borrowed, now=_clock(28, 15))
    assert result.availability == "ready"
    assert result.earliest_selectable_date is not None
    assert result.earliest_selectable_date >= today - timedelta(days=3659)
    assert result.latest_closed_date is not None
    assert (result.latest_closed_date - result.earliest_selectable_date).days + 1 <= 3660
    assert result.open_dates == sorted(set(result.open_dates))
