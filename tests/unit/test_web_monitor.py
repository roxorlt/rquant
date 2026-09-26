"""Read-only recent signals and their receipts from one Serving generation."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import duckdb
import pytest
from fastapi.testclient import TestClient

from rquant.web.app import create_app
from rquant.web.settings import WebSettings
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture


@pytest.fixture
def serving_root(tmp_path: Path) -> Path:
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline")
    return root


def _app(root: Path):
    return create_app(
        WebSettings(serving_root=root),
        clock=lambda: FIXTURE_BUILT_AT + timedelta(seconds=30),
        background=False,
    )


def test_recent_signals_page_has_real_receipts_and_signed_next_cursor(serving_root: Path) -> None:
    with TestClient(_app(serving_root)) as client:
        first = client.get("/api/v1/monitor/signals", params={"page_size": 1})
        assert first.status_code == 200, first.text
        data = first.json()["data"]
        assert data["source_state"] == "ready"
        assert data["receipt_state"] == "has_receipts"
        assert data["total"] == 2
        assert data["mode"] == "shadow"
        assert len(data["items"]) == 1
        assert data["items"][0]["sequence"] == 1
        assert data["items"][0]["strategy_name"] == "N 字"
        assert data["items"][0]["action_label"] == "买入意向"
        assert data["items"][0]["delivery_label"] == "送达未确认"
        assert data["items"][0]["receipts"][0]["status_label"] == "送达未确认"
        assert "当时" in data["items"][0]["delivery_note"]
        assert data["next_cursor"]

        second = client.get(
            "/api/v1/monitor/signals",
            params={"page_size": 1, "cursor": data["next_cursor"]},
        )
        assert second.status_code == 200, second.text
        older = second.json()["data"]
        assert [item["sequence"] for item in older["items"]] == [2]
        assert older["items"][0]["delivery_label"] == "发送中"
        assert older["items"][0]["receipts"][0]["status_label"] == "待发送"
        assert older["next_cursor"] is None


def test_cursor_rejects_generation_change_and_tampering(serving_root: Path) -> None:
    app = _app(serving_root)
    with TestClient(app) as client:
        first = client.get("/api/v1/monitor/signals", params={"page_size": 1})
        cursor = first.json()["data"]["next_cursor"]
        assert cursor
        altered = f"{cursor[:-1]}{'a' if cursor[-1] != 'a' else 'b'}"
        tampered = client.get("/api/v1/monitor/signals", params={"cursor": altered, "page_size": 1})
        assert tampered.status_code == 409
        resized = client.get("/api/v1/monitor/signals", params={"cursor": cursor, "page_size": 2})
        assert resized.status_code == 409
        build_web_fixture(serving_root, "baseline", sequence=1)
        app.state.web.tracker.refresh()
        changed = client.get("/api/v1/monitor/signals", params={"cursor": cursor, "page_size": 1})
        assert changed.status_code == 409
        assert "数据已更新" in changed.json()["detail"]


def test_no_serving_has_separate_source_state(tmp_path: Path) -> None:
    with TestClient(_app(tmp_path / "missing")) as client:
        response = client.get("/api/v1/monitor/signals")
        assert response.status_code == 200
        assert response.json()["data"]["source_state"] == "unavailable"
        assert response.json()["data"]["total"] is None
        assert response.json()["data"]["items"] == []


def test_unpublished_signal_source_is_distinct_from_an_empty_page(
    serving_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.web.routes import monitor

    monkeypatch.setattr(monitor, "_source_published", lambda _borrowed: False)
    with TestClient(_app(serving_root)) as client:
        response = client.get("/api/v1/monitor/signals")
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["source_state"] == "not_published"
        assert data["receipt_state"] == "not_published"
        assert data["total"] is None


def test_page_reads_beyond_the_overview_500_row_limit_and_names_missing_receipts() -> None:
    from rquant.web.routes.monitor import _decode_cursor, _page

    connection = duckdb.connect()
    connection.execute(
        "CREATE TABLE signals AS SELECT i::BIGINT AS global_sequence, "
        "'signal-' || i AS signal_id, 'n_shape' AS strategy_id, 'v1' AS strategy_version, "
        "'600001.SH' AS candidate_id, 'watch' AS action, "
        "'2026-09-24 01:47:00+00'::TIMESTAMPTZ AS available_at, "
        "NULL::TIMESTAMPTZ AS expires_at, '[]' AS reason_codes_json "
        "FROM range(1, 502) t(i)"
    )
    connection.execute(
        "CREATE TABLE deliveries (outbox_id VARCHAR, signal_id VARCHAR, recipient_id VARCHAR, "
        "channel VARCHAR, status VARCHAR, attempt_count INTEGER, updated_at TIMESTAMPTZ, "
        "last_error VARCHAR)"
    )
    connection.execute(
        "CREATE TABLE runtime_services (service_id VARCHAR, status VARCHAR, stale BOOLEAN, "
        "consecutive_failures INTEGER, last_error VARCHAR)"
    )
    connection.execute(
        "CREATE TABLE projection_status (table_name VARCHAR, available BOOLEAN, "
        "row_count INTEGER, available_at TIMESTAMPTZ)"
    )
    borrowed = SimpleNamespace(
        cursor=connection.cursor(), manifest=SimpleNamespace(generation_id="generation-a")
    )
    key = b"a" * 32
    try:
        seen: list[int] = []
        after = None
        while True:
            page = _page(borrowed, page_size=50, after=after, key=key, now=FIXTURE_BUILT_AT)
            seen.extend(item.sequence for item in page.items)
            assert page.receipt_state == "no_receipts"
            assert all(item.delivery_label == "暂无回执" for item in page.items)
            if page.next_cursor is None:
                break
            decoded = _decode_cursor(page.next_cursor, key)
            after = (decoded.last_available_at, decoded.last_sequence)
        assert len(seen) == 501
        assert seen == list(range(501, 0, -1))

        connection.execute(
            "INSERT INTO deliveries VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "outbox-501",
                "signal-501",
                "admin",
                "pushdeer",
                "dead_letter",
                1,
                FIXTURE_BUILT_AT,
                "https://push.example/send?pushkey=secret-token-123",
            ),
        )
        page_with_error = _page(borrowed, page_size=1, after=None, key=key, now=FIXTURE_BUILT_AT)
        assert page_with_error.items[0].delivery_label == "失败"
        assert "secret-token-123" not in page_with_error.model_dump_json()
        assert "pushkey" not in page_with_error.model_dump_json()

        connection.execute("DELETE FROM signals")
        empty = _page(borrowed, page_size=20, after=None, key=key, now=FIXTURE_BUILT_AT)
        assert empty.source_state == "empty"
        assert empty.total == 0
        assert empty.receipt_label == "尚无通知回执"

        shanghai = ZoneInfo("Asia/Shanghai")
        ten = datetime(2026, 9, 24, 10, tzinfo=shanghai)
        nine_thirty = datetime(2026, 9, 24, 9, 30, tzinfo=shanghai)
        connection.executemany(
            "INSERT INTO signals VALUES "
            "(?, ?, 'n_shape', 'v1', '600001.SH', 'watch', ?, NULL, '[]')",
            [
                (1, "late-1", ten),
                (2, "late-2", nine_thirty),
                (3, "late-3", ten),
                (4, "late-4", nine_thirty),
            ],
        )
        seen_late: list[int] = []
        after = None
        while True:
            page = _page(borrowed, page_size=2, after=after, key=key, now=FIXTURE_BUILT_AT)
            seen_late.extend(item.sequence for item in page.items)
            if page.next_cursor is None:
                break
            decoded = _decode_cursor(page.next_cursor, key)
            assert decoded.last_available_at.astimezone(UTC) == page.items[-1].at
            after = (decoded.last_available_at, decoded.last_sequence)
        assert seen_late == [3, 1, 4, 2]
    finally:
        borrowed.cursor.close()
        connection.close()
