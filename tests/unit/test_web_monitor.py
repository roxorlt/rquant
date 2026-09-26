"""Read-only recent signals and their receipts from one Serving generation."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import duckdb
import pytest
from fastapi.testclient import TestClient

from rquant.serving_read_models import ServingProjectionPayload
from rquant.signal_contracts import SignalAction, SignalEnvelope
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


def test_timeline_interleaves_three_sources_across_pages(serving_root: Path) -> None:
    with TestClient(_app(serving_root)) as client:
        seen: list[tuple[str, str]] = []
        cursor = None
        while True:
            response = client.get(
                "/api/v1/monitor/timeline",
                params={"page_size": 1, **({"cursor": cursor} if cursor else {})},
            )
            assert response.status_code == 200, response.text
            data = response.json()["data"]
            assert data["total"] == 4
            assert len(data["items"]) == 1
            item = data["items"][0]
            seen.append((item["kind"], item["at"]))
            cursor = data["next_cursor"]
            if cursor is None:
                break
        assert [kind for kind, _ in seen] == ["monitor", "surge", "signal", "signal"]
        assert [at for _, at in seen] == sorted((at for _, at in seen), reverse=True)


def test_timeline_ties_and_late_sequences_have_stable_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tests.support import web_serving_fixture as fixture

    same_time = "09:47"
    monkeypatch.setattr(
        fixture,
        "_monitor_events",
        lambda: [
            {
                "trade_date": "2026-09-24",
                "trigger_time": "2026-09-24T01:47:00Z",
                "ts_code": "600004.SH",
                "level": "attack_break_high",
                "trigger_price": 12.34,
                "level_price": 12.0,
                "trigger_type": "attack",
                "pool": "pool2",
            }
        ],
    )
    monkeypatch.setattr(
        fixture,
        "_timeline_surge_events",
        lambda _scenario: [
            {
                **fixture._sample_surge_event(),
                "confirmed_at": same_time,
            }
        ],
    )
    original_signal = fixture._signal

    def signal_at_event_time(
        *, strategy_id: str, candidate_id: str, action: SignalAction, available_at: datetime
    ) -> SignalEnvelope:
        signal = original_signal(
            strategy_id=strategy_id,
            candidate_id=candidate_id,
            action=action,
            available_at=available_at,
        )
        if signal.candidate_id != "600001.SH":
            return signal
        return type(signal).model_validate(
            {
                **signal.model_dump(mode="python"),
                "event_time": signal.available_at,
                "signal_id": None,
            }
        )

    monkeypatch.setattr(fixture, "_signal", signal_at_event_time)
    root = tmp_path / "tie-serving"
    build_web_fixture(root, "baseline")
    with TestClient(_app(root)) as client:
        keys: list[str] = []
        cursor = None
        while True:
            response = client.get(
                "/api/v1/monitor/timeline",
                params={"page_size": 1, **({"cursor": cursor} if cursor else {})},
            )
            assert response.status_code == 200, response.text
            data = response.json()["data"]
            keys.extend(item["event_key"] for item in data["items"])
            cursor = data["next_cursor"]
            if cursor is None:
                break
        assert len(keys) == len(set(keys)) == 4
        assert [key.split(":", 1)[0] for key in keys] == ["signal", "monitor", "surge", "signal"]


def test_bad_surge_time_is_visible_as_partial_data_without_leaking_raw_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tests.support import web_serving_fixture as fixture

    monkeypatch.setattr(
        fixture,
        "_timeline_surge_events",
        lambda _scenario: [{**fixture._sample_surge_event(), "confirmed_at": "25:90"}],
    )
    root = tmp_path / "bad-time-serving"
    build_web_fixture(root, "baseline")
    with TestClient(_app(root)) as client:
        response = client.get("/api/v1/monitor/timeline")
        assert response.status_code == 200, response.text
        data = response.json()["data"]
        assert data["total"] == 3
        assert "1 条爆量记录时间无效" in data["source_note"]
        assert "25:90" not in response.text


def test_timeline_cursor_is_bound_to_generation_page_size_and_full_sort_key(
    serving_root: Path,
) -> None:
    app = _app(serving_root)
    with TestClient(app) as client:
        first = client.get("/api/v1/monitor/timeline", params={"page_size": 1})
        assert first.status_code == 200, first.text
        cursor = first.json()["data"]["next_cursor"]
        assert cursor
        assert (
            client.get(
                "/api/v1/monitor/timeline", params={"page_size": 2, "cursor": cursor}
            ).status_code
            == 409
        )
        changed = f"{cursor[:-1]}{'a' if cursor[-1] != 'a' else 'b'}"
        assert (
            client.get(
                "/api/v1/monitor/timeline", params={"page_size": 1, "cursor": changed}
            ).status_code
            == 409
        )
        build_web_fixture(serving_root, "baseline", sequence=1)
        app.state.web.tracker.refresh()
        assert (
            client.get(
                "/api/v1/monitor/timeline", params={"page_size": 1, "cursor": cursor}
            ).status_code
            == 409
        )


def test_timeline_keeps_real_receipts_on_signal_rows_only(serving_root: Path) -> None:
    with TestClient(_app(serving_root)) as client:
        first = client.get("/api/v1/monitor/timeline", params={"page_size": 3})
        assert first.status_code == 200, first.text
        data = first.json()["data"]
        assert data["source_state"] == "ready"
        assert data["receipt_state"] == "has_receipts"
        assert data["total"] == 4
        assert data["mode"] == "shadow"
        assert [item["kind"] for item in data["items"]] == ["monitor", "surge", "signal"]
        assert "receipts" not in data["items"][0]
        assert "receipts" not in data["items"][1]
        signal = data["items"][2]
        assert signal["sequence"] == 1
        assert signal["strategy_name"] == "N 字"
        assert signal["action_label"] == "买入意向"
        assert signal["delivery_label"] == "送达未确认"
        assert signal["receipts"][0]["status_label"] == "送达未确认"
        assert "当时" in signal["delivery_note"]
        assert data["next_cursor"]

        second = client.get(
            "/api/v1/monitor/timeline",
            params={"page_size": 3, "cursor": data["next_cursor"]},
        )
        assert second.status_code == 200, second.text
        older = second.json()["data"]
        assert [item["sequence"] for item in older["items"]] == [2]
        assert older["items"][0]["delivery_label"] == "发送中"
        assert older["items"][0]["receipts"][0]["status_label"] == "待发送"
        assert older["next_cursor"] is None


def test_surge_near_limit_status_does_not_claim_the_stock_hit_limit(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    build_web_fixture(root, "panorama")
    with TestClient(_app(root)) as client:
        response = client.get("/api/v1/monitor/timeline", params={"page_size": 50})
        assert response.status_code == 200, response.text
        labels = [
            item["status_label"]
            for item in response.json()["data"]["items"]
            if item["kind"] == "surge"
        ]
        assert "临近涨停" in labels
        assert "已涨停" not in labels


def test_no_serving_has_separate_source_state(tmp_path: Path) -> None:
    with TestClient(_app(tmp_path / "missing")) as client:
        response = client.get("/api/v1/monitor/timeline")
        assert response.status_code == 200
        assert response.json()["data"]["source_state"] == "unavailable"
        assert response.json()["data"]["total"] is None
        assert response.json()["data"]["items"] == []


def test_empty_timeline_with_missing_legacy_sources_is_labeled_partial(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tests.support import web_serving_fixture as fixture

    original = fixture._projections
    monkeypatch.setattr(fixture, "_signal_bundle", lambda _built_at: ((), (), ()))
    monkeypatch.setattr(
        fixture,
        "_projections",
        lambda scenario, *, built_at, generations: tuple(
            item
            for item in original(scenario, built_at=built_at, generations=generations)
            if item.table_name not in {"monitor_event", "surge_event"}
        ),
    )
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline")
    with TestClient(_app(root)) as client:
        response = client.get("/api/v1/monitor/timeline")
        assert response.status_code == 200, response.text
        data = response.json()["data"]
        assert data["source_state"] == "empty"
        assert data["source_label"] == "告警数据暂不完整"
        assert "仅显示已有记录" in data["source_note"]


def test_all_three_sources_can_publish_a_truly_empty_timeline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tests.support import web_serving_fixture as fixture

    monkeypatch.setattr(fixture, "_signal_bundle", lambda _built_at: ((), (), ()))
    root = tmp_path / "serving"
    build_web_fixture(
        root,
        "baseline",
        event_projections=tuple(
            ServingProjectionPayload(table_name=name, available_at=FIXTURE_BUILT_AT, rows=())
            for name in ("monitor_event", "surge_event")
        ),
    )
    with TestClient(_app(root)) as client:
        response = client.get("/api/v1/monitor/timeline")
        assert response.status_code == 200, response.text
        data = response.json()["data"]
        assert data["source_state"] == "empty"
        assert data["source_label"] == "最近 30 天没有告警"
        assert data["source_note"] is None
        assert data["total"] == 0


def test_unpublished_signal_source_is_distinct_from_an_empty_page(
    serving_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.web.routes import monitor

    monkeypatch.setattr(monitor, "_source_published", lambda _borrowed: False)
    with TestClient(_app(serving_root)) as client:
        response = client.get("/api/v1/monitor/timeline")
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
        "'2026-09-24 01:47:00+00'::TIMESTAMPTZ AS event_time, "
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
            after = (decoded.last_at, decoded.last_rank, decoded.last_key)
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
        assert empty.receipt_label == "本页没有新运行时通知回执"

        shanghai = ZoneInfo("Asia/Shanghai")
        ten = datetime(2026, 9, 24, 10, tzinfo=shanghai)
        nine_thirty = datetime(2026, 9, 24, 9, 30, tzinfo=shanghai)
        connection.executemany(
            "INSERT INTO signals VALUES "
            "(?, ?, 'n_shape', 'v1', '600001.SH', 'watch', ?, ?, NULL, '[]')",
            [
                (1, "late-1", ten, ten),
                (2, "late-2", nine_thirty, nine_thirty),
                (3, "late-3", ten, ten),
                (4, "late-4", nine_thirty, nine_thirty),
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
            assert decoded.last_at.astimezone(UTC) == page.items[-1].at
            after = (decoded.last_at, decoded.last_rank, decoded.last_key)
        assert seen_late == [3, 1, 4, 2]
        connection.execute(
            "INSERT INTO signals VALUES "
            "(5, 'old-signal', 'n_shape', 'v1', '600001.SH', 'watch', "
            "'2026-08-01 01:00:00+00'::TIMESTAMPTZ, "
            "'2026-09-24 01:55:00+00'::TIMESTAMPTZ, NULL, '[]')"
        )
        recent = _page(borrowed, page_size=50, after=None, key=key, now=FIXTURE_BUILT_AT)
        assert recent.total == 4
        assert all(item.event_key != "signal:old-signal" for item in recent.items)
        connection.execute(
            "INSERT INTO signals VALUES "
            "(6, 'delayed-signal', 'n_shape', 'v1', '600001.SH', 'watch', "
            "'2026-09-24 01:40:00+00'::TIMESTAMPTZ, "
            "'2026-09-24 02:20:00+00'::TIMESTAMPTZ, NULL, '[]')"
        )
        delayed = _page(borrowed, page_size=50, after=None, key=key, now=FIXTURE_BUILT_AT)
        assert delayed.total == 5
        assert [item.sequence for item in delayed.items] == [3, 1, 6, 4, 2]
        assert delayed.items[2].event_key == "signal:delayed-signal"
        assert delayed.items[2].at == datetime(2026, 9, 24, 1, 40, tzinfo=UTC)
        local_window_start = datetime(2026, 8, 26, tzinfo=shanghai)
        connection.executemany(
            "INSERT INTO signals VALUES "
            "(?, ?, 'n_shape', 'v1', '600001.SH', 'watch', ?, ?, NULL, '[]')",
            [
                (7, "window-edge", local_window_start, ten),
                (8, "before-window", local_window_start - timedelta(seconds=1), ten),
            ],
        )
        bounded = _page(borrowed, page_size=50, after=None, key=key, now=FIXTURE_BUILT_AT)
        assert bounded.total == 6
        assert {item.event_key for item in bounded.items} >= {"signal:window-edge"}
        assert "signal:before-window" not in {item.event_key for item in bounded.items}
    finally:
        borrowed.cursor.close()
        connection.close()
