"""Single-stock formula preview against a synthetic verified read-only replica."""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from rquant.replica_generation import (
    capture_database_watermark,
    replica_generation_path,
    write_replica_generation_metadata,
)
from rquant.screen.formula_history_projection import (
    _BARS_SQL,
    publish_formula_history_projection,
)
from rquant.screen.replica_source import VerifiedReplicaScreenSource
from rquant.storage.duckdb import DuckDBStore
from rquant.web.app import create_app
from rquant.web.settings import WebSettings
from tests.support.web_serving_fixture import build_web_fixture

DAY = date(2026, 4, 15)
AFTER_CLOSE = datetime(2026, 4, 15, 9, tzinfo=UTC)


def _publish(primary: Path, replica: Path) -> None:
    shutil.copy2(primary, replica)
    write_replica_generation_metadata(
        primary_path=primary,
        replica_path=replica,
        output_path=replica_generation_path(replica),
        source_before=capture_database_watermark(primary),
    )
    publish_formula_history_projection(
        VerifiedReplicaScreenSource(primary_path=primary, replica_path=replica),
        primary.parent / "history",
    )


def _world(tmp_path: Path) -> tuple[Path, Path]:
    primary = tmp_path / "rquant.duckdb"
    replica = tmp_path / "rquant_ro.duckdb"
    with DuckDBStore(primary) as store:
        for offset in range(6):
            day = date(2026, 4, 10) + timedelta(days=offset)
            is_open = day.weekday() < 5
            store._conn.execute(
                "INSERT INTO trade_calendar (exchange, cal_date, is_open, source, updated_at) "
                "VALUES ('SSE', ?, ?, 'fixture', ?)",
                [day, is_open, AFTER_CLOSE],
            )
        for code in ("600001.SH", "600002.SH"):
            store._conn.execute(
                "INSERT INTO stock_basic (ts_code, list_date) VALUES (?, ?)",
                [code, date(2026, 4, 13)],
            )
            for index, day in enumerate((date(2026, 4, 13), date(2026, 4, 14), DAY)):
                close = float(index + 1) if code == "600001.SH" else 5.0
                store._conn.execute(
                    "INSERT INTO daily_bar "
                    "(ts_code, trade_date, open, high, low, close, vol, amount) "
                    "VALUES (?, ?, ?, ?, ?, ?, 100, 1000)",
                    [code, day, close, close + 1, close - 1, close],
                )
    _publish(primary, replica)
    return primary, replica


def _client(
    tmp_path: Path, primary: Path | None = None, replica: Path | None = None,
    *, now: datetime = AFTER_CLOSE, serving_root: Path | None = None,
    history_root: Path | None = None,
) -> TestClient:
    return TestClient(create_app(
        WebSettings(
            serving_root=serving_root or tmp_path / "absent",
            screen_primary_path=primary,
            screen_replica_path=replica,
            screen_history_root=history_root or tmp_path / "history",
        ),
        clock=lambda: now,
        background=False,
    ))


def _preview(
    client: TestClient, identity: str, formula: str,
    *, code: str = "600001.SH", day: str = "2026-04-15",
):
    return client.post(
        "/api/v1/screen/tdx/preview",
        json={
            "source": formula,
            "stock_code": code,
            "trade_date": day,
            "source_identity": identity,
        },
        headers={"X-Rquant-Csrf": "1"},
    )


def _identity(client: TestClient) -> str:
    response = client.get("/api/v1/screen/tdx/preview/source")
    assert response.status_code == 200, response.text
    assert response.json()["available"]
    return response.json()["source"]["identity"]


@pytest.mark.parametrize(
    ("formula", "expected"),
    [
        ("CLOSE>MA(CLOSE,2)", "match"),
        ("CLOSE<MA(CLOSE,2)", "no_match"),
        ("EMA(CLOSE,2)>2", "match"),
    ],
)
def test_preview_evaluates_one_stock_from_verified_replica(
    tmp_path: Path, formula: str, expected: str,
) -> None:
    primary, replica = _world(tmp_path)
    with _client(tmp_path, primary, replica) as client:
        identity = _identity(client)
        response = _preview(client, identity, formula)

    assert response.status_code == 200, response.text
    data = response.json()
    assert data["stock_code"] == "600001.SH"
    assert data["trade_date"] == DAY.isoformat()
    assert data["status"] == expected
    assert data["reason"] is None
    assert data["source_updated_at"]
    assert identity not in response.text
    assert str(primary) not in response.text


def test_new_listing_with_complete_but_short_history_remains_unknown(tmp_path: Path) -> None:
    primary, replica = _world(tmp_path)
    with _client(tmp_path, primary, replica) as client:
        response = _preview(client, _identity(client), "MA(CLOSE,20)>0")

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "unknown"
    assert response.json()["reason"] == "历史天数不足，暂无法判断。"


def test_prelisting_dirty_bar_cannot_complete_fixed_window(tmp_path: Path) -> None:
    primary, replica = _world(tmp_path)
    with DuckDBStore(primary) as store:
        store._conn.execute(
            "INSERT INTO daily_bar "
            "(ts_code, trade_date, open, high, low, close, vol, amount) "
            "VALUES ('600001.SH', '2026-04-10', 10, 11, 9, 10, 100, 1000)"
        )
    _publish(primary, replica)
    with _client(tmp_path, primary, replica) as client:
        response = _preview(
            client, _identity(client), "MA(CLOSE,2)>0", day="2026-04-13",
        )

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "unknown"
    assert response.json()["reason"] == "历史天数不足，暂无法判断。"


@pytest.mark.parametrize(
    ("change", "formula"),
    [
        ("missing_target", "CLOSE>0"),
        ("missing_bar", "MA(CLOSE,3)>0"),
        ("missing_calendar", "MA(CLOSE,3)>0"),
        ("missing_listing", "EMA(CLOSE,2)>0"),
        ("non_finite", "CLOSE>0"),
    ],
)
def test_missing_date_history_calendar_or_listing_is_unknown(
    tmp_path: Path, change: str, formula: str,
) -> None:
    primary, replica = _world(tmp_path)
    with DuckDBStore(primary) as store:
        if change == "missing_target":
            store._conn.execute(
                "DELETE FROM daily_bar WHERE ts_code = '600001.SH' AND trade_date = ?", [DAY]
            )
        elif change == "missing_bar":
            store._conn.execute(
                "DELETE FROM daily_bar WHERE ts_code = '600001.SH' AND trade_date = ?",
                [date(2026, 4, 14)],
            )
        elif change == "missing_calendar":
            store._conn.execute(
                "DELETE FROM trade_calendar WHERE exchange = 'SSE' AND cal_date = ?",
                [date(2026, 4, 14)],
            )
        elif change == "non_finite":
            store._conn.execute(
                "UPDATE daily_bar SET close = CAST('NaN' AS DOUBLE) "
                "WHERE ts_code = '600001.SH' AND trade_date = ?", [DAY]
            )
        else:
            store._conn.execute("DELETE FROM stock_basic WHERE ts_code = '600001.SH'")
    _publish(primary, replica)
    with _client(tmp_path, primary, replica) as client:
        response = _preview(client, _identity(client), formula)

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "unknown"
    assert response.json()["reason"]


def test_preview_rejects_stale_source_identity_after_projection_rotation(tmp_path: Path) -> None:
    primary, replica = _world(tmp_path)
    with _client(tmp_path, primary, replica) as client:
        identity = _identity(client)
        publish_formula_history_projection(
            VerifiedReplicaScreenSource(primary_path=primary, replica_path=replica),
            tmp_path / "history",
        )
        response = _preview(client, identity, "CLOSE>0")

    assert response.status_code == 409
    assert response.json() == {"detail": "公式预览数据已更新，请刷新后重试。"}


def test_preview_rejects_hardlinked_projection(tmp_path: Path) -> None:
    primary, replica = _world(tmp_path)
    history = tmp_path / "history"
    file_name = json.loads((history / "current.json").read_text())["file_name"]
    os.link(history / file_name, history / "alias.sqlite")
    with _client(tmp_path, primary, replica) as client:
        response = _preview(client, "a" * 64, "CLOSE>0")

    assert response.status_code == 503
    assert response.json() == {"detail": "公式预览数据暂不可用，请稍后重试。"}


def test_preview_never_reads_or_stats_primary_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary, replica = _world(tmp_path)
    original_stat = Path.stat
    original_lstat = Path.lstat

    def no_primary_stat(path: Path, *args, **kwargs):
        if path == primary:
            raise PermissionError("main database is hidden")
        return original_stat(path, *args, **kwargs)

    def no_primary_lstat(path: Path, *args, **kwargs):
        if path == primary:
            raise PermissionError("main database is hidden")
        return original_lstat(path, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", no_primary_stat)
    monkeypatch.setattr(Path, "lstat", no_primary_lstat)
    with _client(tmp_path, primary, replica) as client:
        response = _preview(client, _identity(client), "CLOSE>0")

    assert response.status_code == 200
    assert response.json()["status"] == "match"


def test_preview_does_not_reopen_the_screen_replica(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary, replica = _world(tmp_path)
    with _client(tmp_path, primary, replica) as client:
        screen_replica = client.app.state.web.screen_service.replica
        assert screen_replica is not None

        def forbidden_open():
            raise AssertionError("preview must use its published history source")

        monkeypatch.setattr(screen_replica, "_open", forbidden_open)
        response = _preview(client, _identity(client), "CLOSE>0")

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "match"


def test_history_lookup_is_a_bounded_primary_key_search_with_one_million_rows() -> None:
    connection = sqlite3.connect(":memory:")
    try:
        connection.execute(
            "CREATE TABLE bars (ts_code TEXT NOT NULL, trade_date TEXT NOT NULL, "
            "open REAL, high REAL, low REAL, close REAL, vol REAL, amount REAL, "
            "PRIMARY KEY(ts_code,trade_date)) WITHOUT ROWID"
        )
        connection.executemany(
            "INSERT INTO bars VALUES (?,?,?,?,?,?,?,?)",
            (
                (f"{code:06d}.SH", f"2025-{day // 28 + 1:02d}-{day % 28 + 1:02d}",
                 1.0, 1.0, 1.0, 1.0, 100.0, 100.0)
                for code in range(600000, 605000)
                for day in range(200)
            ),
        )
        sql = _BARS_SQL.format(listing="")
        parameters = ("600001.SH", "2025-08-04", 3)
        plan = connection.execute("EXPLAIN QUERY PLAN " + sql, parameters).fetchall()
        steps = 0

        def count_step() -> int:
            nonlocal steps
            steps += 1
            return 0

        connection.set_progress_handler(count_step, 1)
        rows = connection.execute(sql, parameters).fetchall()
        connection.set_progress_handler(None, 0)
        assert len(rows) == 3
        assert connection.execute("SELECT COUNT(*) FROM bars").fetchone()[0] == 1_000_000
        assert any("SEARCH bars USING PRIMARY KEY (ts_code=?" in row[3] for row in plan)
        assert not any("SCAN bars" in row[3] for row in plan)
        assert steps < 500
    finally:
        connection.close()


def test_preview_requires_open_calendar_date_and_closed_daily_bar(tmp_path: Path) -> None:
    primary, replica = _world(tmp_path)
    with _client(tmp_path, primary, replica) as client:
        identity = _identity(client)
        absent = _preview(client, identity, "CLOSE>0", day="2026-04-09")
        earlier_open = _preview(client, identity, "CLOSE>0", day="2026-04-10")
    with _client(tmp_path, primary, replica, now=AFTER_CLOSE - timedelta(minutes=1)) as client:
        early = _preview(client, _identity(client), "CLOSE>0")

    assert absent.status_code == 422
    assert absent.json() == {"detail": "请选择已开市的交易日。"}
    assert earlier_open.status_code == 200
    assert earlier_open.json()["status"] == "unknown"
    assert early.status_code == 422
    assert early.json() == {"detail": "这一天的日线尚未收盘，请换日期。"}


def test_preview_only_queries_selected_stock_history_and_calendar(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary, replica = _world(tmp_path)
    with _client(tmp_path, primary, replica) as client:
        identity = _identity(client)
        source = client.app.state.web.screen_service.history
        assert source is not None
        original_open = source._open
        statements: list[str] = []

        class ObservedConnection:
            def __init__(self, connection):
                self.connection = connection

            def execute(self, query: str, parameters=None):
                statements.append(query)
                return self.connection.execute(query, parameters)

            def close(self):
                self.connection.close()

            def set_progress_handler(self, callback, instructions):
                self.connection.set_progress_handler(callback, instructions)

        def observed_open():
            connection, descriptor, generation = original_open()
            return ObservedConnection(connection), descriptor, generation

        monkeypatch.setattr(source, "_open", observed_open)
        response = _preview(client, identity, "MA(CLOSE,2)>0")

    assert response.status_code == 200, response.text
    assert not any("daily_bar" in query for query in statements)
    assert any("FROM bars INDEXED BY" in query and "LIMIT ?" in query for query in statements)


def test_preview_rejects_invalid_formula_and_bounded_inputs(tmp_path: Path) -> None:
    primary, replica = _world(tmp_path)
    with _client(tmp_path, primary, replica) as client:
        identity = _identity(client)
        invalid = _preview(client, identity, "DYNAINFO(7)>0")
        oversized = _preview(client, identity, "X" * 4097)
        bad_code = _preview(client, identity, "CLOSE>0", code="600001.SH' OR 1=1")

    assert invalid.status_code == 422
    assert invalid.json() == {"detail": "公式尚未通过检查，请修改后重试。"}
    assert oversized.status_code == 413
    assert bad_code.status_code == 422
    assert "600001.SH' OR 1=1" not in bad_code.text


def test_preview_never_falls_back_to_unindexed_replica(tmp_path: Path) -> None:
    primary, replica = _world(tmp_path)
    with _client(tmp_path, primary, replica, history_root=tmp_path / "absent-history") as client:
        catalog = client.get("/api/v1/screen/tdx/preview/source")
        response = _preview(client, "a" * 64, "CLOSE>0")

    assert catalog.status_code == 200
    assert catalog.json() == {"available": False, "dates": [], "source": None}
    assert response.status_code == 503
    assert response.json() == {"detail": "公式预览数据暂不可用，请稍后重试。"}


def test_unconfigured_preview_never_falls_back_to_serving(tmp_path: Path) -> None:
    serving = tmp_path / "serving"
    build_web_fixture(serving, "baseline")
    with _client(tmp_path, serving_root=serving) as client:
        response = _preview(client, "a" * 64, "CLOSE>0")
    assert response.status_code == 503
    assert response.json() == {"detail": "公式预览数据暂不可用，请稍后重试。"}
