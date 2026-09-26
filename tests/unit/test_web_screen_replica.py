"""The optional web screen source stays bound to one verified replica generation."""

from __future__ import annotations

import json
import shutil
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from rquant.replica_generation import (
    capture_database_watermark,
    replica_generation_path,
    write_replica_generation_metadata,
)
from rquant.storage.duckdb import DuckDBStore
from rquant.web.app import create_app
from rquant.web.settings import WebSettings
from tests.support.web_serving_fixture import build_web_fixture


def _replica_world(tmp_path: Path, *, days: int = 3) -> tuple[Path, Path, date]:
    primary = tmp_path / "rquant.duckdb"
    replica = tmp_path / "rquant_ro.duckdb"
    latest = date(2026, 4, 15)
    with DuckDBStore(primary) as store:
        for offset in range(days):
            day = latest - timedelta(days=offset)
            store._conn.execute(
                "INSERT INTO trade_calendar (exchange, cal_date, is_open, source, updated_at) "
                "VALUES ('SSE', ?, TRUE, 'fixture', ?)",
                [day, datetime(2026, 4, 16, tzinfo=UTC)],
            )
            for index in range(3):
                code = f"60000{index + 1}.SH"
                store._conn.execute(
                    "INSERT INTO daily_bar (ts_code, trade_date, open, high, low, close, "
                    "pre_close, pct_chg, vol, amount) "
                    "VALUES (?, ?, 10, 11, 9, ?, 10, 1, 100, 1000)",
                    [code, day, 10.0 + index],
                )
                store._conn.execute(
                    "INSERT INTO stock_status_daily (ts_code, trade_date, name, is_st, "
                    "name_source, st_source, available_at, ingested_at) "
                    "VALUES (?, ?, ?, FALSE, 'fixture', 'fixture', ?, ?)",
                    [
                        code,
                        day,
                        f"样本{index + 1}",
                        datetime(day.year, day.month, day.day, 1, 25, tzinfo=UTC),
                        datetime(2026, 4, 16, tzinfo=UTC),
                    ],
                )
                store._conn.execute(
                    "INSERT INTO daily_state (ts_code, trade_date, is_st, is_bj, board_type, "
                    "is_limit_up, is_limit_down, is_first_limit_up, is_yiziban, "
                    "consecutive_limit_ups, body_upper, body_lower) "
                    "VALUES (?, ?, FALSE, FALSE, 'main', FALSE, "
                    "FALSE, FALSE, FALSE, 0, 0.1, 0.1)",
                    [code, day],
                )
                store._conn.execute(
                    "INSERT INTO daily_indicator "
                    "(ts_code, trade_date, ma5, ma10, ma20, ma60, rsi6, rsi14) "
                    "VALUES (?, ?, 9, 9, 9, 9, 35, 35)",
                    [code, day],
                )
                store._conn.execute(
                    "INSERT INTO daily_basic (ts_code, trade_date, circ_mv, turnover_rate) "
                    "VALUES (?, ?, ?, 2)",
                    [code, day, 100_000.0 + index * 10_000],
                )
    _publish(primary, replica)
    return primary, replica, latest


def _publish(primary: Path, replica: Path) -> None:
    shutil.copy2(primary, replica)
    write_replica_generation_metadata(
        primary_path=primary,
        replica_path=replica,
        output_path=replica_generation_path(replica),
        source_before=capture_database_watermark(primary),
    )


def _client(root: Path, primary: Path, replica: Path) -> TestClient:
    return TestClient(
        create_app(
            WebSettings(
                serving_root=root,
                screen_primary_path=primary,
                screen_replica_path=replica,
            ),
            background=False,
        )
    )


def _run(client: TestClient, *, conditions: list[dict] | None = None,
         cursor: str | None = None, page_size: int = 2, ranking: dict | None = None):
    return client.post(
        "/api/v1/screen/run",
        json={
            "trade_date": "2026-04-15",
            "conditions": conditions or [{"key": "not_st", "args": {}}],
            "page_size": page_size,
            "cursor": cursor,
            "ranking": ranking,
        },
        headers={"X-Rquant-Csrf": "1"},
    )


def test_explicit_replica_configuration_requires_both_paths(tmp_path: Path) -> None:
    primary, replica, _ = _replica_world(tmp_path)
    settings = WebSettings.from_env({
        "RQUANT_WEB_SCREEN_PRIMARY_PATH": str(primary),
        "RQUANT_WEB_SCREEN_REPLICA_PATH": str(replica),
    })
    assert settings.screen_primary_path == primary
    assert settings.screen_replica_path == replica
    assert WebSettings.from_env({}).screen_replica_path is None
    with pytest.raises(ValueError):
        WebSettings.from_env({"RQUANT_WEB_SCREEN_REPLICA_PATH": str(replica)})


def test_replica_catalog_and_pages_work_without_serving_and_bind_source_identity(
    tmp_path: Path,
) -> None:
    primary, replica, latest = _replica_world(tmp_path)
    with _client(tmp_path / "absent", primary, replica) as client:
        catalog = client.get("/api/v1/screen/blocks")
        first = _run(client)
        second = _run(client, cursor=first.json()["data"]["next_cursor"])

    assert catalog.status_code == first.status_code == second.status_code == 200
    assert catalog.json()["serving"]["generation_id"] is None
    assert catalog.json()["data"]["available"] is True
    assert catalog.json()["data"]["dates"][0] == latest.isoformat()
    identity = catalog.json()["data"]["source"]["identity"]
    assert len(identity) == 64
    assert catalog.json()["data"]["source"]["updated_at"]
    assert first.json()["data"]["source"]["identity"] == identity
    assert second.json()["data"]["source"]["identity"] == identity
    assert [row["ts_code"] for row in first.json()["data"]["rows"]] == [
        "600001.SH", "600002.SH",
    ]
    assert [row["ts_code"] for row in second.json()["data"]["rows"]] == ["600003.SH"]
    assert first.json()["data"]["steps"] == [{"label": "排除 ST", "count": 3}]


def test_configured_broken_replica_never_falls_back_to_available_serving(
    tmp_path: Path,
) -> None:
    primary, replica, _ = _replica_world(tmp_path)
    serving = tmp_path / "serving"
    build_web_fixture(serving, "baseline")
    replica_generation_path(replica).write_text("{}", encoding="utf-8")
    with _client(serving, primary, replica) as client:
        catalog = client.get("/api/v1/screen/blocks")
        result = _run(client)

    assert catalog.status_code == 200
    assert catalog.json()["data"]["available"] is False
    assert catalog.json()["data"]["source"] is None
    assert result.status_code == 503
    assert result.json() == {"detail": "选股数据暂不可用，请稍后重试。"}
    assert "rquant_ro.duckdb" not in result.text


def test_replica_rotation_rejects_previous_cursor_even_when_serving_does_not_change(
    tmp_path: Path,
) -> None:
    primary, replica, _ = _replica_world(tmp_path)
    with _client(tmp_path / "absent", primary, replica) as client:
        first = _run(client)
        assert first.status_code == 200, first.text
        old_identity = first.json()["data"]["source"]["identity"]
        cursor = first.json()["data"]["next_cursor"]
        replacement = tmp_path / "replacement.duckdb"
        shutil.copy2(replica, replacement)
        replacement.replace(replica)
        write_replica_generation_metadata(
            primary_path=primary, replica_path=replica,
            output_path=replica_generation_path(replica),
            source_before=capture_database_watermark(primary),
        )
        catalog = client.get("/api/v1/screen/blocks")
        stale_page = _run(client, cursor=cursor)

    assert catalog.json()["data"]["source"]["identity"] != old_identity
    assert stale_page.status_code == 409
    assert stale_page.json() == {"detail": "选股数据已更新，请重新筛选。"}


def test_missing_open_day_facts_are_unavailable_instead_of_zero_hits(tmp_path: Path) -> None:
    primary, replica, latest = _replica_world(tmp_path)
    with DuckDBStore(primary) as store:
        store._conn.execute("DELETE FROM daily_bar WHERE trade_date = ?", [latest])
    _publish(primary, replica)
    with _client(tmp_path / "absent", primary, replica) as client:
        result = _run(client)
    assert result.status_code == 503
    assert result.json() == {"detail": "所选日期的数据不完整，请换日期或稍后重试。"}


def test_missing_indicator_facts_are_unavailable_instead_of_zero_hits(tmp_path: Path) -> None:
    primary, replica, latest = _replica_world(tmp_path)
    with DuckDBStore(primary) as store:
        store._conn.execute("DELETE FROM daily_indicator WHERE trade_date = ?", [latest])
    _publish(primary, replica)
    with _client(tmp_path / "absent", primary, replica) as client:
        result = _run(client, conditions=[{"key": "above_ma", "args": {"period": 20}}])
    assert result.status_code == 503
    assert result.json() == {"detail": "所选日期的数据不完整，请换日期或稍后重试。"}


def test_missing_aggregate_facts_are_unavailable_instead_of_zero_hits(tmp_path: Path) -> None:
    primary, replica, latest = _replica_world(tmp_path)
    with DuckDBStore(primary) as store:
        store._conn.execute(
            "DELETE FROM daily_state WHERE trade_date = ?", [latest - timedelta(days=1)]
        )
    _publish(primary, replica)
    with _client(tmp_path / "absent", primary, replica) as client:
        result = _run(
            client, conditions=[{"key": "no_limit_down_in_window", "args": {"window": 2}}]
        )
    assert result.status_code == 503
    assert result.json() == {"detail": "所选日期的数据不完整，请换日期或稍后重试。"}


def test_catalog_and_api_offer_only_persisted_indicator_periods_and_metrics(
    tmp_path: Path,
) -> None:
    primary, replica, _ = _replica_world(tmp_path)
    with _client(tmp_path / "absent", primary, replica) as client:
        catalog = client.get("/api/v1/screen/blocks").json()["data"]
        unsupported = _run(client, conditions=[{"key": "above_ma", "args": {"period": 7}}])
        supported = _run(client, conditions=[{"key": "above_ma", "args": {"period": "20"}}])
        missing_metric = _run(client, ranking={
            "conditions": [{"metric": "RETURN_20D_PCT[0]", "ascending": False, "weight": 100}],
            "top_n": 10,
        })

    blocks = {block["key"]: block for block in catalog["blocks"]}
    above_period = next(p for p in blocks["above_ma"]["parameters"] if p["key"] == "period")
    rsi_period = next(p for p in blocks["rsi_oversold"]["parameters"] if p["key"] == "period")
    assert above_period["input"] == rsi_period["input"] == "choice"
    assert {item["value"] for item in above_period["options"]} == {"5", "10", "20", "60"}
    assert {item["value"] for item in rsi_period["options"]} == {"6", "14"}
    assert "RETURN_20D_PCT[0]" not in {item["value"] for item in catalog["ranking_metrics"]}
    assert unsupported.status_code == 422
    assert "当前仅支持" in unsupported.json()["detail"]
    assert supported.status_code == 200, supported.text
    assert missing_metric.status_code == 422


def test_bad_sidecar_source_identity_is_unavailable(tmp_path: Path) -> None:
    primary, replica, _ = _replica_world(tmp_path)
    sidecar = replica_generation_path(replica)
    payload = json.loads(sidecar.read_text(encoding="utf-8"))
    payload["source_database"] = str(tmp_path / "other.duckdb")
    sidecar.write_text(json.dumps(payload), encoding="utf-8")
    with _client(tmp_path / "absent", primary, replica) as client:
        result = _run(client)
    assert result.status_code == 503
    assert "other.duckdb" not in result.text


def test_all_26_catalog_conditions_execute_against_the_same_formal_replica_schema(
    tmp_path: Path,
) -> None:
    primary, replica, _ = _replica_world(tmp_path, days=121)
    with _client(tmp_path / "absent", primary, replica) as client:
        catalog = client.get("/api/v1/screen/blocks").json()["data"]
        identity = catalog["source"]["identity"]
        assert len(catalog["blocks"]) == 26
        for block in catalog["blocks"]:
            args = {
                parameter["key"]: parameter["initial"]
                for parameter in block["parameters"]
                if parameter["initial"] is not None
            }
            response = _run(
                client,
                conditions=[{"key": block["key"], "args": args}],
                page_size=3,
            )
            assert response.status_code == 200, (block["key"], response.text)
            data = response.json()["data"]
            assert data["status"] == "ready", block["key"]
            assert data["source"]["identity"] == identity
            assert data["steps"] == [{"label": block["label"], "count": data["total"]}]


def test_replica_ranking_keeps_order_across_pages(tmp_path: Path) -> None:
    primary, replica, _ = _replica_world(tmp_path)
    ranking = {
        "conditions": [{"metric": "CIRC_MV[0]", "ascending": False, "weight": 100}],
        "top_n": 3,
    }
    with _client(tmp_path / "absent", primary, replica) as client:
        first = _run(client, ranking=ranking)
        second = _run(client, ranking=ranking, cursor=first.json()["data"]["next_cursor"])

    assert first.status_code == second.status_code == 200
    assert [row["ts_code"] for row in first.json()["data"]["rows"]] == [
        "600003.SH", "600002.SH",
    ]
    assert [row["ts_code"] for row in second.json()["data"]["rows"]] == ["600001.SH"]
    assert [row["rank_position"] for row in first.json()["data"]["rows"]] == [1, 2]
    assert second.json()["data"]["rows"][0]["rank_position"] == 3


def test_replica_calendar_gap_reports_incomplete_data(tmp_path: Path) -> None:
    primary, replica, latest = _replica_world(tmp_path)
    with DuckDBStore(primary) as store:
        store._conn.execute(
            "DELETE FROM trade_calendar WHERE cal_date = ?", [latest - timedelta(days=1)]
        )
    _publish(primary, replica)
    with _client(tmp_path / "absent", primary, replica) as client:
        response = _run(client, conditions=[{"key": "first_limit_up", "args": {"offset": 2}}])
    assert response.status_code == 503
    assert response.json() == {"detail": "所选日期的数据不完整，请换日期或稍后重试。"}
