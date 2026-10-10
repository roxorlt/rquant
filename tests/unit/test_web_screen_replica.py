"""The optional web screen source stays bound to one verified replica generation."""

from __future__ import annotations

import json
import shutil
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from rquant.replica_generation import (
    capture_database_watermark,
    replica_generation_path,
    write_replica_generation_metadata,
)
from rquant.storage.duckdb import DuckDBStore
from rquant.web.settings import WebSettings
from tests.support.web_proxy_identity import ResearcherTestClient as TestClient
from tests.support.web_proxy_identity import create_private_test_app as create_app
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


def test_replica_nl_preview_stays_available_without_serving(tmp_path: Path) -> None:
    from tests.support.ai_assistance_fixture import OfflineModelScenario, original_ai_test_app
    scenario = OfflineModelScenario({"trade_date": "2026-04-15",
        "stages": [{"label": "条件", "rules": [{"name": "not_st", "args": {}}]}]})

    primary, replica, _ = _replica_world(tmp_path)
    app = original_ai_test_app(tmp_path / "absent", scenario,
        clock=lambda: datetime.now(UTC), primary_path=primary, replica_path=replica)
    with TestClient(app) as client:
        catalog = client.get("/api/v1/screen/blocks").json()
        source = catalog["data"]["source"]
        response = client.post(
            "/api/v1/screen/nl-preview",
            json={
                "source_kind": "replica",
                "source_identity": source["identity"],
                "trade_date": "2026-04-15",
                "instruction": "排除 ST",
            },
            headers={
                "x-rquant-user": "researcher",
                "x-rquant-csrf": "1",
                "origin": "http://testserver",
                "x-rquant-ai-request-id": "e28878d8-2d82-4c92-93ab-6c1e963f8530",
            },
        )
    assert catalog["serving"]["generation_id"] is None
    assert catalog["data"]["nl_generate_available"] is True
    assert response.status_code == 200, response.text
    assert response.json() == {
        "source_kind": "replica",
        "source_identity": source["identity"],
        "trade_date": "2026-04-15",
        "conditions": [{"key": "not_st", "args": {}}],
    }
    assert scenario.calls == 1
    request = json.loads(scenario.requests[0].content)
    assert request["messages"][1]["content"] == "排除 ST"
    assert "只使用当前目录的条件" in request["messages"][0]["content"]


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
    assert catalog.json()["data"]["source_kind"] == "replica"
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
    assert first.json()["data"]["steps"] == [
        {"label": "排除 ST", "count": 3, "unknown_count": 0}
    ]


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
    assert catalog.json()["data"]["source_kind"] == "replica"
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


def test_one_missing_indicator_fact_is_explicitly_unknown_in_each_screen_step(
    tmp_path: Path,
) -> None:
    primary, replica, latest = _replica_world(tmp_path)
    with DuckDBStore(primary) as store:
        store._conn.execute(
            "DELETE FROM daily_indicator WHERE ts_code = '600001.SH' AND trade_date = ?",
            [latest],
        )
        store._conn.execute(
            "UPDATE daily_indicator SET ma20 = 100 "
            "WHERE ts_code != '600001.SH' AND trade_date = ?",
            [latest],
        )
    _publish(primary, replica)
    with _client(tmp_path / "absent", primary, replica) as client:
        response = _run(client, conditions=[
            {"key": "not_st", "args": {}},
            {"key": "above_ma", "args": {"period": 20}},
        ])
        resolved = _run(client, conditions=[
            {"key": "not_st", "args": {}},
            {"key": "above_ma", "args": {"period": 20}},
            {"key": "circ_mv_lt", "args": {"threshold_yi": 1}},
        ])
    assert response.status_code == 200, response.text
    data = response.json()["data"]
    assert (data["base_count"], data["total"], data["unknown_count"]) == (3, 0, 1)
    assert data["steps"] == [
        {"label": "排除 ST", "count": 3, "unknown_count": 0},
        {"label": "收盘价高于均线", "count": 0, "unknown_count": 1},
    ]
    assert resolved.status_code == 200
    assert resolved.json()["data"]["unknown_count"] == 0
    assert resolved.json()["data"]["steps"][-1] == {
        "label": "流通市值低于", "count": 0, "unknown_count": 0,
    }


def test_legitimate_short_indicator_history_remains_explicitly_unknown(
    tmp_path: Path,
) -> None:
    primary, replica, latest = _replica_world(tmp_path)
    with DuckDBStore(primary) as store:
        store._conn.execute(
            "UPDATE daily_indicator SET ma20 = NULL "
            "WHERE ts_code = '600001.SH' AND trade_date = ?",
            [latest],
        )
        store._conn.execute(
            "UPDATE daily_indicator SET ma20 = 100 "
            "WHERE ts_code != '600001.SH' AND trade_date = ?",
            [latest],
        )
    _publish(primary, replica)
    with _client(tmp_path / "absent", primary, replica) as client:
        response = _run(client, conditions=[{"key": "above_ma", "args": {"period": 20}}])
    assert response.status_code == 200, response.text
    assert response.json()["data"]["unknown_count"] == 1


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


def test_replica_catalog_offers_bounded_ma_periods_and_custom_compare_fields(
    tmp_path: Path,
) -> None:
    primary, replica, _ = _replica_world(tmp_path)
    with _client(tmp_path / "absent", primary, replica) as client:
        catalog = client.get("/api/v1/screen/blocks").json()["data"]
        legacy_period = _run(client, conditions=[{"key": "above_ma", "args": {"period": "20"}}])
        incomplete_return = _run(client, ranking={
            "conditions": [{"metric": "RETURN_20D_PCT[0]", "ascending": False, "weight": 100}],
            "top_n": 10,
        })

    blocks = {block["key"]: block for block in catalog["blocks"]}
    above_period = next(p for p in blocks["above_ma"]["parameters"] if p["key"] == "period")
    rsi_period = next(p for p in blocks["rsi_oversold"]["parameters"] if p["key"] == "period")
    assert above_period["input"] == "integer"
    assert above_period["minimum"] == 2
    assert above_period["maximum"] == 250
    assert above_period["initial"] == 20
    assert above_period["options"] == []
    for rule_name in ("cross_above", "cross_below"):
        params = {item["key"]: item for item in blocks[rule_name]["parameters"]}
        for key, initial in (("fast", 5), ("slow", 20)):
            assert params[key]["input"] == "integer"
            assert params[key]["initial"] == initial
            assert (params[key]["minimum"], params[key]["maximum"]) == (2, 250)
        assert (params["offset"]["minimum"], params["offset"]["maximum"]) == (0, 30)
    assert rsi_period["input"] == "choice"
    assert {item["value"] for item in rsi_period["options"]} == {"6", "14"}
    for rule_name in ("gt", "lt", "gte", "lte"):
        for parameter in blocks[rule_name]["parameters"]:
            assert parameter["custom_ma"] is True
            assert parameter["input"] == "operand"
    assert next(p for p in blocks["between"]["parameters"] if p["key"] == "field")[
        "custom_ma"
    ] is True
    assert {item["value"] for item in next(
        p for p in blocks["gt"]["parameters"] if p["key"] == "left"
    )["options"]} >= {"CLOSE[0]", "MA5[0]"}
    assert "MA7[0]" not in {item["value"] for item in next(
        p for p in blocks["gt"]["parameters"] if p["key"] == "left"
    )["options"]}
    assert "RETURN_20D_PCT[0]" in {item["value"] for item in catalog["ranking_metrics"]}
    assert legacy_period.status_code == 200, legacy_period.text
    assert incomplete_return.status_code == 503
    assert incomplete_return.json()["detail"] == "当前排名数据不完整，请换一个指标或稍后重试。"


@pytest.mark.parametrize("condition", [
    {"key": "gt", "args": {"left": "MA7[2]", "right": 9}},
    {"key": "lt", "args": {"left": 9, "right": "MA7[2]"}},
    {"key": "gte", "args": {"left": "MA7[2]", "right": 10}},
    {"key": "lte", "args": {"left": "MA7[2]", "right": 12}},
    {"key": "between", "args": {"field": "MA2[30]", "low": 10, "high": 11}},
])
def test_replica_compare_and_between_use_custom_ma_from_verified_replica(
    tmp_path: Path, condition: dict,
) -> None:
    primary, replica, _ = _replica_world(tmp_path, days=34)
    with DuckDBStore(primary) as store:
        store._conn.execute(
            "INSERT INTO adj_factor (ts_code, trade_date, adj_factor) "
            "SELECT ts_code, trade_date, 1 FROM daily_bar"
        )
    _publish(primary, replica)
    with _client(tmp_path / "absent", primary, replica) as client:
        result = _run(client, conditions=[condition])
    assert result.status_code == 200, result.text
    assert result.json()["data"]["source"]["identity"]
    assert result.json()["data"]["unknown_count"] == 0


def test_custom_ma_maximum_period_and_offset_are_accepted_before_history_check(
    tmp_path: Path,
) -> None:
    primary, replica, _ = _replica_world(tmp_path)
    with _client(tmp_path / "absent", primary, replica) as client:
        result = _run(client, conditions=[
            {"key": "between", "args": {"field": "MA250[30]", "low": 0, "high": 10}},
        ])
    assert result.status_code == 503
    assert result.json()["detail"] == "所选日期的数据不完整，请换日期或稍后重试。"


def test_custom_ma_cursor_rejects_a_rotated_replica(tmp_path: Path) -> None:
    primary, replica, _ = _replica_world(tmp_path)
    with DuckDBStore(primary) as store:
        store._conn.execute(
            "INSERT INTO adj_factor (ts_code, trade_date, adj_factor) "
            "SELECT ts_code, trade_date, 1 FROM daily_bar"
        )
    _publish(primary, replica)
    condition = [{"key": "gt", "args": {"left": "MA2[0]", "right": 9}}]
    with _client(tmp_path / "absent", primary, replica) as client:
        first = _run(client, conditions=condition, page_size=1)
        assert first.status_code == 200, first.text
        cursor = first.json()["data"]["next_cursor"]
        assert cursor
        replacement = tmp_path / "replacement.duckdb"
        shutil.copy2(replica, replacement)
        replacement.replace(replica)
        write_replica_generation_metadata(
            primary_path=primary, replica_path=replica,
            output_path=replica_generation_path(replica),
            source_before=capture_database_watermark(primary),
        )
        stale = _run(client, conditions=condition, cursor=cursor, page_size=1)
    assert stale.status_code == 409
    assert stale.json() == {"detail": "选股数据已更新，请重新筛选。"}


def test_custom_ma_operand_missing_price_and_adjustment_remain_unknown(
    tmp_path: Path,
) -> None:
    primary, replica, latest = _replica_world(tmp_path, days=34)
    with DuckDBStore(primary) as store:
        store._conn.execute(
            "INSERT INTO adj_factor (ts_code, trade_date, adj_factor) "
            "SELECT ts_code, trade_date, 1 FROM daily_bar"
        )
        store._conn.execute(
            "DELETE FROM daily_bar WHERE ts_code = '600002.SH' AND trade_date = ?",
            [latest - timedelta(days=5)],
        )
        store._conn.execute(
            "DELETE FROM adj_factor WHERE ts_code = '600003.SH' AND trade_date = ?",
            [latest - timedelta(days=5)],
        )
    _publish(primary, replica)
    with _client(tmp_path / "absent", primary, replica) as client:
        compare = _run(client, conditions=[
            {"key": "gt", "args": {"left": "MA7[2]", "right": 9}},
        ])
        interval = _run(client, conditions=[
            {"key": "between", "args": {"field": "MA7[2]", "low": 9, "high": 11}},
        ])
    for result in (compare, interval):
        assert result.status_code == 200, result.text
        assert result.json()["data"]["total"] == 1
        assert result.json()["data"]["unknown_count"] == 2
        assert [row["ts_code"] for row in result.json()["data"]["rows"]] == ["600001.SH"]


def test_dynamic_ma_screen_runs_with_offsets_and_keeps_missing_stock_history_unknown(
    tmp_path: Path,
) -> None:
    primary, replica, latest = _replica_world(tmp_path, days=34)
    with DuckDBStore(primary) as store:
        store._conn.execute(
            "INSERT INTO adj_factor (ts_code, trade_date, adj_factor) "
            "SELECT ts_code, trade_date, 1 FROM daily_bar"
        )
        store._conn.execute(
            "UPDATE daily_bar SET close = 20 WHERE ts_code = '600001.SH' AND trade_date IN (?, ?)",
            [latest - timedelta(days=2), latest - timedelta(days=30)],
        )
        store._conn.execute(
            "UPDATE daily_bar SET close = 1 WHERE ts_code = '600002.SH' AND trade_date = ?",
            [latest - timedelta(days=30)],
        )
        store._conn.execute(
            "DELETE FROM adj_factor WHERE ts_code = '600003.SH' AND trade_date = ?",
            [latest - timedelta(days=5)],
        )
    _publish(primary, replica)
    with _client(tmp_path / "absent", primary, replica) as client:
        above = _run(client, conditions=[
            {"key": "above_ma", "args": {"period": 7, "offset": 2}},
        ])
        up = _run(client, conditions=[
            {"key": "cross_above", "args": {"fast": 2, "slow": 3, "offset": 30}},
        ])
        down = _run(client, conditions=[
            {"key": "cross_below", "args": {"fast": 2, "slow": 3, "offset": 30}},
        ])
        legacy_up = _run(client, conditions=[
            {"key": "cross_above", "args": {"fast": "MA2", "slow": "MA3", "offset": 30}},
        ])

    for response, code in ((above, "600001.SH"), (up, "600001.SH"), (down, "600002.SH")):
        assert response.status_code == 200, response.text
        assert response.json()["data"]["total"] == 1
        assert [row["ts_code"] for row in response.json()["data"]["rows"]] == [code]
    assert above.json()["data"]["unknown_count"] == 1
    assert legacy_up.status_code == 200, legacy_up.text
    assert legacy_up.json()["data"]["rows"] == up.json()["data"]["rows"]

    with DuckDBStore(primary) as store:
        store._conn.execute(
            "DELETE FROM adj_factor WHERE trade_date = ?", [latest - timedelta(days=5)],
        )
    _publish(primary, replica)
    with _client(tmp_path / "absent", primary, replica) as client:
        unavailable = _run(client, conditions=[
            {"key": "above_ma", "args": {"period": 7, "offset": 2}},
        ])
    assert unavailable.status_code == 503
    assert unavailable.json()["detail"] == "所选日期的数据不完整，请换日期或稍后重试。"


@pytest.mark.parametrize("condition", [
    {"key": "above_ma", "args": {"period": 1}},
    {"key": "above_ma", "args": {"period": 251}},
    {"key": "above_ma", "args": {"period": 7.5}},
    {"key": "above_ma", "args": {"period": True}},
    {"key": "above_ma", "args": {"period": "07"}},
    {"key": "cross_above", "args": {"fast": "MA251", "slow": 20}},
    {"key": "cross_below", "args": {"fast": "MA7[0]", "slow": 20}},
    {"key": "cross_above", "args": {"fast": [], "slow": 20}},
    {"key": "cross_above", "args": {"fast": 2, "slow": 20, "offset": 31}},
    {"key": "rsi_oversold", "args": {"period": 7, "threshold": 30}},
    {"key": "gt", "args": {"left": "MA1[0]", "right": "CLOSE[0]"}},
    {"key": "gt", "args": {"left": "MA251[0]", "right": "CLOSE[0]"}},
    {"key": "gt", "args": {"left": "MA07[0]", "right": "CLOSE[0]"}},
    {"key": "gt", "args": {"left": "MA7[00]", "right": "CLOSE[0]"}},
    {"key": "gt", "args": {"left": "MA7[31]", "right": "CLOSE[0]"}},
    {"key": "gt", "args": {"left": "MA999999999999999999999[0]", "right": "CLOSE[0]"}},
    {"key": "gt", "args": {"left": "MA7[0];DROP", "right": "CLOSE[0]"}},
    {"key": "lt", "args": {"left": "CLOSE[0]", "right": "RSI7[0]"}},
    {"key": "gte", "args": {"left": "CLOSE[1]", "right": 9}},
    {"key": "lte", "args": {"left": True, "right": "MA7[0]"}},
    {"key": "between", "args": {"field": "MA7", "low": 0, "high": 10}},
    {"key": "between", "args": {"field": "MA7[31]", "low": 0, "high": 10}},
])
def test_replica_rejects_unlisted_or_malformed_indicator_requests(
    tmp_path: Path, condition: dict,
) -> None:
    primary, replica, _ = _replica_world(tmp_path)
    with _client(tmp_path / "absent", primary, replica) as client:
        result = _run(client, conditions=[condition])
    assert result.status_code == 422
    assert "Traceback" not in result.text


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
            assert data["steps"] == [
                {"label": block["label"], "count": data["total"], "unknown_count": 0}
            ]


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


def test_replica_twenty_day_adjusted_return_ranks_across_pages(
    tmp_path: Path,
) -> None:
    primary, replica, latest = _replica_world(tmp_path, days=21)
    with DuckDBStore(primary) as store:
        store._conn.execute(
            "INSERT INTO adj_factor (ts_code, trade_date, adj_factor) "
            "SELECT ts_code, trade_date, CASE "
            "WHEN ts_code = '600001.SH' AND trade_date = ? THEN 2 "
            "WHEN ts_code = '600002.SH' AND trade_date = ? THEN 2 "
            "ELSE 1 END FROM daily_bar",
            [latest - timedelta(days=20), latest],
        )
    _publish(primary, replica)
    ranking = {
        "conditions": [{"metric": "RETURN_20D_PCT[0]", "ascending": False, "weight": 100}],
        "top_n": 3,
    }
    with _client(tmp_path / "absent", primary, replica) as client:
        catalog = client.get("/api/v1/screen/blocks").json()["data"]
        first = _run(client, ranking=ranking)
        second = _run(client, ranking=ranking, cursor=first.json()["data"]["next_cursor"])

    assert "RETURN_20D_PCT[0]" in {item["value"] for item in catalog["ranking_metrics"]}
    assert first.status_code == second.status_code == 200
    identity = catalog["source"]["identity"]
    assert first.json()["data"]["source"]["identity"] == identity
    assert second.json()["data"]["source"]["identity"] == identity
    assert [row["ts_code"] for row in first.json()["data"]["rows"]] == [
        "600002.SH", "600003.SH",
    ]
    assert [row["ts_code"] for row in second.json()["data"]["rows"]] == ["600001.SH"]
    assert [row["rank_position"] for row in first.json()["data"]["rows"]] == [1, 2]
    assert second.json()["data"]["rows"][0]["rank_position"] == 3


def test_replica_twenty_day_return_with_missing_stock_history_stays_rankable(
    tmp_path: Path,
) -> None:
    primary, replica, latest = _replica_world(tmp_path, days=21)
    with DuckDBStore(primary) as store:
        store._conn.execute(
            "INSERT INTO adj_factor (ts_code, trade_date, adj_factor) "
            "SELECT ts_code, trade_date, 1 FROM daily_bar "
            "WHERE ts_code != '600001.SH' OR trade_date != ?",
            [latest - timedelta(days=5)],
        )
    _publish(primary, replica)
    with _client(tmp_path / "absent", primary, replica) as client:
        response = _run(client, ranking={
            "conditions": [{"metric": "RETURN_20D_PCT[0]", "ascending": False, "weight": 100}],
            "top_n": 3,
        }, page_size=3)

    assert response.status_code == 200, response.text
    assert [row["ts_code"] for row in response.json()["data"]["rows"]] == [
        "600002.SH", "600003.SH", "600001.SH",
    ]
    assert response.json()["data"]["rows"][-1]["ranking_score"] == 0


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
