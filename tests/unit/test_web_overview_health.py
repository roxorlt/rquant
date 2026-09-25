"""``GET /api/v1/overview`` and ``GET /api/v1/health`` over the synthetic fixture."""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from rquant.web.app import create_app
from rquant.web.settings import WebSettings
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture

#: 2026-09-24 15:31:30 in Shanghai: after the close of an open day.
AFTER_CLOSE = FIXTURE_BUILT_AT + timedelta(seconds=30)
#: 2026-09-24 10:00 in Shanghai: continuous trading.
MID_SESSION = datetime(2026, 9, 24, 2, 0, tzinfo=UTC)
#: 2026-09-25 10:00 in Shanghai: 中秋, closed.
HOLIDAY = datetime(2026, 9, 25, 2, 0, tzinfo=UTC)
_SHA256 = re.compile(r"\b[0-9a-f]{64}\b")


def _get(root: Path, path: str, now: datetime, *, stale_after: float = 600.0) -> dict:
    app = create_app(
        WebSettings(serving_root=root, stale_after_seconds=stale_after),
        clock=lambda: now,
        background=False,
    )
    with TestClient(app) as client:
        response = client.get(path)
    assert response.status_code == 200, response.text
    assert response.headers["cache-control"] == "no-store"
    return response.json()


@pytest.fixture(scope="module")
def baseline(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("baseline") / "serving"
    build_web_fixture(root, "baseline")
    return root


@pytest.fixture(scope="module")
def degraded(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("degraded") / "serving"
    build_web_fixture(root, "degraded")
    return root


# ------------------------------------------------------------------ overview


def test_overview_after_the_close_shows_today(baseline: Path) -> None:
    body = _get(baseline, "/api/v1/overview", AFTER_CLOSE)
    data = body["data"]

    assert body["serving"]["state"] == "ready"
    assert data["session"] == {
        "today": "2026-09-24",
        "trade_date": "2026-09-24",
        "is_today": True,
        "phase": "after_close",
        "next_trading_day": "2026-09-28",
    }
    assert [stage["key"] for stage in data["pipeline"]] == [
        "reference",
        "auction",
        "signals",
        "paper",
        "notify",
    ]
    assert {stage["state"] for stage in data["pipeline"]} == {"done"}
    signals = data["signals"]
    assert signals["total"] == 2
    assert [item["code"] for item in signals["items"]] == ["600001.SH", "600003.SH"]
    first = signals["items"][0]
    assert (first["name"], first["strategy_name"], first["action_label"]) == (
        "样本01",
        "N 字",
        "买入意向",
    )
    assert first["delivery"] == "delivered"
    assert signals["items"][1]["delivery"] == "sending"
    assert data["deliveries"] == {
        "total": 2,
        "delivered": 1,
        "sending": 1,
        "failed": 0,
        "expired": 0,
    }
    groups = {group["key"]: group for group in data["candidates"]["groups"]}
    assert groups["screen:n-shape-pool1"]["count"] == 3
    assert groups["screen:n-shape-pool1"]["as_of"] == "2026-09-23"
    assert groups["screen:n-shape-pool2"]["name"] == "N 字二池"
    assert groups["signals:auction_gap"]["count"] == 1
    assert data["candidates"]["total"] == 7
    paper = data["paper"]
    assert paper["account_id"] == "shadow-main"
    assert [holding["code"] for holding in paper["holdings"]] == ["600005.SH", "600001.SH"]
    assert paper["holdings"][0]["name"] == "样本05"
    assert paper["holdings"][0]["unrealized_pnl"] == pytest.approx(42.0)
    assert paper["note"] is None
    assert data["services"]["total"] == 7


def test_overview_mid_session_is_running(baseline: Path) -> None:
    data = _get(baseline, "/api/v1/overview", MID_SESSION, stale_after=1e9)["data"]

    states = {stage["key"]: stage["state"] for stage in data["pipeline"]}
    assert states == {
        "reference": "done",
        "auction": "done",
        "signals": "running",
        "paper": "running",
        "notify": "running",
    }
    assert data["signals"]["total"] == 2


def test_overview_on_a_holiday_shows_the_last_trading_day(baseline: Path) -> None:
    body = _get(baseline, "/api/v1/overview", HOLIDAY, stale_after=1e9)
    data = body["data"]

    assert data["session"] == {
        "today": "2026-09-25",
        "trade_date": "2026-09-24",
        "is_today": False,
        "phase": "non_trading_day",
        "next_trading_day": "2026-09-28",
    }
    assert data["signals"]["total"] == 2
    assert {stage["state"] for stage in data["pipeline"]} == {"done"}
    # A market-hours service with no heartbeat on a holiday is expected, not a fault.
    assert data["services"]["waiting"] == 1
    assert not any("竞价撮合" in item["title"] for item in data["attention"])


def test_overview_attention_lists_what_needs_a_look(baseline: Path) -> None:
    attention = _get(baseline, "/api/v1/overview", AFTER_CLOSE)["data"]["attention"]

    titles = [item["title"] for item in attention]
    assert "通知推送需要注意" in titles
    assert all(item["to"] == "/health" for item in attention)
    assert all(not _SHA256.search(item["title"] + item["reason"]) for item in attention)


def test_a_stale_generation_is_the_first_thing_to_look_at(baseline: Path) -> None:
    body = _get(baseline, "/api/v1/overview", FIXTURE_BUILT_AT + timedelta(minutes=30))

    assert body["serving"]["state"] == "stale"
    first = body["data"]["attention"][0]
    assert first["level"] == "crit"
    assert first["title"] == "页面数据没有按时更新"
    assert first["reason"] == body["serving"]["message"]


def test_overview_and_health_without_serving_are_empty_and_unavailable(tmp_path: Path) -> None:
    root = tmp_path / "absent"

    overview = _get(root, "/api/v1/overview", AFTER_CLOSE)
    health = _get(root, "/api/v1/health", AFTER_CLOSE)

    assert overview["serving"]["state"] == "unavailable"
    assert overview["data"]["session"]["trade_date"] is None
    assert overview["data"]["signals"]["items"] == []
    assert overview["data"]["attention"][0]["title"] == "页面数据没有按时更新"
    assert health["serving"]["state"] == "unavailable"
    assert health["data"]["services"] == []
    assert health["data"]["page_data"]["status"]["state"] == "crit"
    assert not root.exists()


# ------------------------------------------------------------------ health


def _services(body: dict) -> dict[str, dict]:
    return {item["service_id"]: item for item in body["data"]["services"]}


def test_health_names_services_in_plain_words_and_keeps_ids_for_tooltips(
    baseline: Path,
) -> None:
    body = _get(baseline, "/api/v1/health", AFTER_CLOSE)
    services = _services(body)

    notifier = services["notifier.admin.shadow.v1"]
    assert notifier["name"] == "通知推送"
    assert notifier["plane_label"] == "盘中"
    assert notifier["status"] == {
        "state": "warn",
        "label": "注意",
        "reason": "影子模式：只记录，不真正推送",
    }
    assert services["paper-broker.shadow-main.v1"]["name"] == "模拟撮合"
    assert services["paper-broker.shadow-main.v1"]["status"]["label"] == "正常"
    assert services["serving-publisher.primary.v1"]["plane_label"] == "页面数据"
    lab = services["lab-jobs.serving.v1"]
    assert (lab["name"], lab["status"]["state"], lab["status"]["label"]) == (
        "研究任务",
        "idle",
        "未运行",
    )
    # After the close a market-hours service without a heartbeat has simply stopped.
    auction = services["auction-match.source.v1"]
    assert (auction["status"]["state"], auction["status"]["label"]) == ("waiting", "已收盘")
    assert body["data"]["counts"] == {
        "total": 7,
        "ok": 4,
        "warn": 1,
        "crit": 0,
        "idle": 1,
        "waiting": 1,
    }
    # Worst first.
    assert body["data"]["services"][0]["service_id"] == "notifier.admin.shadow.v1"


def test_mid_session_a_missing_market_service_is_not_running(baseline: Path) -> None:
    services = _services(_get(baseline, "/api/v1/health", MID_SESSION, stale_after=1e9))

    auction = services["auction-match.source.v1"]
    assert (auction["status"]["state"], auction["status"]["label"]) == ("idle", "未运行")


def test_health_freshness_rows_use_plain_states(baseline: Path) -> None:
    body = _get(baseline, "/api/v1/health", AFTER_CLOSE)
    rows = {item["key"]: item for item in body["data"]["freshness"]}

    assert rows["signals"]["name"] == "盘中信号"
    assert rows["signals"]["status"]["label"] == "按时"
    assert rows["trade_calendar"]["latest_date"] == "2026-12-31"
    assert rows["trade_calendar"]["status"]["state"] == "ok"
    # The fixture's screen is from 09-23; at 15:31 on 09-24 today's screen is not due yet.
    assert rows["canvas_latest_trade_date"]["status"]["label"] == "按时"
    assert rows["minute_coverage"]["latest_date"] == "2026-09-24"
    assert rows["minute_coverage"]["status"]["label"] == "按时"


def test_daily_data_is_late_once_its_evening_job_is_due(baseline: Path) -> None:
    # 2026-09-24 19:00 in Shanghai: today's screen and daily bars are due (18:30).
    body = _get(
        baseline, "/api/v1/health", datetime(2026, 9, 24, 11, 0, tzinfo=UTC), stale_after=1e9
    )
    rows = {item["key"]: item for item in body["data"]["freshness"]}

    screen = rows["canvas_latest_trade_date"]["status"]
    assert (screen["state"], screen["label"]) == ("warn", "延迟")
    assert screen["reason"].startswith("落后 1 个交易日")


def test_degraded_watermarks_become_plain_rows_not_a_banner(degraded: Path) -> None:
    body = _get(degraded, "/api/v1/health", AFTER_CLOSE)
    rows = {item["key"]: item for item in body["data"]["freshness"]}

    assert body["serving"]["state"] == "ready"
    assert rows["runtime_health"]["status"]["label"] == "按时"
    assert rows["lab_jobs"]["status"] == {
        "state": "idle",
        "label": "未发布",
        "reason": "研究任务服务没有运行，暂时没有数据",
    }
    assert rows["lab_jobs"]["latest_at"] is None
    page = body["data"]["page_data"]
    unpublished = {item["key"]: item["name"] for item in page["unpublished"]}
    assert unpublished["dashboard_summary"] == "运维摘要"
    assert unpublished["minute_coverage"] == "分钟线覆盖"
    assert page["status"]["label"] == "正常"


def test_page_data_goes_red_when_the_generation_is_old(baseline: Path) -> None:
    page = _get(baseline, "/api/v1/health", FIXTURE_BUILT_AT + timedelta(hours=2))["data"][
        "page_data"
    ]

    assert page["status"]["state"] == "crit"
    assert page["status"]["reason"] == "已 2 小时没有更新"
    assert page["age_seconds"] == 7200.0


def test_replayed_production_errors_are_summarised_with_the_raw_text_kept() -> None:
    from rquant.dashboard.runtime_console_data import RuntimeServiceRow
    from rquant.web.market import MarketPhase
    from rquant.web.routes.health import error_items, service_item

    row = RuntimeServiceRow(
        service_id="reference-slow.publisher.v1",
        plane="live",
        status="degraded",
        stale=False,
        observed_at=AFTER_CLOSE,
        heartbeat_at=AFTER_CLOSE,
        input_sequence=0,
        output_sequence=0,
        backlog_count=0,
        consecutive_failures=239,
        last_error="ReferenceSlowRuntimeError: reference slow publisher started after 09:25",
    )
    item = service_item(row, phase=MarketPhase.CONTINUOUS, now=AFTER_CLOSE)

    assert item.name == "参考数据发布"
    assert (item.status.state, item.status.label, item.status.reason) == (
        "crit",
        "异常",
        "连续失败 239 次",
    )
    [error] = error_items([item])
    assert error.summary == "连续失败 239 次"
    assert error.message.startswith("ReferenceSlowRuntimeError")
