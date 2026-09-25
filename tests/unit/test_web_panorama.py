"""``/api/v1/panorama/*`` over the synthetic panorama generation."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from rquant.web import panorama
from rquant.web.app import create_app
from rquant.web.settings import WebSettings
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture

#: 2026-09-24 15:31:30 in Shanghai, after the close.
AFTER_CLOSE = FIXTURE_BUILT_AT + timedelta(seconds=30)


@pytest.fixture(scope="module")
def pano(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("panorama") / "serving"
    build_web_fixture(root, "panorama")
    return root


@pytest.fixture(scope="module")
def baseline(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("baseline") / "serving"
    build_web_fixture(root, "baseline")
    return root


def _get(root: Path, path: str, now: datetime = AFTER_CLOSE, status: int = 200) -> dict:
    app = create_app(
        WebSettings(serving_root=root, stale_after_seconds=1e9),
        clock=lambda: now,
        background=False,
    )
    with TestClient(app) as client:
        response = client.get(path)
    assert response.status_code == status, response.text
    return response.json()


# ------------------------------------------------------------------ session axis


@pytest.mark.parametrize(
    ("clock", "slot"),
    (
        ("09:30", 0),
        ("09:31", 1),
        ("11:29", 119),
        ("11:30", 120),
        ("13:00", 120),
        ("13:01", 121),
        ("14:59", 239),
        ("15:00", 240),
        ("09:29", None),
        ("12:00", None),
        ("15:01", None),
    ),
)
def test_the_session_axis_has_241_points_with_the_noon_break_shared(
    clock: str, slot: int | None
) -> None:
    hour, minute = (int(part) for part in clock.split(":"))
    assert panorama.session_slot(datetime(2026, 9, 24, hour, minute)) == slot


# ------------------------------------------------------------------ pulse


def test_the_pulse_is_computed_from_the_snapshot_with_derived_limit_prices(pano: Path) -> None:
    data = _get(pano, "/api/v1/panorama/pulse")["data"]

    # The projection has no limit prices; without deriving them every count would be 0.
    assert data["source"] == "snapshot"
    assert data["counts"]["limit_up"] == 2
    assert data["counts"]["broken"] == 1
    assert data["counts"]["total"] == 30
    assert data["trade_date"] == "2026-09-24"
    assert (data["freshness"]["state"], data["freshness"]["label"]) == ("idle", "收盘数据")
    assert len(data["history"]) == 120
    assert data["history"][0] == {
        "t": "09:30",
        "limit_up": 20,
        "broken": 2,
        "limit_down": 3,
        "up_ratio_pct": 48.0,
    }
    assert [alert["kind_label"] for alert in data["alerts"]] == ["炸板潮"]
    # 14:20's alert is more than 30 minutes old at 15:31.
    assert data["recent_alert"] is None


def test_a_recent_alert_is_surfaced_within_30_minutes(pano: Path) -> None:
    # 2026-09-24 14:35 in Shanghai.
    data = _get(pano, "/api/v1/panorama/pulse", datetime(2026, 9, 24, 6, 35, tzinfo=UTC))["data"]

    assert data["recent_alert"]["message"] == "炸板 10 分钟 2 → 6（+4）"
    # In session a snapshot from 15:00 is in the future at 14:35; the age is clamped at 0.
    assert data["freshness"]["label"] == "实时"


def test_a_stale_snapshot_in_session_is_flagged(pano: Path) -> None:
    # 2026-09-25 is a holiday; take the next open day, 09-28 10:00, a snapshot 3 days old.
    data = _get(pano, "/api/v1/panorama/pulse", datetime(2026, 9, 28, 2, 0, tzinfo=UTC))["data"]

    assert (data["freshness"]["state"], data["freshness"]["label"]) == ("idle", "最近交易日")
    assert data["trade_date"] == "2026-09-24"


def test_without_panorama_data_the_pulse_is_empty_not_an_error(baseline: Path) -> None:
    data = _get(baseline, "/api/v1/panorama/pulse")["data"]

    assert data["counts"] is None
    assert data["freshness"]["label"] == "暂无快照"
    assert data["history"] == []


# ------------------------------------------------------------------ boards and members


def test_kpl_boards_hide_fund_flow_and_sort_by_limit_ups(pano: Path) -> None:
    data = _get(pano, "/api/v1/panorama/boards?system=开盘啦题材")["data"]

    assert data["systems"] == ["东财行业", "东财概念", "开盘啦题材"]
    assert data["has_flow"] is False
    assert [row["board_name"] for row in data["rows"]] == ["人形机器人", "存储芯片"]
    assert all(row["main_net_amount"] is None for row in data["rows"])


def test_eastmoney_boards_carry_fund_flow(pano: Path) -> None:
    data = _get(pano, "/api/v1/panorama/boards?system=东财行业")["data"]

    first = data["rows"][0]
    assert data["has_flow"] is True
    assert (first["board_code"], first["board_name"]) == ("BK0001.DC", "半导体")
    assert first["main_net_amount"] == pytest.approx(1.23e9)
    assert first["leading_stock"] == "样本01"


def test_an_unknown_board_system_is_refused(pano: Path) -> None:
    _get(pano, "/api/v1/panorama/boards?system=nope", status=422)


def test_board_members_are_strongest_first_with_limit_up_and_pool_marks(pano: Path) -> None:
    data = _get(pano, "/api/v1/panorama/boards/000001.KP/members")["data"]
    rows = {row["ts_code"]: row for row in data["rows"]}

    assert data["board_name"] == "人形机器人"
    assert len(rows) == 6
    strengths = [row["strength"] for row in data["rows"]]
    assert strengths == sorted(strengths, reverse=True)
    assert rows["600001.SH"]["is_limit_up"] is True
    assert rows["600001.SH"]["pools"] == ["N 字一池"]
    assert rows["600005.SH"]["pools"] == ["二池盯盘"]
    assert rows["600015.SH"]["turnover_pct"] is not None


def test_an_unknown_board_has_no_members(pano: Path) -> None:
    data = _get(pano, "/api/v1/panorama/boards/NOPE.KP/members")["data"]
    assert (data["board_name"], data["rows"]) == (None, [])


# ------------------------------------------------------------------ charts


def test_the_intraday_chart_pins_bars_to_the_session_axis(pano: Path) -> None:
    data = _get(pano, "/api/v1/panorama/stocks/600001.SH/intraday")["data"]
    bars = data["bars"]

    assert data["name"] == "样本01"
    assert data["days"] == ["2026-09-24"]
    assert len(bars) == 240
    assert [bars[0]["t"], bars[0]["slot"]] == ["09:30", 0]
    assert [bars[120]["t"], bars[120]["slot"]] == ["13:00", 120]
    assert [bars[-1]["t"], bars[-1]["slot"]] == ["14:59", 239]
    assert bars[0]["direction"] == "flat"
    assert {bar["direction"] for bar in bars} == {"flat", "up", "down"}
    # The running average is estimated from closes: it starts at the first close.
    assert bars[0]["avg_price"] == pytest.approx(bars[0]["price"])
    # The fixture day's first 爆量确认 is 09:47.
    assert [(mark["t"], mark["slot"]) for mark in data["marks"]] == [("09:47", 17)]


def test_the_five_day_chart_has_five_days_and_one_mark_per_day(pano: Path) -> None:
    data = _get(pano, "/api/v1/panorama/stocks/600001.SH/intraday?days=5")["data"]

    assert data["days"] == ["2026-09-18", "2026-09-21", "2026-09-22", "2026-09-23", "2026-09-24"]
    assert len(data["bars"]) == 5 * 240
    assert [(mark["day"], mark["t"]) for mark in data["marks"]] == [
        ("2026-09-23", "10:31"),
        ("2026-09-24", "09:47"),
    ]


def test_one_day_of_history_shows_every_confirmation(pano: Path) -> None:
    data = _get(pano, "/api/v1/panorama/stocks/600001.SH/intraday?date=2026-09-24")["data"]

    assert data["days"] == ["2026-09-24"]
    assert [mark["t"] for mark in data["marks"]] == ["09:47", "09:52"]
    assert data["marks"][0]["label"] == "09:47 爆量确认 · 3.2×"


def test_a_stock_without_minute_bars_is_empty(pano: Path) -> None:
    data = _get(pano, "/api/v1/panorama/stocks/600030.SH/intraday")["data"]
    assert (data["bars"], data["marks"], data["name"]) == ([], [], "样本30")


def test_a_malformed_code_is_refused(pano: Path) -> None:
    _get(pano, "/api/v1/panorama/stocks/600001/intraday", status=422)
    _get(pano, "/api/v1/panorama/stocks/600001.SH/intraday?days=9", status=422)


def test_daily_bars_carry_moving_averages(pano: Path) -> None:
    data = _get(pano, "/api/v1/panorama/stocks/600001.SH/daily")["data"]
    bars = data["bars"]

    assert len(bars) == 120
    assert bars[3]["ma5"] is None
    assert bars[4]["ma5"] == pytest.approx(sum(bar["close"] for bar in bars[:5]) / 5)
    assert bars[19]["ma20"] is not None
    assert bars[-1]["date"] == "2026-09-24"
    assert not any(bar["provisional"] for bar in bars)


def test_todays_bar_comes_from_the_snapshot_when_daily_data_ends_yesterday() -> None:
    from rquant.web.readers import TableState

    class Cursor:
        def execute(self, _sql: str, _parameters: tuple[object, ...] = ()) -> Cursor:
            return self

        def fetchdf(self) -> pd.DataFrame:
            return pd.DataFrame(
                {
                    "trade_date": [date(2026, 9, 23)],
                    "open": [10.0],
                    "high": [10.5],
                    "low": [9.8],
                    "close": [10.2],
                    "volume": [1000.0],
                }
            )

    snapshot = pd.DataFrame(
        [
            {
                "ts_code": "600001.SH",
                "open": 10.3,
                "high": 10.9,
                "low": 10.1,
                "price": 10.8,
                "volume": 250000.0,
            }
        ]
    )
    snapshot.attrs["as_of"] = datetime(2026, 9, 24, 3, 0, tzinfo=UTC)
    tables = {"daily_bar": TableState("daily_bar", True, 1, None)}

    bars = panorama.daily_bars(Cursor(), tables, "600001.SH", snapshot)

    assert [bar.date.isoformat() for bar in bars] == ["2026-09-23", "2026-09-24"]
    today = bars[-1]
    assert today.provisional is True
    assert (today.open, today.close) == (10.3, 10.8)
    # The snapshot counts shares; daily bars count lots of 100.
    assert today.volume == 2500.0


# ------------------------------------------------------------------ surge ledger


def test_the_surge_ledger_keeps_each_stocks_first_confirmation_of_the_day(pano: Path) -> None:
    data = _get(pano, "/api/v1/panorama/surge")["data"]

    assert data["trade_date"] == "2026-09-24"
    assert data["dates"] == ["2026-09-24", "2026-09-23"]
    assert [(row["confirmed_at"], row["ts_code"]) for row in data["rows"]] == [
        ("09:47", "600001.SH"),
        ("10:12", "600003.SH"),
        ("13:05", "600010.SH"),
    ]
    assert data["rows"][0]["status_label"] == "已涨停"
    assert data["rows"][1]["status_label"] == "可买"
    assert data["config"]["boards"] == ["主板", "创业板", "科创板"]
    assert "观察提示，不是买入信号" in data["config"]["summary"]


def test_another_day_of_the_ledger(pano: Path) -> None:
    data = _get(pano, "/api/v1/panorama/surge?date=2026-09-23")["data"]
    assert [row["ts_code"] for row in data["rows"]] == ["600001.SH"]


def test_cross_day_search_by_code_or_name(pano: Path) -> None:
    by_code = _get(pano, "/api/v1/panorama/surge/search?q=600001")["data"]
    by_name = _get(pano, "/api/v1/panorama/surge/search?q=假票600003")["data"]

    assert [(row["trade_date"], row["confirmed_at"]) for row in by_code["rows"]] == [
        ("2026-09-24", "09:47"),
        ("2026-09-23", "10:31"),
    ]
    assert by_code["truncated"] is False
    assert [row["ts_code"] for row in by_name["rows"]] == ["600003.SH"]
    _get(pano, "/api/v1/panorama/surge/search?q=", status=422)


def test_every_panorama_endpoint_answers_without_serving(tmp_path: Path) -> None:
    root = tmp_path / "absent"
    for path in (
        "/api/v1/panorama/pulse",
        "/api/v1/panorama/boards",
        "/api/v1/panorama/boards/000001.KP/members",
        "/api/v1/panorama/stocks/600001.SH/intraday",
        "/api/v1/panorama/stocks/600001.SH/daily",
        "/api/v1/panorama/surge",
        "/api/v1/panorama/surge/search?q=a",
    ):
        body = _get(root, path)
        assert body["serving"]["state"] == "unavailable", path
