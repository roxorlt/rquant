"""monitor 模块单测。"""

from __future__ import annotations

from datetime import date, datetime
from unittest.mock import patch

import pandas as pd
import pytest

from rquant.storage.duckdb import DuckDBStore


@pytest.fixture()
def store(tmp_path):
    s = DuckDBStore(tmp_path / "test.duckdb")
    yield s
    s.close()


class TestBuildWatchlist:
    def test_loads_pool2_active(self, store: DuckDBStore) -> None:
        from rquant.monitor import build_watchlist

        p2 = pd.DataFrame([{
            "ts_code": "002415.SZ",
            "entry_date": date(2026, 4, 18),
            "limit_up_date": date(2026, 4, 17),
            "body_upper": 13.20, "body_lower": 11.80,
            "level_40": 12.36, "level_30": 12.22, "level_20": 12.08,
            "stop_strong": 11.80, "stop_weak": 11.52,
            "status": "active",
        }])
        store.upsert_pool2_watch(p2)

        items = build_watchlist(store)
        assert len(items) == 1
        assert items[0].ts_code == "002415.SZ"
        assert items[0].pool == "pool2"
        assert items[0].level_40 == 12.36

    def test_loads_pool1_with_level_calc(self, store: DuckDBStore) -> None:
        from rquant.monitor import build_watchlist

        # Pool 1 screen_result from yesterday
        sr = pd.DataFrame([{
            "trade_date": "2026-04-21",
            "preset_name": "n-shape-pool1",
            "ts_code": "300001.SZ",
            "name": "特锐德", "close": 15.0, "pct_chg": 3.0, "extra": None,
        }])
        store.upsert_screen_result(sr)

        # daily_bar (so _get_latest_screen_date can find it)
        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('300001.SZ', '2026-04-21', 14,16,13,15,14,1,3,500,5000)"
        )

        # daily_state: limit-up was 04-20
        store._conn.execute(
            """
            INSERT INTO daily_state VALUES
            ('300001.SZ', '2026-04-20', false, false, 'gem', 0.20,
             18.0, 12.0, true, false, true, false, 1, 16.50, 14.80)
            """
        )

        items = build_watchlist(store, screen_date="2026-04-21")
        assert len(items) == 1
        assert items[0].pool == "pool1"
        body = 16.50 - 14.80
        assert items[0].level_40 == pytest.approx(14.80 + body * 0.4)

    def test_dedup_pool2_wins(self, store: DuckDBStore) -> None:
        from rquant.monitor import build_watchlist

        # Same stock in both Pool 1 and Pool 2
        p2 = pd.DataFrame([{
            "ts_code": "002415.SZ",
            "entry_date": date(2026, 4, 18),
            "limit_up_date": date(2026, 4, 17),
            "body_upper": 13.20, "body_lower": 11.80,
            "level_40": 12.36, "level_30": 12.22, "level_20": 12.08,
            "stop_strong": 11.80, "stop_weak": 11.52,
            "status": "active",
        }])
        store.upsert_pool2_watch(p2)

        sr = pd.DataFrame([{
            "trade_date": "2026-04-21",
            "preset_name": "n-shape-pool1",
            "ts_code": "002415.SZ",
            "name": "海康威视", "close": 12.50, "pct_chg": -2.0, "extra": None,
        }])
        store.upsert_screen_result(sr)

        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('002415.SZ', '2026-04-21', 12,13,11,12.5,12,0.5,5,1000,10000)"
        )

        items = build_watchlist(store, screen_date="2026-04-21")
        assert len(items) == 1
        assert items[0].pool == "pool2"


class TestIsTradingDay:
    @patch("rquant.monitor.ak")
    def test_trading_day_returns_true(self, mock_ak) -> None:
        from rquant.monitor import is_trading_day

        mock_ak.tool_trade_date_hist_sina.return_value = pd.DataFrame(
            {"trade_date": ["2026-04-21", "2026-04-22"]}
        )
        assert is_trading_day(date(2026, 4, 21)) is True

    @patch("rquant.monitor.ak")
    def test_non_trading_day_returns_false(self, mock_ak) -> None:
        from rquant.monitor import is_trading_day

        mock_ak.tool_trade_date_hist_sina.return_value = pd.DataFrame(
            {"trade_date": ["2026-04-21", "2026-04-22"]}
        )
        assert is_trading_day(date(2026, 4, 19)) is False


class TestFetchRealtimePrices:
    @patch("rquant.monitor.ak")
    def test_returns_price_and_low(self, mock_ak) -> None:
        from rquant.monitor import fetch_realtime_prices

        # sina 源：代码带前缀 sh/sz/bj
        mock_ak.stock_zh_a_spot.return_value = pd.DataFrame({
            "代码": ["sz002415", "sz300001", "sh600000"],
            "最新价": [12.35, 15.00, 8.50],
            "最低": [12.10, 14.80, 8.30],
        })
        result = fetch_realtime_prices(["002415.SZ", "300001.SZ"])
        assert "002415.SZ" in result
        assert result["002415.SZ"]["price"] == 12.35
        assert result["002415.SZ"]["low"] == 12.10
        assert "300001.SZ" in result
        assert "600000.SH" not in result

    @patch("rquant.monitor.ak")
    def test_missing_stock_skipped(self, mock_ak) -> None:
        from rquant.monitor import fetch_realtime_prices

        mock_ak.stock_zh_a_spot.return_value = pd.DataFrame({
            "代码": ["sh600000"],
            "最新价": [8.50],
            "最低": [8.30],
        })
        result = fetch_realtime_prices(["002415.SZ"])
        assert result == {}

    @patch("rquant.monitor.ak")
    def test_handles_bj_prefix(self, mock_ak) -> None:
        """北交所代码 bj920xxx 也能识别。"""
        from rquant.monitor import fetch_realtime_prices

        mock_ak.stock_zh_a_spot.return_value = pd.DataFrame({
            "代码": ["bj920001"],
            "最新价": [14.18],
            "最低": [14.09],
        })
        result = fetch_realtime_prices(["920001.BJ"])
        assert result["920001.BJ"]["price"] == 14.18


class TestCheckLevels:
    def _make_item(self) -> "WatchItem":
        from rquant.monitor import WatchItem
        return WatchItem(
            ts_code="002415.SZ", pool="pool2",
            limit_up_date=date(2026, 4, 17),
            body_upper=13.20, body_lower=11.80, body=1.40,
            level_40=12.36, level_30=12.22, level_20=12.08,
            stop_strong=11.80, stop_weak=11.52,
        )

    def test_no_trigger_above_all_levels(self) -> None:
        from rquant.monitor import check_levels
        item = self._make_item()
        events = check_levels(item, current_price=12.50, daily_low=12.50)
        assert events == []

    def test_triggers_40_level(self) -> None:
        from rquant.monitor import check_levels
        item = self._make_item()
        events = check_levels(item, current_price=12.30, daily_low=12.30)
        assert len(events) == 1
        assert events[0]["level"] == "40"
        assert events[0]["trigger_type"] == "realtime"
        assert item.triggered["40"] is True

    def test_triggers_multiple_levels(self) -> None:
        from rquant.monitor import check_levels
        item = self._make_item()
        events = check_levels(item, current_price=12.00, daily_low=12.00)
        triggered_levels = {e["level"] for e in events}
        assert "40" in triggered_levels
        assert "30" in triggered_levels
        assert "20" in triggered_levels

    def test_daily_low_backup_trigger(self) -> None:
        from rquant.monitor import check_levels
        item = self._make_item()
        # Price bounced back above 40, but daily low touched it
        events = check_levels(item, current_price=12.50, daily_low=12.30)
        assert len(events) == 1
        assert events[0]["trigger_type"] == "daily_low"

    def test_no_retrigger(self) -> None:
        from rquant.monitor import check_levels
        item = self._make_item()
        check_levels(item, current_price=12.30, daily_low=12.30)
        assert item.triggered["40"] is True

        events2 = check_levels(item, current_price=12.30, daily_low=12.30)
        assert events2 == []

    def test_strong_stop_trigger(self) -> None:
        from rquant.monitor import check_levels
        item = self._make_item()
        events = check_levels(item, current_price=11.75, daily_low=11.75)
        levels = {e["level"] for e in events}
        assert "strong" in levels


class TestCheckExits:
    def test_breakdown_auto_kicks(self, store: DuckDBStore) -> None:
        from rquant.monitor import check_exits

        p2 = pd.DataFrame([{
            "ts_code": "002415.SZ",
            "entry_date": date(2026, 4, 18),
            "limit_up_date": date(2026, 4, 17),
            "body_upper": 13.20, "body_lower": 11.80,
            "level_40": 12.36, "level_30": 12.22, "level_20": 12.08,
            "stop_strong": 11.80, "stop_weak": 11.52,
            "status": "active",
        }])
        store.upsert_pool2_watch(p2)

        # Close price below stop_weak
        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('002415.SZ', '2026-04-21', 12,12,11.5,11.40,12,0,0,1000,10000)"
        )

        with patch("rquant.notify.notify") as mock_notify:
            kicked = check_exits(store, date(2026, 4, 21))

        assert kicked == 1
        active = store.query_pool2_active()
        assert len(active) == 0

        # Verify pool2_exit notification fired with auto_kicked
        mock_notify.assert_called_once()
        scene = mock_notify.call_args.args[0]
        assert scene == "pool2_exit"
        kwargs = mock_notify.call_args.kwargs
        assert len(kwargs["auto_kicked"]) == 1
        assert kwargs["auto_kicked"][0]["ts_code"] == "002415.SZ"
        assert kwargs["auto_kicked"][0]["reason_label"] == "弱止"
        assert kwargs["expired_held"] == []

    def test_expired_held_active(self, store: DuckDBStore) -> None:
        """超期不再自动踢出，保留 active 加入待决策列表。"""
        from rquant.monitor import check_exits

        p2 = pd.DataFrame([{
            "ts_code": "002415.SZ",
            "entry_date": date(2026, 4, 16),  # 3+ trading days ago
            "limit_up_date": date(2026, 4, 15),
            "body_upper": 13.20, "body_lower": 11.80,
            "level_40": 12.36, "level_30": 12.22, "level_20": 12.08,
            "stop_strong": 11.80, "stop_weak": 11.52,
            "status": "active",
        }])
        store.upsert_pool2_watch(p2)

        # Close above stop levels but 3+ days old
        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('002415.SZ', '2026-04-16', 12,13,11,12,11,1,5,1000,10000),"
            "('002415.SZ', '2026-04-17', 12,13,11,12,11,1,5,1000,10000),"
            "('002415.SZ', '2026-04-18', 12,13,11,12,11,1,5,1000,10000),"
            "('002415.SZ', '2026-04-21', 12,13,11,12.5,12,0.5,5,1000,10000)"
        )

        with patch("rquant.notify.notify") as mock_notify:
            kicked = check_exits(store, date(2026, 4, 21))

        assert kicked == 0
        active = store.query_pool2_active()
        assert len(active) == 1  # 仍 active

        # Verify expired_held in notification
        mock_notify.assert_called_once()
        kwargs = mock_notify.call_args.kwargs
        assert kwargs["auto_kicked"] == []
        assert len(kwargs["expired_held"]) == 1
        assert kwargs["expired_held"][0]["ts_code"] == "002415.SZ"

    def test_no_events_no_notify(self, store: DuckDBStore) -> None:
        """无任何退出事件时不推送。"""
        from rquant.monitor import check_exits

        p2 = pd.DataFrame([{
            "ts_code": "002415.SZ",
            "entry_date": date(2026, 4, 20),  # only 1 day old
            "limit_up_date": date(2026, 4, 19),
            "body_upper": 13.20, "body_lower": 11.80,
            "level_40": 12.36, "level_30": 12.22, "level_20": 12.08,
            "stop_strong": 11.80, "stop_weak": 11.52,
            "status": "active",
        }])
        store.upsert_pool2_watch(p2)

        # Close well above stops, not expired
        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('002415.SZ', '2026-04-21', 12,13,12,12.50,12,0.5,5,1000,10000)"
        )

        with patch("rquant.notify.notify") as mock_notify:
            kicked = check_exits(store, date(2026, 4, 21))

        assert kicked == 0
        mock_notify.assert_not_called()


class TestWaitForMarketOpen:
    @patch("rquant.monitor.time.sleep")
    @patch("rquant.monitor._now")
    def test_sleeps_until_open_when_just_before(self, mock_now, mock_sleep) -> None:
        from rquant.monitor import _wait_for_market_open

        mock_now.return_value = datetime(2026, 4, 28, 9, 29, 0)
        _wait_for_market_open()

        mock_sleep.assert_called_once()
        slept = mock_sleep.call_args[0][0]
        assert slept == pytest.approx(60, abs=1)

    @patch("rquant.monitor.time.sleep")
    @patch("rquant.monitor._now")
    def test_no_sleep_after_open(self, mock_now, mock_sleep) -> None:
        from rquant.monitor import _wait_for_market_open

        mock_now.return_value = datetime(2026, 4, 28, 10, 0, 0)
        _wait_for_market_open()
        mock_sleep.assert_not_called()

    @patch("rquant.monitor.time.sleep")
    @patch("rquant.monitor._now")
    def test_no_sleep_when_too_early(self, mock_now, mock_sleep) -> None:
        from rquant.monitor import _wait_for_market_open

        # 7 AM boot - don't sleep 2.5h, just exit
        mock_now.return_value = datetime(2026, 4, 28, 7, 0, 0)
        _wait_for_market_open()
        mock_sleep.assert_not_called()

    @patch("rquant.monitor.time.sleep")
    @patch("rquant.monitor._now")
    def test_no_sleep_at_exactly_open(self, mock_now, mock_sleep) -> None:
        from rquant.monitor import _wait_for_market_open

        mock_now.return_value = datetime(2026, 4, 28, 9, 30, 0)
        _wait_for_market_open()
        mock_sleep.assert_not_called()


class TestRunMonitor:
    @patch("rquant.monitor.is_trading_day", return_value=False)
    def test_exits_on_non_trading_day(self, _mock) -> None:
        from rquant.monitor import run_monitor
        result = run_monitor(interval=5)
        assert result == 0

    @patch("rquant.monitor._count_trading_days_since", return_value=4)
    @patch("rquant.monitor.check_exits", return_value=0)
    @patch("rquant.monitor.fetch_realtime_prices")
    @patch("rquant.monitor.build_watchlist")
    @patch("rquant.monitor.is_trading_day", return_value=True)
    @patch("rquant.monitor._is_trading_hours")
    @patch("rquant.monitor._now")
    def test_polls_and_detects(
        self, mock_now, mock_hours, _td, mock_build, mock_fetch, _exits,
        _count_days,
    ) -> None:
        from rquant.monitor import WatchItem, run_monitor

        item = WatchItem(
            ts_code="002415.SZ", pool="pool2",
            limit_up_date=date(2026, 4, 17),
            body_upper=13.20, body_lower=11.80, body=1.40,
            level_40=12.36, level_30=12.22, level_20=12.08,
            stop_strong=11.80, stop_weak=11.52,
            name="海康威视",
            entry_date=date(2026, 4, 18),
        )
        mock_build.return_value = [item]
        mock_fetch.return_value = {
            "002415.SZ": {"price": 12.30, "low": 12.30}
        }

        # First call: trading hours. Second call: after close.
        mock_hours.side_effect = [True, False]
        mock_now.return_value = datetime(2026, 4, 21, 10, 0, 0)

        with patch("rquant.notify.notify") as mock_notify:
            with patch("rquant.monitor.DuckDBStore") as MockStore:
                mock_store = MockStore.return_value.__enter__.return_value
                mock_store.upsert_monitor_event.return_value = 1
                mock_store.query_monitor_events.return_value = pd.DataFrame()
                run_monitor(interval=5)

        assert item.triggered["40"] is True

        # heartbeat start + price_level (40) + heartbeat stop = 3 calls
        scenes = [c.args[0] for c in mock_notify.call_args_list]
        assert "heartbeat" in scenes
        assert "price_level" in scenes
        assert scenes.count("heartbeat") == 2  # start + stop
