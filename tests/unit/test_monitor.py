"""monitor 模块单测。"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

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

        mock_ak.stock_zh_a_spot_em.return_value = pd.DataFrame({
            "代码": ["002415", "300001", "600000"],
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

        mock_ak.stock_zh_a_spot_em.return_value = pd.DataFrame({
            "代码": ["600000"],
            "最新价": [8.50],
            "最低": [8.30],
        })
        result = fetch_realtime_prices(["002415.SZ"])
        assert result == {}


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


class TestAlertPriceLevel:
    @patch("rquant.monitor.subprocess")
    def test_formats_alert_correctly(self, mock_sub) -> None:
        from rquant.monitor import WatchItem, alert_price_level

        item = WatchItem(
            ts_code="002415.SZ", pool="pool2",
            limit_up_date=date(2026, 4, 17),
            body_upper=13.20, body_lower=11.80, body=1.40,
            level_40=12.36, level_30=12.22, level_20=12.08,
            stop_strong=11.80, stop_weak=11.52,
        )
        alert_price_level(item, "30", 12.18)

        mock_sub.Popen.assert_called_once()
        cmd = mock_sub.Popen.call_args[0][0]
        script = cmd[2]  # osascript -e "..."
        assert "002415.SZ | 30%" in script
        assert "current" in script
        assert "12.18" in script
        assert "强止" in script

    @patch("rquant.monitor.subprocess")
    def test_strong_stop_label(self, mock_sub) -> None:
        from rquant.monitor import WatchItem, alert_price_level

        item = WatchItem(
            ts_code="002415.SZ", pool="pool2",
            limit_up_date=date(2026, 4, 17),
            body_upper=13.20, body_lower=11.80, body=1.40,
            level_40=12.36, level_30=12.22, level_20=12.08,
            stop_strong=11.80, stop_weak=11.52,
        )
        alert_price_level(item, "strong", 11.75)

        cmd = mock_sub.Popen.call_args[0][0]
        script = cmd[2]
        assert "002415.SZ | 强止" in script


class TestAlertExitConfirm:
    @patch("rquant.monitor.subprocess")
    def test_returns_true_on_kick(self, mock_sub) -> None:
        from rquant.monitor import alert_exit_confirm

        mock_sub.run.return_value = MagicMock(
            stdout="button returned:踢出\n"
        )
        result = alert_exit_confirm(
            ts_code="002415.SZ",
            reason="跌破强止 ¥11.80",
            entry_date="04-18",
            days_in_pool=2,
            close_price=11.65,
            levels={"40": 12.36, "30": 12.22, "20": 12.08},
            stop_strong=11.80,
            stop_weak=11.52,
            triggered_levels=["40"],
        )
        assert result is True

    @patch("rquant.monitor.subprocess")
    def test_returns_false_on_keep(self, mock_sub) -> None:
        from rquant.monitor import alert_exit_confirm

        mock_sub.run.return_value = MagicMock(
            stdout="button returned:保留\n"
        )
        result = alert_exit_confirm(
            ts_code="002415.SZ",
            reason="观察期满",
            entry_date="04-18",
            days_in_pool=3,
            close_price=12.50,
            levels={"40": 12.36, "30": 12.22, "20": 12.08},
            stop_strong=11.80,
            stop_weak=11.52,
            triggered_levels=["40"],
        )
        assert result is False
