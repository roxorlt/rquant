"""monitor 模块单测。"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
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

    def test_hydrates_attack_reference_prices(self, store: DuckDBStore) -> None:
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
        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('002415.SZ', '2026-04-18', 12.0,13.0,11.8,12.5,12.0,0.5,4.17,1000,10000)"
        )
        store._conn.execute(
            """
            INSERT INTO daily_state VALUES
            ('002415.SZ', '2026-04-18', false, false, 'main', 0.10,
             13.20, 10.80, false, false, false, false, 0, 12.50, 12.00)
            """
        )

        items = build_watchlist(store)

        assert len(items) == 1
        assert items[0].reference_date == date(2026, 4, 18)
        assert items[0].t_high == 13.0
        assert items[0].t_close == 12.5
        assert items[0].limit_up_price_next == 13.75


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
    def test_returns_structured_quote_fields(self, mock_ak) -> None:
        from rquant.monitor import fetch_realtime_quotes

        mock_ak.stock_zh_a_spot.return_value = pd.DataFrame({
            "代码": ["sz002415"],
            "最新价": [12.35],
            "最低": [12.10],
            "今开": [12.25],
            "最高": [12.80],
            "昨收": [12.00],
            "涨跌幅": [2.92],
            "成交量": [100000.0],
            "成交额": [1235000.0],
        })

        result = fetch_realtime_quotes(["002415.SZ"])

        quote = result["002415.SZ"]
        assert quote.ts_code == "002415.SZ"
        assert quote.source == "sina"
        assert quote.price == 12.35
        assert quote.low == 12.10
        assert quote.open == 12.25
        assert quote.high == 12.80
        assert quote.pre_close == 12.00
        assert quote.pct_chg == 2.92
        assert quote.volume == 100000.0
        assert quote.amount == 1235000.0

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


class TestIntradayMinuteQuoteProvider:
    def test_builds_quote_from_rt_min_and_rt_min_daily(
        self, store: DuckDBStore
    ) -> None:
        from rquant.monitor import IntradayMinuteQuoteProvider

        class FakeAdapter:
            def __init__(self) -> None:
                self.calls: list[tuple[str, tuple[str, ...], str]] = []

            def rt_min(self, ts_codes: list[str], freq: str = "1min") -> pd.DataFrame:
                self.calls.append(("rt_min", tuple(ts_codes), freq))
                return pd.DataFrame([
                    {
                        "ts_code": "002415.SZ",
                        "trade_time": pd.Timestamp("2026-07-02 10:01:00"),
                        "freq": "1min",
                        "open": 12.70,
                        "high": 12.90,
                        "low": 12.60,
                        "close": 12.80,
                        "vol": 1000.0,
                        "amount": 12800.0,
                        "source": "tushare_rt",
                    }
                ])

            def rt_min_daily(
                self, ts_codes: list[str], freq: str = "1min"
            ) -> pd.DataFrame:
                self.calls.append(("rt_min_daily", tuple(ts_codes), freq))
                return pd.DataFrame([
                    {
                        "ts_code": "002415.SZ",
                        "trade_time": pd.Timestamp("2026-07-02 09:30:00"),
                        "freq": "1min",
                        "open": 12.00,
                        "high": 12.20,
                        "low": 11.90,
                        "close": 12.10,
                        "vol": 2000.0,
                        "amount": 24100.0,
                        "source": "tushare_rt_daily",
                    },
                    {
                        "ts_code": "002415.SZ",
                        "trade_time": pd.Timestamp("2026-07-02 10:00:00"),
                        "freq": "1min",
                        "open": 12.20,
                        "high": 13.10,
                        "low": 12.10,
                        "close": 12.70,
                        "vol": 3000.0,
                        "amount": 38100.0,
                        "source": "tushare_rt_daily",
                    },
                ])

        adapter = FakeAdapter()
        provider = IntradayMinuteQuoteProvider(
            adapter=adapter,
            store=store,
            daily_refresh_seconds=300,
        )

        quotes = provider.fetch(["002415.SZ"])

        assert adapter.calls == [
            ("rt_min", ("002415.SZ",), "1min"),
            ("rt_min_daily", ("002415.SZ",), "1min"),
        ]
        quote = quotes["002415.SZ"]
        assert quote.source == "tushare_rt_minute"
        assert quote.price == 12.80
        assert quote.open == 12.00
        assert quote.high == 13.10
        assert quote.low == 11.90
        assert quote.volume == 6000.0
        assert quote.amount == 75000.0

        persisted = store.query_minute_bars(
            "002415.SZ",
            pd.Timestamp("2026-07-02 09:30:00"),
            pd.Timestamp("2026-07-02 10:01:00"),
        )
        assert len(persisted) == 3
        assert set(persisted["source"]) == {"tushare_rt", "tushare_rt_daily"}


class TestRtMinThrottle:
    """C1：rt_min 节流——主循环 interval=5s，分钟级数据不该 5s 打一次 API。"""

    class _ThrottleAdapter:
        def __init__(self) -> None:
            self.rt_min_calls = 0
            self.rt_min_daily_calls = 0

        def rt_min(self, ts_codes: list[str], freq: str = "1min") -> pd.DataFrame:
            self.rt_min_calls += 1
            return pd.DataFrame([{
                "ts_code": "002415.SZ",
                "trade_time": pd.Timestamp("2026-07-02 10:00:00"),
                "freq": "1min",
                "open": 12.70, "high": 12.90, "low": 12.60, "close": 12.80,
                "vol": 1000.0, "amount": 12800.0,
                "source": "tushare_rt",
            }])

        def rt_min_daily(
            self, ts_codes: list[str], freq: str = "1min"
        ) -> pd.DataFrame:
            self.rt_min_daily_calls += 1
            return pd.DataFrame()

    @patch("rquant.monitor._now")
    def test_second_fetch_within_interval_uses_cache_and_stays_fresh(
        self, mock_now
    ) -> None:
        from rquant.monitor import IntradayMinuteQuoteProvider

        adapter = self._ThrottleAdapter()
        provider = IntradayMinuteQuoteProvider(
            adapter=adapter, store=None, rt_min_poll_seconds=15,
        )

        mock_now.return_value = datetime(2026, 7, 2, 10, 0, 0)
        quotes1 = provider.fetch(["002415.SZ"])
        assert adapter.rt_min_calls == 1
        assert quotes1["002415.SZ"].price == 12.80
        assert provider.used_fresh_tushare is True

        # 5 秒后（< 15s 节流间隔）：跳过 rt_min API，仍从内存缓存合成 quotes；
        # used_fresh_tushare 必须保持 True，否则 run_monitor 会把全部 code
        # 转 akshare 全量 fallback，节流反而放大调用量
        mock_now.return_value = datetime(2026, 7, 2, 10, 0, 5)
        quotes2 = provider.fetch(["002415.SZ"])
        assert adapter.rt_min_calls == 1
        assert quotes2["002415.SZ"].price == 12.80
        assert provider.used_fresh_tushare is True

    @patch("rquant.monitor._now")
    def test_poll_resumes_after_interval_elapsed(self, mock_now) -> None:
        from rquant.monitor import IntradayMinuteQuoteProvider

        adapter = self._ThrottleAdapter()
        provider = IntradayMinuteQuoteProvider(
            adapter=adapter, store=None, rt_min_poll_seconds=15,
        )

        mock_now.return_value = datetime(2026, 7, 2, 10, 0, 0)
        provider.fetch(["002415.SZ"])
        assert adapter.rt_min_calls == 1

        # 超过节流间隔后恢复真实 API 拉取
        mock_now.return_value = datetime(2026, 7, 2, 10, 0, 16)
        quotes = provider.fetch(["002415.SZ"])
        assert adapter.rt_min_calls == 2
        assert quotes["002415.SZ"].price == 12.80
        assert provider.used_fresh_tushare is True


class TestCheckAttackSignals:
    def _make_item(self):
        from rquant.monitor import WatchItem

        return WatchItem(
            ts_code="002415.SZ", pool="pool2",
            limit_up_date=date(2026, 4, 17),
            body_upper=13.20, body_lower=11.80, body=1.40,
            level_40=12.36, level_30=12.22, level_20=12.08,
            stop_strong=11.80, stop_weak=11.52,
            reference_date=date(2026, 4, 18),
            t_high=13.00,
            t_close=12.50,
            limit_up_price_next=13.75,
        )

    def test_triggers_open_strength_and_break_high(self) -> None:
        from rquant.monitor import RealtimeQuote, check_attack_signals

        item = self._make_item()
        quote = RealtimeQuote(
            ts_code="002415.SZ",
            price=13.05,
            low=12.60,
            open=12.88,
            high=13.10,
        )

        events = check_attack_signals(item, quote)

        levels = {e["level"] for e in events}
        assert "attack_open_strength" in levels
        assert "attack_break_high" in levels
        assert item.triggered["attack_open_strength"] is True
        assert item.triggered["attack_break_high"] is True

    def test_near_limit_requires_break_high_and_strong_carry(self) -> None:
        from rquant.monitor import RealtimeQuote, check_attack_signals

        item = self._make_item()
        quote = RealtimeQuote(
            ts_code="002415.SZ",
            price=13.58,
            low=12.55,
            open=12.70,
            high=13.60,
        )

        events = check_attack_signals(item, quote)

        levels = {e["level"] for e in events}
        assert "attack_near_limit" in levels

    def test_attack_signals_adjust_reference_levels_on_ex_right_day(self) -> None:
        from rquant.monitor import RealtimeQuote, WatchItem, check_attack_signals

        item = WatchItem(
            ts_code="688662.SH", pool="pool1",
            limit_up_date=date(2026, 6, 11),
            body_upper=132.00, body_lower=124.00, body=8.00,
            level_40=127.20, level_30=126.40, level_20=125.60,
            stop_strong=124.00, stop_weak=122.40,
            reference_date=date(2026, 6, 12),
            t_high=153.60,
            t_close=150.80,
            limit_up_price_next=181.00,
        )
        quote = RealtimeQuote(
            ts_code="688662.SH",
            price=118.00,
            low=115.80,
            open=116.20,
            high=118.20,
            pre_close=115.70,
        )

        events = check_attack_signals(item, quote)

        by_level = {event["level"]: event for event in events}
        assert "attack_strong_carry" in by_level
        assert "attack_break_high" in by_level
        assert by_level["attack_strong_carry"]["level_price"] == pytest.approx(115.70)
        assert by_level["attack_break_high"]["level_price"] == pytest.approx(
            153.60 * 115.70 / 150.80
        )

    def test_no_attack_signal_without_reference_prices(self) -> None:
        from rquant.monitor import RealtimeQuote, WatchItem, check_attack_signals

        item = WatchItem(
            ts_code="002415.SZ", pool="pool2",
            limit_up_date=date(2026, 4, 17),
            body_upper=13.20, body_lower=11.80, body=1.40,
            level_40=12.36, level_30=12.22, level_20=12.08,
            stop_strong=11.80, stop_weak=11.52,
        )
        quote = RealtimeQuote(
            ts_code="002415.SZ",
            price=13.58,
            low=12.55,
            open=12.70,
            high=13.60,
        )

        assert check_attack_signals(item, quote) == []

    def test_no_retrigger(self) -> None:
        from rquant.monitor import RealtimeQuote, check_attack_signals

        item = self._make_item()
        quote = RealtimeQuote(
            ts_code="002415.SZ",
            price=13.05,
            low=12.60,
            open=12.88,
            high=13.10,
        )

        check_attack_signals(item, quote)
        events2 = check_attack_signals(item, quote)

        assert events2 == []


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
        assert kwargs["auto_kicked"][0]["kind"] == "breakdown"
        assert kwargs["auto_kicked"][0]["reason_label"] == "弱止"
        assert kwargs["expired_held"] == []

    def test_aged_out_auto_kicks(self, store: DuckDBStore) -> None:
        """入池超过 pool2_max_age_days（默认 6 个交易日）自动踢出。"""
        from rquant.monitor import check_exits

        # 入池 2026-04-07，到 2026-04-21 含两端共 11 个交易日，> 6
        p2 = pd.DataFrame([{
            "ts_code": "002415.SZ",
            "entry_date": date(2026, 4, 7),
            "limit_up_date": date(2026, 4, 3),
            "body_upper": 13.20, "body_lower": 11.80,
            "level_40": 12.36, "level_30": 12.22, "level_20": 12.08,
            "stop_strong": 11.80, "stop_weak": 11.52,
            "status": "active",
        }])
        store.upsert_pool2_watch(p2)

        # 11 个交易日的 daily_bar，收盘价均高于强止
        bars = ",".join(
            f"('002415.SZ', '{d}', 12,13,11,12.5,12,0.5,5,1000,10000)"
            for d in [
                "2026-04-07", "2026-04-08", "2026-04-09", "2026-04-10",
                "2026-04-13", "2026-04-14", "2026-04-15", "2026-04-16",
                "2026-04-17", "2026-04-20", "2026-04-21",
            ]
        )
        store._conn.execute(f"INSERT INTO daily_bar VALUES {bars}")

        with patch("rquant.notify.notify") as mock_notify:
            kicked = check_exits(store, date(2026, 4, 21))

        assert kicked == 1
        active = store.query_pool2_active()
        assert len(active) == 0

        # 入库的 exit_reason 应为 aged_out，而非 breakdown
        row = store._conn.execute(
            "SELECT status, exit_reason FROM pool2_watch WHERE ts_code = ?",
            ["002415.SZ"],
        ).fetchone()
        assert row == ("exited", "aged_out")

        mock_notify.assert_called_once()
        kwargs = mock_notify.call_args.kwargs
        assert len(kwargs["auto_kicked"]) == 1
        assert kwargs["auto_kicked"][0]["kind"] == "aged_out"
        assert kwargs["auto_kicked"][0]["days_in_pool"] == 11
        assert kwargs["auto_kicked"][0]["threshold_days"] == 6
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


class TestMarketPhase:
    """_market_phase 各时间边界判定。"""

    @pytest.mark.parametrize("h,m,expected", [
        (8, 0, "pre"),
        (9, 24, "pre"),
        (9, 29, "pre"),
        (9, 30, "morning"),
        (10, 0, "morning"),
        (11, 29, "morning"),
        (11, 30, "lunch"),
        (12, 0, "lunch"),
        (12, 59, "lunch"),
        (13, 0, "afternoon"),
        (14, 30, "afternoon"),
        (14, 59, "afternoon"),
        (15, 0, "closed"),
        (15, 30, "closed"),
        (23, 59, "closed"),
    ])
    def test_phase_boundaries(self, h, m, expected) -> None:
        from rquant.monitor import _market_phase

        result = _market_phase(datetime(2026, 4, 30, h, m, 0))
        assert result == expected, f"{h:02d}:{m:02d} → got {result}, expected {expected}"

    def test_is_trading_hours_only_morning_afternoon(self) -> None:
        from rquant.monitor import _is_trading_hours

        with patch("rquant.monitor._now") as mock_now:
            for h, m, expected in [
                (9, 0, False),    # pre
                (10, 0, True),    # morning
                (12, 0, False),   # lunch
                (14, 0, True),    # afternoon
                (15, 30, False),  # closed
            ]:
                mock_now.return_value = datetime(2026, 4, 30, h, m, 0)
                assert _is_trading_hours() is expected, f"{h:02d}:{m:02d}"


class TestWaitForAfternoonOpen:
    @patch("rquant.monitor.time.sleep")
    @patch("rquant.monitor._now")
    def test_sleeps_capped_at_60s(self, mock_now, mock_sleep) -> None:
        """11:30 时距 13:00 还有 90min，每次只 sleep 60s（便于循环响应中断）。"""
        from rquant.monitor import _wait_for_afternoon_open

        mock_now.return_value = datetime(2026, 4, 30, 11, 30, 0)
        _wait_for_afternoon_open()

        mock_sleep.assert_called_once()
        slept = mock_sleep.call_args[0][0]
        assert slept == 60.0

    @patch("rquant.monitor.time.sleep")
    @patch("rquant.monitor._now")
    def test_sleeps_partial_when_close_to_open(self, mock_now, mock_sleep) -> None:
        from rquant.monitor import _wait_for_afternoon_open

        mock_now.return_value = datetime(2026, 4, 30, 12, 59, 30)
        _wait_for_afternoon_open()

        mock_sleep.assert_called_once()
        assert mock_sleep.call_args[0][0] == pytest.approx(30, abs=1)

    @patch("rquant.monitor.time.sleep")
    @patch("rquant.monitor._now")
    def test_no_sleep_after_open(self, mock_now, mock_sleep) -> None:
        from rquant.monitor import _wait_for_afternoon_open

        mock_now.return_value = datetime(2026, 4, 30, 13, 5, 0)
        _wait_for_afternoon_open()
        mock_sleep.assert_not_called()


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

    @patch("rquant.monitor.is_trading_day", return_value=True)
    @patch("rquant.monitor._now")
    def test_after_close_start_returns_without_opening_store(
        self, mock_now, _td
    ) -> None:
        """C2：闭市后启动（如 17:10）不打开 DuckDB 写库直接退出，避免整晚占写锁。"""
        from rquant.monitor import run_monitor

        mock_now.return_value = datetime(2026, 7, 2, 17, 10, 0)
        with patch("rquant.monitor.DuckDBStore") as mock_store_cls:
            result = run_monitor(interval=5)

        assert result == 0
        mock_store_cls.assert_not_called()

    @patch("rquant.monitor.build_watchlist", return_value=[])
    @patch("rquant.monitor.is_trading_day", return_value=True)
    @patch("rquant.monitor._now")
    def test_pre_open_start_still_opens_store(
        self, mock_now, _td, _build
    ) -> None:
        """9:25 systemd 盘前启动不被闭市 guard 误伤，照常打开写库进监控流程。"""
        from rquant.monitor import run_monitor

        mock_now.return_value = datetime(2026, 7, 2, 9, 25, 0)
        with patch("rquant.monitor.DuckDBStore") as mock_store_cls:
            result = run_monitor(interval=5)

        assert result == 0
        mock_store_cls.assert_called_once()

    def test_monitor_watchlist_snapshot_is_opt_in_and_keeps_pool_evidence(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from rquant import config as config_module
        from rquant.monitor import WatchItem, _publish_research_watchlist

        items = [
            WatchItem(
                ts_code="000001.SZ",
                pool="pool1",
                limit_up_date=date(2026, 7, 16),
                body_upper=10.0,
                body_lower=9.0,
                body=1.0,
                level_40=9.4,
                level_30=9.3,
                level_20=9.2,
                stop_strong=9.0,
                stop_weak=8.8,
            )
        ]
        monkeypatch.setattr(
            config_module.settings, "research_staging_dir", tmp_path / "staging"
        )
        monkeypatch.setattr(
            config_module.settings, "research_cloud_ingest_enabled", False
        )

        assert _publish_research_watchlist(items, date(2026, 7, 17)) is None
        assert not (tmp_path / "staging").exists()

        monkeypatch.setattr(
            config_module.settings, "research_cloud_ingest_enabled", True
        )
        monkeypatch.setattr(
            "rquant.research_manifest.detect_code_commit", lambda: "a" * 40
        )
        monkeypatch.setattr(
            "rquant.monitor._now", lambda: datetime(2026, 7, 17, 9, 25)
        )

        path = _publish_research_watchlist(items, date(2026, 7, 17))

        assert path is not None
        payload = path.read_text(encoding="utf-8")
        assert '"ts_code": "000001.SZ"' in payload
        assert '"pool": "pool1"' in payload

    @patch("rquant.monitor._count_trading_days_since", return_value=4)
    @patch("rquant.monitor.check_exits", return_value=0)
    @patch("rquant.monitor.IntradayMinuteQuoteProvider")
    @patch("rquant.monitor.build_watchlist")
    @patch("rquant.monitor.is_trading_day", return_value=True)
    @patch("rquant.monitor._wait_for_market_open")
    @patch("rquant.monitor.time.sleep")
    @patch("rquant.monitor._market_phase")
    @patch("rquant.monitor._now")
    def test_ignores_pullback_levels(
        self, mock_now, mock_phase, _sleep, _wait_open,
        _td, mock_build, mock_provider_cls, _exits, _count_days,
    ) -> None:
        from rquant.monitor import RealtimeQuote, WatchItem, run_monitor

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
        mock_provider = mock_provider_cls.return_value
        mock_provider.fetch.return_value = {
            "002415.SZ": RealtimeQuote(
                ts_code="002415.SZ",
                price=12.30,
                low=12.30,
            )
        }

        # 1st iter: morning. 2nd iter: closed → break.
        # 首个元素被启动时的闭市 guard 消费，之后 1 轮 morning + closed 收尾
        mock_phase.side_effect = ["morning", "morning", "closed"]
        mock_now.return_value = datetime(2026, 4, 21, 10, 0, 0)

        with (
            patch("rquant.notify.notify") as mock_notify,
            patch("rquant.monitor.DuckDBStore") as mock_store_cls,
        ):
            mock_store = mock_store_cls.return_value.__enter__.return_value
            mock_store.upsert_monitor_event.return_value = 1
            mock_store.query_monitor_events.return_value = pd.DataFrame()
            run_monitor(interval=5)

        assert "40" not in item.triggered
        mock_store.upsert_monitor_event.assert_not_called()

        scenes = [c.args[0] for c in mock_notify.call_args_list]
        assert "heartbeat" in scenes
        assert scenes.count("heartbeat") == 2  # start + stop
        assert "price_level" not in scenes

    @patch("rquant.monitor._count_trading_days_since", return_value=4)
    @patch("rquant.monitor.check_exits", return_value=0)
    @patch("rquant.monitor.fetch_realtime_quotes")
    @patch("rquant.monitor.IntradayMinuteQuoteProvider")
    @patch("rquant.monitor.build_watchlist")
    @patch("rquant.monitor.is_trading_day", return_value=True)
    @patch("rquant.monitor._wait_for_market_open")
    @patch("rquant.monitor.time.sleep")
    @patch("rquant.monitor._market_phase")
    @patch("rquant.monitor._now")
    def test_uses_tushare_minute_quotes_and_falls_back_for_missing_codes(
        self, mock_now, mock_phase, _sleep, _wait_open,
        _td, mock_build, mock_provider_cls, mock_ak_fetch, _exits, _count_days,
    ) -> None:
        from rquant.monitor import RealtimeQuote, WatchItem, run_monitor

        p2 = WatchItem(
            ts_code="002415.SZ", pool="pool2",
            limit_up_date=date(2026, 4, 17),
            body_upper=13.20, body_lower=11.80, body=1.40,
            level_40=12.36, level_30=12.22, level_20=12.08,
            stop_strong=11.80, stop_weak=11.52,
            name="海康威视",
            entry_date=date(2026, 4, 18),
        )
        p1 = WatchItem(
            ts_code="300001.SZ", pool="pool1",
            limit_up_date=date(2026, 4, 17),
            body_upper=16.50, body_lower=14.80, body=1.70,
            level_40=15.48, level_30=15.31, level_20=15.14,
            stop_strong=14.80, stop_weak=14.46,
            name="特锐德",
            entry_date=date(2026, 4, 17),
        )
        mock_build.return_value = [p2, p1]
        mock_provider = mock_provider_cls.return_value
        mock_provider.fetch.return_value = {
            "002415.SZ": RealtimeQuote(
                ts_code="002415.SZ",
                price=12.80,
                low=11.90,
                open=12.00,
                high=13.10,
                source="tushare_rt_minute",
            )
        }
        mock_ak_fetch.return_value = {
            "300001.SZ": RealtimeQuote(
                ts_code="300001.SZ",
                price=15.00,
                low=14.90,
                source="sina",
            )
        }
        # 首个元素被启动时的闭市 guard 消费，之后 1 轮 morning + closed 收尾
        mock_phase.side_effect = ["morning", "morning", "closed"]
        mock_now.return_value = datetime(2026, 7, 2, 10, 0, 0)

        with (
            patch("rquant.notify.notify"),
            patch("rquant.monitor.DuckDBStore") as mock_store_cls,
        ):
            mock_store = mock_store_cls.return_value.__enter__.return_value
            mock_store.upsert_monitor_event.return_value = 1
            mock_store.query_monitor_events.return_value = pd.DataFrame()
            run_monitor(interval=5)

        mock_provider_cls.assert_called_once_with(store=mock_store)
        mock_provider.fetch.assert_called_once_with(["002415.SZ", "300001.SZ"])
        mock_ak_fetch.assert_called_once_with(["300001.SZ"])

    @patch("rquant.monitor._count_trading_days_since", return_value=4)
    @patch("rquant.monitor.check_exits", return_value=0)
    @patch("rquant.monitor.fetch_realtime_quotes")
    @patch("rquant.monitor.IntradayMinuteQuoteProvider")
    @patch("rquant.monitor.build_watchlist")
    @patch("rquant.monitor.is_trading_day", return_value=True)
    @patch("rquant.monitor._wait_for_market_open")
    @patch("rquant.monitor.time.sleep")
    @patch("rquant.monitor._market_phase")
    @patch("rquant.monitor._now")
    def test_falls_back_all_codes_when_tushare_has_no_fresh_data(
        self, mock_now, mock_phase, _sleep, _wait_open,
        _td, mock_build, mock_provider_cls, mock_ak_fetch, _exits, _count_days,
    ) -> None:
        from rquant.monitor import RealtimeQuote, WatchItem, run_monitor

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
        mock_provider = mock_provider_cls.return_value
        mock_provider.used_fresh_tushare = False
        mock_provider.fetch.return_value = {
            "002415.SZ": RealtimeQuote(
                ts_code="002415.SZ",
                price=12.80,
                low=11.90,
                source="tushare_rt_minute",
            )
        }
        mock_ak_fetch.return_value = {
            "002415.SZ": RealtimeQuote(
                ts_code="002415.SZ",
                price=12.82,
                low=11.91,
                source="sina",
            )
        }
        # 首个元素被启动时的闭市 guard 消费，之后 1 轮 morning + closed 收尾
        mock_phase.side_effect = ["morning", "morning", "closed"]
        mock_now.return_value = datetime(2026, 7, 2, 10, 0, 0)

        with (
            patch("rquant.notify.notify"),
            patch("rquant.monitor.DuckDBStore") as mock_store_cls,
        ):
            mock_store = mock_store_cls.return_value.__enter__.return_value
            mock_store.upsert_monitor_event.return_value = 1
            mock_store.query_monitor_events.return_value = pd.DataFrame()
            run_monitor(interval=5)

        mock_ak_fetch.assert_called_once_with(["002415.SZ"])

    @patch("rquant.monitor._count_trading_days_since", return_value=4)
    @patch("rquant.monitor.check_exits", return_value=0)
    @patch("rquant.monitor.IntradayMinuteQuoteProvider")
    @patch("rquant.monitor.build_watchlist")
    @patch("rquant.monitor.is_trading_day", return_value=True)
    @patch("rquant.monitor._wait_for_market_open")
    @patch("rquant.monitor._wait_for_afternoon_open")
    @patch("rquant.monitor.time.sleep")
    @patch("rquant.monitor._market_phase")
    def test_crosses_lunch_break_without_exiting(
        self, mock_phase, _sleep, mock_wait_pm, _wait_open,
        _td, mock_build, mock_provider_cls, _exits, _count_days,
    ) -> None:
        """关键回归：morning → lunch → afternoon → closed 期间进程**不退出**。

        bug 修前：上午 11:30 _is_trading_hours False → 立刻退出，下午无监控。
        修后：lunch 阶段 sleep 等下午开盘，afternoon 继续轮询，15:00 才退。
        """
        from rquant.monitor import RealtimeQuote, WatchItem, run_monitor

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
        # afternoon 阶段股价没跌破档位（避免跟踪 trigger 状态干扰断言）
        mock_provider = mock_provider_cls.return_value
        mock_provider.fetch.return_value = {
            "002415.SZ": RealtimeQuote(
                ts_code="002415.SZ",
                price=13.00,
                low=12.50,
            )
        }

        # 模拟一整天阶段流转
        mock_phase.side_effect = [
            "morning",    # 0: 启动时闭市 guard 消费
            "morning",    # 1: fetch
            "morning",    # 2: fetch
            "lunch",      # 3: sleep 等下午
            "lunch",      # 4: sleep 等下午
            "afternoon",  # 5: fetch
            "afternoon",  # 6: fetch
            "closed",     # 7: break
        ]

        with (
            patch("rquant.notify.notify"),
            patch("rquant.monitor.DuckDBStore") as mock_store_cls,
        ):
            mock_store = mock_store_cls.return_value.__enter__.return_value
            mock_store.upsert_monitor_event.return_value = 1
            mock_store.query_monitor_events.return_value = pd.DataFrame()
            run_monitor(interval=5)

        # morning + afternoon = 4 次 fetch；lunch 不该 fetch
        assert mock_provider.fetch.call_count == 4

        # lunch 阶段进入 _wait_for_afternoon_open 至少 2 次
        assert mock_wait_pm.call_count >= 2


class TestIntradayQuoteSourceSwitch:
    """INTRADAY_QUOTE_SOURCE 紧急回退开关：akshare 时跳过 Tushare 分钟行情主源。"""

    @staticmethod
    def _watch_item():
        from rquant.monitor import WatchItem

        return WatchItem(
            ts_code="002415.SZ", pool="pool2",
            limit_up_date=date(2026, 4, 17),
            body_upper=13.20, body_lower=11.80, body=1.40,
            level_40=12.36, level_30=12.22, level_20=12.08,
            stop_strong=11.80, stop_weak=11.52,
            name="海康威视",
            entry_date=date(2026, 4, 18),
        )

    @patch("rquant.monitor._count_trading_days_since", return_value=4)
    @patch("rquant.monitor.check_exits", return_value=0)
    @patch("rquant.monitor.fetch_realtime_quotes")
    @patch("rquant.monitor.IntradayMinuteQuoteProvider")
    @patch("rquant.monitor.build_watchlist")
    @patch("rquant.monitor.is_trading_day", return_value=True)
    @patch("rquant.monitor._wait_for_market_open")
    @patch("rquant.monitor.time.sleep")
    @patch("rquant.monitor._market_phase")
    @patch("rquant.monitor._now")
    def test_akshare_source_skips_tushare_provider(
        self, mock_now, mock_phase, _sleep, _wait_open,
        _td, mock_build, mock_provider_cls, mock_ak_fetch, _exits, _count_days,
        monkeypatch,
    ) -> None:
        from rquant.config import settings as cfg_settings
        from rquant.monitor import RealtimeQuote, run_monitor

        monkeypatch.setattr(cfg_settings, "intraday_quote_source", "akshare")

        mock_build.return_value = [self._watch_item()]
        mock_ak_fetch.return_value = {
            "002415.SZ": RealtimeQuote(
                ts_code="002415.SZ",
                price=13.00,
                low=12.50,
                source="sina",
            )
        }
        # 首个元素被启动时的闭市 guard 消费，之后 1 轮 morning + closed 收尾
        mock_phase.side_effect = ["morning", "morning", "closed"]
        mock_now.return_value = datetime(2026, 7, 2, 10, 0, 0)

        with (
            patch("rquant.notify.notify"),
            patch("rquant.monitor.DuckDBStore") as mock_store_cls,
        ):
            mock_store = mock_store_cls.return_value.__enter__.return_value
            mock_store.upsert_monitor_event.return_value = 1
            mock_store.query_monitor_events.return_value = pd.DataFrame()
            run_monitor(interval=5)

        mock_provider_cls.assert_not_called()
        mock_ak_fetch.assert_called_once_with(["002415.SZ"])

    @patch("rquant.monitor._count_trading_days_since", return_value=4)
    @patch("rquant.monitor.check_exits", return_value=0)
    @patch("rquant.monitor.fetch_realtime_quotes", return_value={})
    @patch("rquant.monitor.IntradayMinuteQuoteProvider")
    @patch("rquant.monitor.build_watchlist")
    @patch("rquant.monitor.is_trading_day", return_value=True)
    @patch("rquant.monitor._wait_for_market_open")
    @patch("rquant.monitor.time.sleep")
    @patch("rquant.monitor._market_phase")
    @patch("rquant.monitor._now")
    def test_tushare_source_constructs_provider(
        self, mock_now, mock_phase, _sleep, _wait_open,
        _td, mock_build, mock_provider_cls, _ak_fetch, _exits, _count_days,
        monkeypatch,
    ) -> None:
        from rquant.config import settings as cfg_settings
        from rquant.monitor import RealtimeQuote, run_monitor

        monkeypatch.setattr(cfg_settings, "intraday_quote_source", "tushare")

        mock_build.return_value = [self._watch_item()]
        mock_provider = mock_provider_cls.return_value
        mock_provider.fetch.return_value = {
            "002415.SZ": RealtimeQuote(
                ts_code="002415.SZ",
                price=13.00,
                low=12.50,
                source="tushare_rt_minute",
            )
        }
        # 首个元素被启动时的闭市 guard 消费，之后 1 轮 morning + closed 收尾
        mock_phase.side_effect = ["morning", "morning", "closed"]
        mock_now.return_value = datetime(2026, 7, 2, 10, 0, 0)

        with (
            patch("rquant.notify.notify"),
            patch("rquant.monitor.DuckDBStore") as mock_store_cls,
        ):
            mock_store = mock_store_cls.return_value.__enter__.return_value
            mock_store.upsert_monitor_event.return_value = 1
            mock_store.query_monitor_events.return_value = pd.DataFrame()
            run_monitor(interval=5)

        mock_provider_cls.assert_called_once_with(store=mock_store)
        mock_provider.fetch.assert_called_once_with(["002415.SZ"])

    def test_invalid_source_rejected_at_settings_validation(self) -> None:
        from pydantic import ValidationError

        from rquant.config import Settings

        with pytest.raises(ValidationError, match="intraday_quote_source"):
            Settings(intraday_quote_source="sina")

    def test_blank_source_falls_back_to_tushare(self) -> None:
        # .env 里 INTRADAY_QUOTE_SOURCE= 留空读到 ""，应回落默认 tushare
        from rquant.config import Settings

        assert Settings(intraday_quote_source="").intraday_quote_source == "tushare"


class TestCheckExitsSingleStockIsolation:
    """check_exits 单票故障隔离：一只票异常不中断整批退出检查（审计 PR1-G）。"""

    def test_one_bad_stock_does_not_block_others(self, store: DuckDBStore) -> None:
        from datetime import date
        from unittest.mock import patch

        from rquant.monitor import check_exits

        # 两只 active：bad 的 stop_strong 为 NaN（float 转换会在比较中产生问题）
        # good 正常跌破弱止应被踢
        p2 = pd.DataFrame([
            {
                "ts_code": "000001.SZ", "entry_date": date(2026, 4, 20),
                "limit_up_date": date(2026, 4, 19),
                "body_upper": 13.20, "body_lower": 11.80,
                "level_40": 12.36, "level_30": 12.22, "level_20": 12.08,
                "stop_strong": 11.80, "stop_weak": 11.52, "status": "active",
            },
            {
                "ts_code": "000002.SZ", "entry_date": date(2026, 4, 20),
                "limit_up_date": date(2026, 4, 19),
                "body_upper": 20.0, "body_lower": 18.0,
                "level_40": 18.8, "level_30": 18.6, "level_20": 18.4,
                "stop_strong": 18.0, "stop_weak": 17.6, "status": "active",
            },
        ])
        store.upsert_pool2_watch(p2)
        # 000002.SZ 收盘跌破弱止 → 应踢；000001.SZ 无 daily_bar 行
        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('000002.SZ', '2026-04-21', 18,18,17,17.4,18,-0.5,-3,1000,10000)"
        )

        # 让 000001.SZ 处理时抛异常：mock update_pool2_exit 对它抛错
        store._conn.execute(
            "INSERT INTO daily_bar VALUES "
            "('000001.SZ', '2026-04-21', 12,12,11,11.4,12,-0.5,-4,1000,10000)"
        )

        orig = type(store).update_pool2_exit

        def flaky_update(self, code, exit_date, reason):
            if code == "000001.SZ":
                raise RuntimeError("simulated single-stock failure")
            return orig(self, code, exit_date, reason)

        with patch("rquant.notify.notify"), \
             patch.object(type(store), "update_pool2_exit", flaky_update):
            kicked = check_exits(store, date(2026, 4, 21))

        # 000002.SZ 仍被成功踢出（000001.SZ 的异常被隔离）
        assert kicked == 1
        active = store.query_pool2_active()
        assert set(active["ts_code"]) == {"000001.SZ"}  # 只剩异常那只仍 active
