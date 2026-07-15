"""load_universe() 单测：用临时 DuckDB 实例塞数据。"""

from datetime import UTC, date, datetime, time
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

import rquant.screen.loader as screen_loader
from rquant.screen.loader import load_universe
from rquant.screen.rules import (
    AggregateRequest,
    has_prior_limit_up,
    no_consec_ups_in_window,
    no_limit_down_in_window,
)
from rquant.security_status import SecurityStatusDaily
from rquant.storage.duckdb import DuckDBStore
from rquant.trade_calendar import TradeCalendarDay

SHANGHAI = ZoneInfo("Asia/Shanghai")


def _status(
    ts_code: str,
    trade_date: date,
    name: str,
    is_st: bool,
    *,
    available_time: time = time(9, 25),
    ingested_at: datetime = datetime(2026, 4, 16, tzinfo=UTC),
) -> SecurityStatusDaily:
    return SecurityStatusDaily(
        ts_code=ts_code,
        trade_date=trade_date,
        name=name,
        is_st=is_st,
        name_source="namechange",
        st_source="namechange+stock_st",
        available_at=datetime.combine(trade_date, available_time, SHANGHAI),
        ingested_at=ingested_at,
    )


@pytest.fixture
def store(tmp_path) -> DuckDBStore:
    s = DuckDBStore(path=tmp_path / "test.duckdb")

    calendar_dates = [date(2026, 4, day) for day in range(6, 16)]
    s.upsert_trade_calendar([
        TradeCalendarDay(
            exchange="SSE",
            cal_date=cal_date,
            is_open=cal_date.weekday() < 5,
            source="tushare",
            updated_at=datetime(2026, 4, 16, tzinfo=UTC),
        )
        for cal_date in calendar_dates
    ])

    daily = pd.DataFrame([
        # 300001.SZ：3 天数据
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 13), "open": 10.0,
         "high": 11.0, "low": 9.0, "close": 10.5, "pre_close": 10.0,
         "change": 0.5, "pct_chg": 5.0, "vol": 1000.0, "amount": 10500.0},
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 14), "open": 10.5,
         "high": 12.0, "low": 10.0, "close": 11.0, "pre_close": 10.5,
         "change": 0.5, "pct_chg": 4.76, "vol": 1200.0, "amount": 13200.0},
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 15), "open": 11.0,
         "high": 13.0, "low": 11.0, "close": 12.5, "pre_close": 11.0,
         "change": 1.5, "pct_chg": 13.64, "vol": 2000.0, "amount": 25000.0},
        # 000001.SZ：3 天数据
        {"ts_code": "000001.SZ", "trade_date": date(2026, 4, 13), "open": 20.0,
         "high": 21.0, "low": 19.0, "close": 20.5, "pre_close": 20.0,
         "change": 0.5, "pct_chg": 2.5, "vol": 500.0, "amount": 10250.0},
        {"ts_code": "000001.SZ", "trade_date": date(2026, 4, 14), "open": 20.5,
         "high": 21.5, "low": 20.0, "close": 21.0, "pre_close": 20.5,
         "change": 0.5, "pct_chg": 2.44, "vol": 600.0, "amount": 12600.0},
        {"ts_code": "000001.SZ", "trade_date": date(2026, 4, 15), "open": 21.0,
         "high": 22.0, "low": 20.5, "close": 21.5, "pre_close": 21.0,
         "change": 0.5, "pct_chg": 2.38, "vol": 700.0, "amount": 15050.0},
    ])
    s.upsert_daily(daily)

    basic = pd.DataFrame([
        {"ts_code": "300001.SZ", "symbol": "300001", "name": "特锐德",
         "area": "山东", "industry": "电气设备", "list_date": date(2009, 10, 30),
         "market": "创业板"},
        {"ts_code": "000001.SZ", "symbol": "000001", "name": "平安银行",
         "area": "深圳", "industry": "银行", "list_date": date(1991, 4, 3),
         "market": "主板"},
    ])
    s.upsert_stock_basic(basic)

    indicators = pd.DataFrame([
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 15), "ma5": 11.5,
         "ma10": 11.0, "ma20": 10.5, "ma60": 10.0, "rsi6": 60.0, "rsi14": 55.0,
         "macd": 0.3, "macd_signal": 0.2, "macd_hist": 0.1,
         "kdj_k": 70.0, "kdj_d": 65.0, "kdj_j": 80.0},
        {"ts_code": "000001.SZ", "trade_date": date(2026, 4, 15), "ma5": 21.0,
         "ma10": 20.5, "ma20": 20.0, "ma60": 19.5, "rsi6": 50.0, "rsi14": 48.0,
         "macd": 0.1, "macd_signal": 0.1, "macd_hist": 0.0,
         "kdj_k": 55.0, "kdj_d": 52.0, "kdj_j": 60.0},
    ])
    s.upsert_indicators(indicators)

    state = pd.DataFrame([
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 14),
         "is_st": True, "is_bj": False, "board_type": "gem",
         "limit_pct": 0.20, "limit_up_price": 12.60, "limit_down_price": 8.40,
         "is_limit_up": False, "is_limit_down": False, "is_first_limit_up": False,
         "is_yiziban": False, "consecutive_limit_ups": 0,
         "body_upper": 11.0, "body_lower": 10.5},
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 15),
         "is_st": False, "is_bj": False, "board_type": "gem",
         "limit_pct": 0.20, "limit_up_price": 13.20, "limit_down_price": 8.80,
         "is_limit_up": True, "is_limit_down": False, "is_first_limit_up": True,
         "is_yiziban": False, "consecutive_limit_ups": 1,
         "body_upper": 12.5, "body_lower": 11.0},
        {"ts_code": "000001.SZ", "trade_date": date(2026, 4, 15),
         "is_st": False, "is_bj": False, "board_type": "main",
         "limit_pct": 0.10, "limit_up_price": 23.10, "limit_down_price": 18.90,
         "is_limit_up": False, "is_limit_down": False, "is_first_limit_up": False,
         "is_yiziban": False, "consecutive_limit_ups": 0,
         "body_upper": 21.5, "body_lower": 21.0},
    ])
    s.upsert_state(state)

    daily_basic_data = pd.DataFrame([
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 14),
         "turnover_rate": 1.5, "volume_ratio": 1.1,
         "total_mv": 5000000.0, "circ_mv": 4000000.0},
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 15),
         "turnover_rate": 2.0, "volume_ratio": 1.3,
         "total_mv": 6000000.0, "circ_mv": 5000000.0},
        {"ts_code": "000001.SZ", "trade_date": date(2026, 4, 15),
         "turnover_rate": 0.8, "volume_ratio": 0.9,
         "total_mv": 30000000.0, "circ_mv": 28000000.0},
    ])
    s.upsert_daily_basic(daily_basic_data)

    # Extra state data for aggregate testing (older dates need daily_bar too)
    extra_daily = pd.DataFrame([
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 6), "open": 7.5,
         "high": 8.5, "low": 7.0, "close": 8.0, "pre_close": 7.5,
         "change": 0.5, "pct_chg": 6.67, "vol": 750.0, "amount": 6000.0},
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 7), "open": 8.0,
         "high": 9.0, "low": 7.5, "close": 8.5, "pre_close": 8.0,
         "change": 0.5, "pct_chg": 6.25, "vol": 800.0, "amount": 6800.0},
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 8), "open": 8.5,
         "high": 9.5, "low": 8.0, "close": 9.0, "pre_close": 8.5,
         "change": 0.5, "pct_chg": 5.88, "vol": 900.0, "amount": 8100.0},
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 9), "open": 9.0,
         "high": 10.0, "low": 9.0, "close": 9.5, "pre_close": 9.0,
         "change": 0.5, "pct_chg": 5.56, "vol": 950.0, "amount": 9025.0},
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 10), "open": 9.5,
         "high": 10.5, "low": 9.0, "close": 10.0, "pre_close": 9.5,
         "change": 0.5, "pct_chg": 5.26, "vol": 1100.0, "amount": 11000.0},
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 11), "open": 10.0,
         "high": 11.0, "low": 9.5, "close": 10.5, "pre_close": 10.0,
         "change": 0.5, "pct_chg": 5.0, "vol": 1050.0, "amount": 11025.0},
        {"ts_code": "000001.SZ", "trade_date": date(2026, 4, 6), "open": 18.0,
         "high": 18.5, "low": 17.5, "close": 18.2, "pre_close": 18.0,
         "change": 0.2, "pct_chg": 1.11, "vol": 400.0, "amount": 7280.0},
        {"ts_code": "000001.SZ", "trade_date": date(2026, 4, 7), "open": 18.2,
         "high": 18.8, "low": 18.0, "close": 18.5, "pre_close": 18.2,
         "change": 0.3, "pct_chg": 1.65, "vol": 410.0, "amount": 7585.0},
        {"ts_code": "000001.SZ", "trade_date": date(2026, 4, 8), "open": 18.5,
         "high": 19.0, "low": 18.3, "close": 18.8, "pre_close": 18.5,
         "change": 0.3, "pct_chg": 1.62, "vol": 420.0, "amount": 7896.0},
        {"ts_code": "000001.SZ", "trade_date": date(2026, 4, 9), "open": 18.8,
         "high": 19.5, "low": 18.6, "close": 19.2, "pre_close": 18.8,
         "change": 0.4, "pct_chg": 2.13, "vol": 430.0, "amount": 8256.0},
        {"ts_code": "000001.SZ", "trade_date": date(2026, 4, 10), "open": 19.2,
         "high": 20.0, "low": 19.0, "close": 19.7, "pre_close": 19.2,
         "change": 0.5, "pct_chg": 2.60, "vol": 440.0, "amount": 8668.0},
    ])
    s.upsert_daily(extra_daily)

    extra_state = pd.DataFrame([
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 6),
         "is_st": False, "is_bj": False, "board_type": "gem",
         "limit_pct": 0.20, "limit_up_price": 9.00, "limit_down_price": 6.00,
         "is_limit_up": False, "is_limit_down": False, "is_first_limit_up": False,
         "is_yiziban": False, "consecutive_limit_ups": 0,
         "body_upper": 8.0, "body_lower": 7.5},
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 7),
         "is_st": False, "is_bj": False, "board_type": "gem",
         "limit_pct": 0.20, "limit_up_price": 9.60, "limit_down_price": 6.40,
         "is_limit_up": True, "is_limit_down": False, "is_first_limit_up": True,
         "is_yiziban": False, "consecutive_limit_ups": 1,
         "body_upper": 8.5, "body_lower": 8.0},
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 8),
         "is_st": False, "is_bj": False, "board_type": "gem",
         "limit_pct": 0.20, "limit_up_price": 10.20, "limit_down_price": 6.80,
         "is_limit_up": True, "is_limit_down": False, "is_first_limit_up": False,
         "is_yiziban": False, "consecutive_limit_ups": 2,
         "body_upper": 9.0, "body_lower": 8.5},
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 9),
         "is_st": False, "is_bj": False, "board_type": "gem",
         "limit_pct": 0.20, "limit_up_price": 10.80, "limit_down_price": 7.20,
         "is_limit_up": False, "is_limit_down": False, "is_first_limit_up": False,
         "is_yiziban": False, "consecutive_limit_ups": 0,
         "body_upper": 9.5, "body_lower": 9.0},
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 10),
         "is_st": False, "is_bj": False, "board_type": "gem",
         "limit_pct": 0.20, "limit_up_price": 11.40, "limit_down_price": 7.60,
         "is_limit_up": False, "is_limit_down": True, "is_first_limit_up": False,
         "is_yiziban": False, "consecutive_limit_ups": 0,
         "body_upper": 10.0, "body_lower": 9.5},
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 11),
         "is_st": False, "is_bj": False, "board_type": "gem",
         "limit_pct": 0.20, "limit_up_price": 12.00, "limit_down_price": 8.00,
         "is_limit_up": False, "is_limit_down": False, "is_first_limit_up": False,
         "is_yiziban": False, "consecutive_limit_ups": 0,
         "body_upper": 10.5, "body_lower": 10.0},
        {"ts_code": "300001.SZ", "trade_date": date(2026, 4, 13),
         "is_st": False, "is_bj": False, "board_type": "gem",
         "limit_pct": 0.20, "limit_up_price": 12.00, "limit_down_price": 8.00,
         "is_limit_up": False, "is_limit_down": False, "is_first_limit_up": False,
         "is_yiziban": False, "consecutive_limit_ups": 0,
         "body_upper": 10.5, "body_lower": 10.0},
    ])
    for trade_day in [
        date(2026, 4, 6), date(2026, 4, 7), date(2026, 4, 8),
        date(2026, 4, 9), date(2026, 4, 10), date(2026, 4, 13),
        date(2026, 4, 14),
    ]:
        extra_state.loc[len(extra_state)] = {
            "ts_code": "000001.SZ",
            "trade_date": trade_day,
            "is_st": False,
            "is_bj": False,
            "board_type": "main",
            "limit_pct": 0.10,
            "limit_up_price": 22.0,
            "limit_down_price": 18.0,
            "is_limit_up": False,
            "is_limit_down": False,
            "is_first_limit_up": False,
            "is_yiziban": False,
            "consecutive_limit_ups": 0,
            "body_upper": 20.0,
            "body_lower": 19.5,
        }
    s.upsert_state(extra_state)

    open_dates = [cal_date for cal_date in calendar_dates if cal_date.weekday() < 5]
    statuses: list[SecurityStatusDaily] = []
    for trade_day in open_dates:
        gem_name = {
            date(2026, 4, 13): "戴帽前",
            date(2026, 4, 14): "*ST戴帽期",
            date(2026, 4, 15): "摘帽后",
        }.get(trade_day, "历史特锐德")
        statuses.append(
            _status(
                "300001.SZ",
                trade_day,
                gem_name,
                trade_day == date(2026, 4, 14),
            )
        )
        statuses.append(_status("000001.SZ", trade_day, "历史平安", False))
    s.upsert_stock_status(statuses)

    yield s
    s.close()


class TestLoadUniverse:
    def test_wide_frame_shape_and_columns(self, store: DuckDBStore) -> None:
        df = load_universe("2026-04-15", lookback=2, store=store)

        assert len(df) == 2
        assert set(df["ts_code"]) == {"300001.SZ", "000001.SZ"}
        assert "CLOSE[0]" in df.columns
        assert "CLOSE[1]" in df.columns
        assert "CLOSE[2]" in df.columns
        assert "MA20[0]" in df.columns
        assert "IS_FIRST_LIMIT_UP[0]" in df.columns
        assert "is_st" in df.columns
        assert "board_type" in df.columns
        assert "name" in df.columns

    def test_values_at_t0(self, store: DuckDBStore) -> None:
        df = load_universe("2026-04-15", lookback=2, store=store)
        row = df.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert row["CLOSE[0]"] == pytest.approx(12.5)
        assert row["HIGH[0]"] == pytest.approx(13.0)
        assert row["PRE_CLOSE[0]"] == pytest.approx(11.0)
        assert row["MA20[0]"] == pytest.approx(10.5)
        assert row["IS_FIRST_LIMIT_UP[0]"]
        assert row["board_type"] == "gem"
        assert row["name"] == "摘帽后"

    def test_values_at_t1(self, store: DuckDBStore) -> None:
        df = load_universe("2026-04-15", lookback=2, store=store)
        row = df.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert row["CLOSE[1]"] == pytest.approx(11.0)
        assert row["HIGH[1]"] == pytest.approx(12.0)

    def test_lookback_zero_only_today(self, store: DuckDBStore) -> None:
        df = load_universe("2026-04-15", lookback=0, store=store)
        assert "CLOSE[0]" in df.columns
        assert "CLOSE[1]" not in df.columns

    def test_universe_is_t0_stocks_only(self, store: DuckDBStore) -> None:
        # 如果某只股票 T 日没数据（停牌/未上市），不应出现在结果里
        extra = pd.DataFrame([
            {"ts_code": "900001.SH", "trade_date": date(2026, 4, 13), "open": 5.0,
             "high": 5.5, "low": 4.5, "close": 5.0, "pre_close": 5.0,
             "change": 0.0, "pct_chg": 0.0, "vol": 100.0, "amount": 500.0}
        ])
        store.upsert_daily(extra)
        df = load_universe("2026-04-15", lookback=2, store=store)
        assert "900001.SH" not in set(df["ts_code"])

    @pytest.mark.parametrize(
        ("trade_date", "expected_name", "expected_is_st"),
        [
            ("2026-04-13", "戴帽前", False),
            ("2026-04-14", "*ST戴帽期", True),
            ("2026-04-15", "摘帽后", False),
        ],
    )
    def test_uses_exact_date_historical_status(
        self,
        store: DuckDBStore,
        trade_date: str,
        expected_name: str,
        expected_is_st: bool,
    ) -> None:
        df = load_universe(trade_date, lookback=0, store=store)
        row = df.loc[df["ts_code"] == "300001.SZ"].iloc[0]

        assert row["name"] == expected_name
        assert row["is_st"] == expected_is_st
        assert str(df["name"].dtype) == "string"
        assert str(df["is_st"].dtype) == "boolean"

    def test_default_decision_at_is_shanghai_1700(
        self, store: DuckDBStore
    ) -> None:
        store.upsert_stock_status([
            _status(
                "300001.SZ",
                date(2026, 4, 15),
                "收盘前可见",
                False,
                available_time=time(16, 59),
                ingested_at=datetime(2026, 4, 17, tzinfo=UTC),
            )
        ])

        df = load_universe("2026-04-15", lookback=0, store=store)
        row = df.loc[df["ts_code"] == "300001.SZ"].iloc[0]

        assert row["name"] == "收盘前可见"
        assert not row["is_st"]

    def test_future_visible_status_remains_unknown(
        self, store: DuckDBStore
    ) -> None:
        store.upsert_stock_status([
            _status(
                "300001.SZ",
                date(2026, 4, 15),
                "未来名称",
                False,
                available_time=time(17, 1),
                ingested_at=datetime(2026, 4, 17, tzinfo=UTC),
            )
        ])

        df = load_universe("2026-04-15", lookback=0, store=store)
        row = df.loc[df["ts_code"] == "300001.SZ"].iloc[0]

        assert pd.isna(row["name"])
        assert pd.isna(row["is_st"])
        assert pd.isna(row["IS_LIMIT_UP[0]"])

    def test_close_fields_are_unavailable_before_daily_screen_time(
        self, store: DuckDBStore
    ) -> None:
        decision_at = datetime(2026, 4, 15, 9, 24, tzinfo=SHANGHAI)

        with pytest.raises(ValueError, match="at or after.*17:00"):
            load_universe(
                "2026-04-15",
                lookback=0,
                store=store,
                decision_at=decision_at,
            )

    def test_explicit_post_close_replay_time_is_allowed(
        self, store: DuckDBStore
    ) -> None:
        df = load_universe(
            "2026-04-15",
            lookback=0,
            store=store,
            decision_at=datetime(2026, 4, 15, 18, 30, tzinfo=SHANGHAI),
        )

        assert "CLOSE[0]" in df.columns
        assert len(df) == 2

    def test_conflicted_status_remains_unknown(self, store: DuckDBStore) -> None:
        store.upsert_stock_status([
            SecurityStatusDaily(
                ts_code="300001.SZ",
                trade_date=date(2026, 4, 15),
                name=None,
                is_st=None,
                name_source="conflict",
                st_source=None,
                available_at=None,
                ingested_at=datetime(2026, 4, 17, tzinfo=UTC),
                conflict_reason="overlapping_namechange_intervals",
            )
        ])

        df = load_universe("2026-04-15", lookback=0, store=store)
        row = df.loc[df["ts_code"] == "300001.SZ"].iloc[0]

        assert pd.isna(row["name"])
        assert pd.isna(row["is_st"])
        assert pd.isna(row["IS_LIMIT_UP[0]"])

    def test_known_st_fact_without_name_remains_usable(
        self,
        store: DuckDBStore,
    ) -> None:
        trade_date = date(2026, 4, 15)
        store.upsert_stock_status([
            SecurityStatusDaily(
                ts_code="000001.SZ",
                trade_date=trade_date,
                name=None,
                is_st=False,
                name_source="unknown",
                st_source="tushare.stock_st_absence",
                available_at=datetime.combine(
                    trade_date,
                    time(9, 25),
                    SHANGHAI,
                ),
                ingested_at=datetime(2026, 4, 17, tzinfo=UTC),
            )
        ])

        df = load_universe("2026-04-15", lookback=0, store=store)
        row = df.loc[df["ts_code"] == "000001.SZ"].iloc[0]

        assert pd.isna(row["name"])
        assert row["is_st"] == False  # noqa: E712
        assert row["IS_LIMIT_UP[0]"] == False  # noqa: E712

    def test_missing_status_does_not_fallback(self, store: DuckDBStore) -> None:
        store._conn.execute(
            "DELETE FROM stock_status_daily WHERE ts_code = ? AND trade_date = ?",
            ["300001.SZ", "2026-04-15"],
        )

        df = load_universe("2026-04-15", lookback=0, store=store)
        row = df.loc[df["ts_code"] == "300001.SZ"].iloc[0]

        assert pd.isna(row["name"])
        assert pd.isna(row["is_st"])
        assert pd.isna(row["IS_LIMIT_UP[0]"])

    @pytest.mark.parametrize(
        ("mutation", "message"),
        [
            ("empty", "missing anchor 2026-04-15"),
            ("internal_gap", "incomplete.*2026-04-14"),
            ("closed_anchor", "2026-04-15.*closed"),
        ],
    )
    def test_authoritative_calendar_errors_are_explicit(
        self,
        store: DuckDBStore,
        mutation: str,
        message: str,
    ) -> None:
        if mutation == "empty":
            store._conn.execute("DELETE FROM trade_calendar")
        elif mutation == "internal_gap":
            store._conn.execute(
                "DELETE FROM trade_calendar WHERE exchange = ? AND cal_date = ?",
                ["SSE", "2026-04-14"],
            )
        else:
            store._conn.execute(
                "UPDATE trade_calendar SET is_open = FALSE "
                "WHERE exchange = ? AND cal_date = ?",
                ["SSE", "2026-04-15"],
            )

        with pytest.raises(screen_loader.ScreeningCalendarError, match=message):
            load_universe("2026-04-15", lookback=2, store=store)


class TestLoadUniverseBodyAndBasic:
    def test_body_upper_lower_in_wide_table(self, store: DuckDBStore) -> None:
        df = load_universe("2026-04-15", lookback=1, store=store)
        assert "BODY_UPPER[0]" in df.columns
        assert "BODY_LOWER[0]" in df.columns
        row = df.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert row["BODY_UPPER[0]"] == pytest.approx(12.5)
        assert row["BODY_LOWER[0]"] == pytest.approx(11.0)

    def test_body_upper_lower_at_offset_1(self, store: DuckDBStore) -> None:
        df = load_universe("2026-04-15", lookback=1, store=store)
        row = df.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert row["BODY_UPPER[1]"] == pytest.approx(11.0)
        assert row["BODY_LOWER[1]"] == pytest.approx(10.5)

    def test_circ_mv_in_wide_table(self, store: DuckDBStore) -> None:
        df = load_universe("2026-04-15", lookback=1, store=store)
        assert "CIRC_MV[0]" in df.columns
        assert "TOTAL_MV[0]" in df.columns
        assert "TURNOVER_RATE[0]" in df.columns
        row = df.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert row["CIRC_MV[0]"] == pytest.approx(5000000.0)
        assert row["TOTAL_MV[0]"] == pytest.approx(6000000.0)
        assert row["TURNOVER_RATE[0]"] == pytest.approx(2.0)

    def test_circ_mv_at_offset_1(self, store: DuckDBStore) -> None:
        df = load_universe("2026-04-15", lookback=1, store=store)
        row = df.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert row["CIRC_MV[1]"] == pytest.approx(4000000.0)

    def test_missing_daily_basic_gives_nan(self, store: DuckDBStore) -> None:
        """stock 000001.SZ has daily_basic only for 4/15, not 4/14 — offset 1 should be NaN."""
        df = load_universe("2026-04-15", lookback=1, store=store)
        row = df.loc[df["ts_code"] == "000001.SZ"].iloc[0]
        assert row["CIRC_MV[0]"] == pytest.approx(28000000.0)
        assert pd.isna(row["CIRC_MV[1]"])

    def test_empty_daily_basic_table_keeps_circ_mv_col(self, store: DuckDBStore) -> None:
        """5/29 事故回归：daily_basic 整表空（tushare 延迟）时，CIRC_MV[0] 列
        仍应存在且全 NaN，而非消失导致 screen 规则 KeyError。"""
        store._conn.execute("DELETE FROM daily_basic")
        df = load_universe("2026-04-15", lookback=1, store=store)
        assert "CIRC_MV[0]" in df.columns
        assert "TOTAL_MV[0]" in df.columns
        assert "TURNOVER_RATE[0]" in df.columns
        assert df["CIRC_MV[0]"].isna().all()

    def test_empty_daily_basic_circ_mv_lt_does_not_crash(
        self, store: DuckDBStore
    ) -> None:
        """5/29 事故端到端回归：daily_basic 整表空时，跑含 circ_mv_lt 的 screen
        不再 KeyError，而是把缺市值的股全部排除（circ_mv_lt 内部 fillna(inf)）。"""
        from rquant.screen.core import screen
        from rquant.screen.rules import circ_mv_lt

        store._conn.execute("DELETE FROM daily_basic")
        result = screen(
            trade_date="2026-04-15",
            rules=[circ_mv_lt(150)],
            store=store,
        )
        # 不抛 KeyError；缺市值 → 全部排除
        assert result.empty


class TestLoadUniverseAggregates:
    def test_max_aggregate(self, store: DuckDBStore) -> None:
        """300001.SZ has consecutive_limit_ups: [1,2,0,0,0,...,0,1] over window.
        Max in 8-day window ending 4/15 should be 2."""
        req = AggregateRequest(
            name="max_consec_ups_8d",
            source_table="daily_state",
            source_col="consecutive_limit_ups",
            agg_func="max",
            window=8,
        )
        df = load_universe(
            "2026-04-15", lookback=1, store=store, aggregate_requests=[req]
        )
        assert "max_consec_ups_8d" in df.columns
        row = df.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert row["max_consec_ups_8d"] == 2

    def test_any_aggregate(self, store: DuckDBStore) -> None:
        """300001.SZ has is_limit_down=True on 4/10. any in 8-day window should be True."""
        req = AggregateRequest(
            name="has_limit_down_8d",
            source_table="daily_state",
            source_col="is_limit_down",
            agg_func="any",
            window=8,
        )
        df = load_universe(
            "2026-04-15", lookback=1, store=store, aggregate_requests=[req]
        )
        row = df.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert row["has_limit_down_8d"]

    def test_count_nonzero_aggregate(self, store: DuckDBStore) -> None:
        """300001.SZ has is_limit_up=True on 4/7, 4/8, 4/15. Count in 8-day window should be >=2."""
        req = AggregateRequest(
            name="count_limit_up_8d",
            source_table="daily_state",
            source_col="is_limit_up",
            agg_func="count_nonzero",
            window=8,
        )
        df = load_universe(
            "2026-04-15", lookback=1, store=store, aggregate_requests=[req]
        )
        row = df.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert row["count_limit_up_8d"] >= 2

    def test_count_nonzero_with_exclude_offset(self, store: DuckDBStore) -> None:
        """Exclude offset=0 (4/15, which has is_limit_up=True for 300001.SZ).
        Count should decrease by 1."""
        req_with = AggregateRequest(
            name="count_limit_up_8d",
            source_table="daily_state",
            source_col="is_limit_up",
            agg_func="count_nonzero",
            window=8,
        )
        req_without = AggregateRequest(
            name="count_limit_up_8d_ex0",
            source_table="daily_state",
            source_col="is_limit_up",
            agg_func="count_nonzero",
            window=8,
            exclude_offset=0,
        )
        df = load_universe(
            "2026-04-15", lookback=1, store=store,
            aggregate_requests=[req_with, req_without],
        )
        row = df.loc[df["ts_code"] == "300001.SZ"].iloc[0]
        assert row["count_limit_up_8d_ex0"] == row["count_limit_up_8d"] - 1

    def test_empty_aggregates_no_extra_columns(self, store: DuckDBStore) -> None:
        df = load_universe("2026-04-15", lookback=1, store=store, aggregate_requests=[])
        assert "max_consec_ups_8d" not in df.columns

    def test_no_aggregates_param_backward_compatible(self, store: DuckDBStore) -> None:
        """Calling without aggregate_requests should work as before."""
        df = load_universe("2026-04-15", lookback=1, store=store)
        assert "CLOSE[0]" in df.columns

    @pytest.mark.parametrize(
        "mutation",
        ["missing_state", "null_source", "missing_status", "missing_calendar"],
    )
    def test_negative_aggregate_is_unknown_when_window_fact_is_unknown(
        self, store: DuckDBStore, mutation: str
    ) -> None:
        if mutation == "missing_state":
            store._conn.execute(
                "DELETE FROM daily_state WHERE ts_code = ? AND trade_date = ?",
                ["000001.SZ", "2026-04-09"],
            )
        elif mutation == "null_source":
            store._conn.execute(
                "UPDATE daily_state SET is_limit_down = NULL "
                "WHERE ts_code = ? AND trade_date = ?",
                ["000001.SZ", "2026-04-09"],
            )
        elif mutation == "missing_status":
            store._conn.execute(
                "DELETE FROM stock_status_daily WHERE ts_code = ? AND trade_date = ?",
                ["000001.SZ", "2026-04-09"],
            )
        else:
            store._conn.execute(
                "DELETE FROM trade_calendar WHERE exchange = ? AND cal_date = ?",
                ["SSE", "2026-04-12"],
            )
        req = AggregateRequest(
            name="has_limit_down_8d",
            source_table="daily_state",
            source_col="is_limit_down",
            agg_func="any",
            window=8,
        )

        df = load_universe(
            "2026-04-15", lookback=0, store=store, aggregate_requests=[req]
        )
        value = df.loc[df["ts_code"] == "000001.SZ", req.name].iloc[0]

        assert pd.isna(value)

    def test_negative_aggregate_is_unknown_for_incomplete_window(
        self, store: DuckDBStore
    ) -> None:
        req = AggregateRequest(
            name="has_limit_down_9d",
            source_table="daily_state",
            source_col="is_limit_down",
            agg_func="any",
            window=9,
        )

        df = load_universe(
            "2026-04-15", lookback=0, store=store, aggregate_requests=[req]
        )
        value = df.loc[df["ts_code"] == "000001.SZ", req.name].iloc[0]

        assert pd.isna(value)

    def test_known_violation_decides_false_amid_unknowns(
        self, store: DuckDBStore
    ) -> None:
        store._conn.execute(
            "DELETE FROM daily_state WHERE ts_code = ? AND trade_date = ?",
            ["300001.SZ", "2026-04-09"],
        )
        rule = no_limit_down_in_window(window=8)

        df = load_universe(
            "2026-04-15",
            lookback=0,
            store=store,
            aggregate_requests=rule.aggregate_requests,
        )
        row_index = df.index[df["ts_code"] == "300001.SZ"][0]

        assert df.loc[row_index, "has_limit_down_8d"]
        assert not rule(df).loc[row_index]

    def test_known_violation_is_unknown_when_calendar_is_incomplete(
        self, store: DuckDBStore
    ) -> None:
        store._conn.execute(
            "DELETE FROM trade_calendar WHERE exchange = ? AND cal_date = ?",
            ["SSE", "2026-04-12"],
        )
        rule = no_limit_down_in_window(window=8)

        df = load_universe(
            "2026-04-15",
            lookback=0,
            store=store,
            aggregate_requests=rule.aggregate_requests,
        )
        row_index = df.index[df["ts_code"] == "300001.SZ"][0]

        assert pd.isna(df.loc[row_index, "has_limit_down_8d"])
        assert not rule(df).loc[row_index]

    @pytest.mark.parametrize("mutation", ["missing_state", "missing_calendar"])
    def test_count_nonzero_requires_an_exact_complete_window(
        self, store: DuckDBStore, mutation: str
    ) -> None:
        if mutation == "missing_state":
            store._conn.execute(
                "DELETE FROM daily_state WHERE ts_code = ? AND trade_date = ?",
                ["300001.SZ", "2026-04-09"],
            )
        else:
            store._conn.execute(
                "DELETE FROM trade_calendar WHERE exchange = ? AND cal_date = ?",
                ["SSE", "2026-04-12"],
            )
        req = AggregateRequest(
            name="count_limit_up_8d",
            source_table="daily_state",
            source_col="is_limit_up",
            agg_func="count_nonzero",
            window=8,
        )

        df = load_universe(
            "2026-04-15", lookback=0, store=store, aggregate_requests=[req]
        )
        value = df.loc[df["ts_code"] == "300001.SZ", req.name].iloc[0]

        assert pd.isna(value)

    def test_nonviolating_max_is_unknown_when_window_is_incomplete(
        self, store: DuckDBStore
    ) -> None:
        store._conn.execute(
            "DELETE FROM daily_state WHERE ts_code = ? AND trade_date = ?",
            ["300001.SZ", "2026-04-09"],
        )
        req = AggregateRequest(
            name="max_consec_ups_8d",
            source_table="daily_state",
            source_col="consecutive_limit_ups",
            agg_func="max",
            window=8,
        )

        df = load_universe(
            "2026-04-15", lookback=0, store=store, aggregate_requests=[req]
        )
        value = df.loc[df["ts_code"] == "300001.SZ", req.name].iloc[0]

        assert pd.isna(value)


class TestWindowRulesIntegration:
    """End-to-end test: rule declares aggregate → loader generates SQL → rule evaluates."""

    def test_no_consec_ups_in_window_integration(self, store: DuckDBStore) -> None:
        """300001.SZ has max consecutive_limit_ups=2 in 8d window. threshold=3 → passes."""
        rule = no_consec_ups_in_window(threshold=3, window=8)
        reqs = rule.aggregate_requests
        df = load_universe("2026-04-15", lookback=0, store=store, aggregate_requests=reqs)
        mask = rule(df)
        row_mask = mask.loc[df["ts_code"] == "300001.SZ"]
        assert row_mask.iloc[0]

    def test_no_consec_ups_in_window_fails(self, store: DuckDBStore) -> None:
        """threshold=2: max_consec=2 NOT < 2 → fails."""
        rule = no_consec_ups_in_window(threshold=2, window=8)
        reqs = rule.aggregate_requests
        df = load_universe("2026-04-15", lookback=0, store=store, aggregate_requests=reqs)
        mask = rule(df)
        row_mask = mask.loc[df["ts_code"] == "300001.SZ"]
        assert not row_mask.iloc[0]

    def test_no_limit_down_in_window_integration(self, store: DuckDBStore) -> None:
        """300001.SZ has is_limit_down=True on 4/10. Window=8 covers 4/10 → fails."""
        rule = no_limit_down_in_window(window=8)
        reqs = rule.aggregate_requests
        df = load_universe("2026-04-15", lookback=0, store=store, aggregate_requests=reqs)
        mask = rule(df)
        row_mask = mask.loc[df["ts_code"] == "300001.SZ"]
        assert not row_mask.iloc[0]

    def test_no_limit_down_passes_for_clean_stock(self, store: DuckDBStore) -> None:
        """000001.SZ has no limit_down in any date → passes."""
        rule = no_limit_down_in_window(window=8)
        reqs = rule.aggregate_requests
        df = load_universe("2026-04-15", lookback=0, store=store, aggregate_requests=reqs)
        mask = rule(df)
        row_mask = mask.loc[df["ts_code"] == "000001.SZ"]
        assert row_mask.iloc[0]

    def test_has_prior_limit_up_integration(self, store: DuckDBStore) -> None:
        """300001.SZ has limit_up on 4/7 and 4/8 (excluding 4/15 at offset=0).
        With window=8, exclude_offset=0 → count >= 1 → passes."""
        rule = has_prior_limit_up(window=8, exclude_offset=0)
        reqs = rule.aggregate_requests
        df = load_universe("2026-04-15", lookback=0, store=store, aggregate_requests=reqs)
        mask = rule(df)
        row_mask = mask.loc[df["ts_code"] == "300001.SZ"]
        assert row_mask.iloc[0]


class TestLoadUniverseStateResilience:
    """daily_state 整表缺失时标准列补全，不让 screen 规则 KeyError（审计 PR1-H）。"""

    def test_empty_daily_state_keeps_scalar_and_offset_cols(
        self, store: DuckDBStore
    ) -> None:
        store._conn.execute("DELETE FROM daily_state")
        df = load_universe("2026-04-15", lookback=1, store=store)
        # is_st 独立来自 PIT status；daily_state 缺失只影响派生/板块事实。
        assert "is_st" in df.columns
        assert "is_bj" in df.columns
        assert "board_type" in df.columns
        assert df["is_st"].eq(False).all()
        assert df["is_bj"].isna().all()
        # STATE offset 列补全（含历史 offset [1]）
        assert "BODY_UPPER[0]" in df.columns
        assert "BODY_UPPER[1]" in df.columns
        assert df["BODY_UPPER[0]"].isna().all()

    def test_empty_daily_state_not_st_and_board_in_do_not_crash(
        self, store: DuckDBStore
    ) -> None:
        from rquant.screen.core import screen
        from rquant.screen.rules import board_in, not_st

        store._conn.execute("DELETE FROM daily_state")
        # not_st 引用 is_st、board_in 引用 board_type，整表缺时不应 KeyError
        result = screen(
            trade_date="2026-04-15",
            rules=[not_st(), board_in(["main", "gem"])],
            store=store,
        )
        # 不抛异常即通过（board_type 全 "" 不在白名单 → 全排除）
        assert result.empty

    def test_missing_historical_daily_bar_keeps_price_offset_columns(
        self, store: DuckDBStore
    ) -> None:
        store._conn.execute(
            "DELETE FROM daily_bar WHERE trade_date = ?",
            ["2026-04-14"],
        )

        df = load_universe("2026-04-15", lookback=1, store=store)

        assert "CLOSE[1]" in df.columns
        assert "VOL[1]" in df.columns
        assert df["CLOSE[1]"].isna().all()
        assert df["VOL[1]"].isna().all()
