"""load_universe() 单测：用临时 DuckDB 实例塞数据。"""

from datetime import date

import pandas as pd
import pytest

from rquant.screen.loader import load_universe
from rquant.storage.duckdb import DuckDBStore


@pytest.fixture
def store(tmp_path) -> DuckDBStore:
    s = DuckDBStore(path=tmp_path / "test.duckdb")

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
         "is_st": False, "is_bj": False, "board_type": "gem",
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
        assert row["name"] == "特锐德"

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
