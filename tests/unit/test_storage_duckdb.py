"""DuckDB 存储层单测：用临时库验证 upsert/查询逻辑。"""

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from rquant.storage.duckdb import DuckDBStore


@pytest.fixture
def tmp_store(tmp_path: Path) -> DuckDBStore:
    db_path = tmp_path / "test.duckdb"
    return DuckDBStore(path=db_path)


class TestDuckDBStore:
    def test_init_creates_schema(self, tmp_store: DuckDBStore) -> None:
        tables = tmp_store.query("SHOW TABLES")
        names = set(tables["name"].tolist())
        assert "daily_bar" in names
        assert "stock_basic" in names

    def test_upsert_daily_inserts_rows(self, tmp_store: DuckDBStore) -> None:
        df = pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": date(2024, 1, 2),
                    "open": 10.0,
                    "high": 10.5,
                    "low": 9.8,
                    "close": 10.3,
                    "pre_close": 9.9,
                    "change": 0.4,
                    "pct_chg": 4.04,
                    "vol": 100000.0,
                    "amount": 1030000.0,
                }
            ]
        )
        count = tmp_store.upsert_daily(df)
        assert count == 1
        assert tmp_store.count_daily("000001.SZ") == 1

    def test_upsert_daily_idempotent(self, tmp_store: DuckDBStore) -> None:
        df = pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": date(2024, 1, 2),
                    "open": 10.0,
                    "high": 10.5,
                    "low": 9.8,
                    "close": 10.3,
                    "pre_close": 9.9,
                    "change": 0.4,
                    "pct_chg": 4.04,
                    "vol": 100000.0,
                    "amount": 1030000.0,
                }
            ]
        )
        tmp_store.upsert_daily(df)
        tmp_store.upsert_daily(df)
        assert tmp_store.count_daily("000001.SZ") == 1

    def test_empty_df_returns_zero(self, tmp_store: DuckDBStore) -> None:
        assert tmp_store.upsert_daily(pd.DataFrame()) == 0


class TestAdjFactor:
    def _factor_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"ts_code": "000001.SZ", "trade_date": date(2024, 1, 2), "adj_factor": 1.0},
                {"ts_code": "000001.SZ", "trade_date": date(2024, 1, 3), "adj_factor": 1.0},
                {"ts_code": "000001.SZ", "trade_date": date(2024, 1, 4), "adj_factor": 2.0},
            ]
        )

    def _daily_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": date(2024, 1, 2),
                    "open": 10.0, "high": 10.5, "low": 9.8, "close": 10.0,
                    "pre_close": 9.9, "change": 0.1, "pct_chg": 1.01,
                    "vol": 100.0, "amount": 1000.0,
                },
                {
                    "ts_code": "000001.SZ",
                    "trade_date": date(2024, 1, 3),
                    "open": 10.0, "high": 11.5, "low": 10.0, "close": 11.0,
                    "pre_close": 10.0, "change": 1.0, "pct_chg": 10.0,
                    "vol": 120.0, "amount": 1320.0,
                },
                {
                    "ts_code": "000001.SZ",
                    "trade_date": date(2024, 1, 4),
                    "open": 11.5, "high": 12.5, "low": 11.0, "close": 12.0,
                    "pre_close": 11.0, "change": 1.0, "pct_chg": 9.09,
                    "vol": 200.0, "amount": 2400.0,
                },
            ]
        )

    def test_upsert_adj_factor_inserts_rows(self, tmp_store: DuckDBStore) -> None:
        count = tmp_store.upsert_adj_factor(self._factor_df())
        assert count == 3
        assert tmp_store.count_adj_factor("000001.SZ") == 3

    def test_upsert_adj_factor_idempotent(self, tmp_store: DuckDBStore) -> None:
        tmp_store.upsert_adj_factor(self._factor_df())
        tmp_store.upsert_adj_factor(self._factor_df())
        assert tmp_store.count_adj_factor("000001.SZ") == 3

    def test_empty_factor_df_returns_zero(self, tmp_store: DuckDBStore) -> None:
        assert tmp_store.upsert_adj_factor(pd.DataFrame()) == 0

    def test_get_daily_qfq_applies_factor(self, tmp_store: DuckDBStore) -> None:
        tmp_store.upsert_daily(self._daily_df())
        tmp_store.upsert_adj_factor(self._factor_df())

        df = tmp_store.get_daily_qfq("000001.SZ")

        assert len(df) == 3
        # ref_factor = max factor = 2.0
        # day1: raw 10 * 1.0/2.0 = 5.0
        # day2: raw 11 * 1.0/2.0 = 5.5
        # day3: raw 12 * 2.0/2.0 = 12.0
        assert df.iloc[0]["qfq_close"] == pytest.approx(5.0)
        assert df.iloc[1]["qfq_close"] == pytest.approx(5.5)
        assert df.iloc[2]["qfq_close"] == pytest.approx(12.0)
        assert df.iloc[0]["raw_close"] == 10.0
        assert df.iloc[2]["ref_factor"] == 2.0

    def test_get_daily_qfq_respects_date_range(self, tmp_store: DuckDBStore) -> None:
        tmp_store.upsert_daily(self._daily_df())
        tmp_store.upsert_adj_factor(self._factor_df())

        df = tmp_store.get_daily_qfq("000001.SZ", start="2024-01-03", end="2024-01-04")
        assert len(df) == 2
        assert df.iloc[0]["trade_date"] == "2024-01-03"
        assert df.iloc[1]["trade_date"] == "2024-01-04"


class TestIndicators:
    def test_upsert_indicators(self, tmp_store: DuckDBStore) -> None:
        df = pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": date(2024, 1, 2),
                    "ma5": 10.0, "ma10": None, "ma20": None, "ma60": None,
                    "rsi6": 55.0, "rsi14": 50.0,
                    "macd": 0.1, "macd_signal": 0.05, "macd_hist": 0.05,
                    "kdj_k": 60.0, "kdj_d": 55.0, "kdj_j": 70.0,
                }
            ]
        )
        count = tmp_store.upsert_indicators(df)
        assert count == 1
        assert tmp_store.count_indicators("000001.SZ") == 1

    def test_empty_indicators_returns_zero(self, tmp_store: DuckDBStore) -> None:
        assert tmp_store.upsert_indicators(pd.DataFrame()) == 0


class TestDailyState:
    def _state_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "ts_code": "600519.SH",
                    "trade_date": date(2024, 1, 2),
                    "is_st": False, "is_bj": False,
                    "board_type": "main", "limit_pct": 0.10,
                    "limit_up_price": 11.00, "limit_down_price": 9.00,
                    "is_limit_up": True, "is_limit_down": False,
                    "is_first_limit_up": True, "is_yiziban": False,
                    "consecutive_limit_ups": 1,
                    "body_upper": 11.0, "body_lower": 10.2,
                },
                {
                    "ts_code": "600519.SH",
                    "trade_date": date(2024, 1, 3),
                    "is_st": False, "is_bj": False,
                    "board_type": "main", "limit_pct": 0.10,
                    "limit_up_price": 12.10, "limit_down_price": 9.90,
                    "is_limit_up": True, "is_limit_down": False,
                    "is_first_limit_up": False, "is_yiziban": True,
                    "consecutive_limit_ups": 2,
                    "body_upper": 12.10, "body_lower": 12.10,
                },
            ]
        )

    def test_upsert_state_inserts_rows(self, tmp_store: DuckDBStore) -> None:
        count = tmp_store.upsert_state(self._state_df())
        assert count == 2
        assert tmp_store.count_state("600519.SH") == 2

    def test_upsert_state_idempotent(self, tmp_store: DuckDBStore) -> None:
        tmp_store.upsert_state(self._state_df())
        tmp_store.upsert_state(self._state_df())
        assert tmp_store.count_state("600519.SH") == 2

    def test_empty_state_returns_zero(self, tmp_store: DuckDBStore) -> None:
        assert tmp_store.upsert_state(pd.DataFrame()) == 0

    def test_get_state_reads_back(self, tmp_store: DuckDBStore) -> None:
        tmp_store.upsert_state(self._state_df())
        df = tmp_store.get_state("600519.SH")
        assert len(df) == 2
        assert bool(df.iloc[1]["is_yiziban"])
        assert df.iloc[1]["consecutive_limit_ups"] == 2

    def test_get_state_respects_date_range(self, tmp_store: DuckDBStore) -> None:
        tmp_store.upsert_state(self._state_df())
        df = tmp_store.get_state("600519.SH", start="2024-01-03")
        assert len(df) == 1
        assert df.iloc[0]["trade_date"] == "2024-01-03"


class TestDailyBasic:
    def _basic_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "ts_code": "600519.SH",
                    "trade_date": date(2026, 4, 15),
                    "turnover_rate": 0.35,
                    "volume_ratio": 1.2,
                    "total_mv": 20000000.0,
                    "circ_mv": 18000000.0,
                },
                {
                    "ts_code": "000001.SZ",
                    "trade_date": date(2026, 4, 15),
                    "turnover_rate": 0.80,
                    "volume_ratio": 0.9,
                    "total_mv": 3000000.0,
                    "circ_mv": 2800000.0,
                },
            ]
        )

    def test_upsert_daily_basic_inserts_rows(self, tmp_store: DuckDBStore) -> None:
        count = tmp_store.upsert_daily_basic(self._basic_df())
        assert count == 2
        assert tmp_store.count_daily_basic("600519.SH") == 1

    def test_upsert_daily_basic_idempotent(self, tmp_store: DuckDBStore) -> None:
        tmp_store.upsert_daily_basic(self._basic_df())
        tmp_store.upsert_daily_basic(self._basic_df())
        assert tmp_store.count_daily_basic() == 2

    def test_empty_daily_basic_returns_zero(self, tmp_store: DuckDBStore) -> None:
        assert tmp_store.upsert_daily_basic(pd.DataFrame()) == 0

    def test_daily_basic_table_exists_on_init(self, tmp_store: DuckDBStore) -> None:
        tables = tmp_store.query("SHOW TABLES")
        names = set(tables["name"].tolist())
        assert "daily_basic" in names
