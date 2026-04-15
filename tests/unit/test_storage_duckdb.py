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
