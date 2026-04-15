"""DuckDB 存储层：建表、upsert、查询。"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
from loguru import logger

from rquant.config import settings
from rquant.storage.schema import ALL_DDL


class DuckDBStore:
    def __init__(self, path: Path | None = None) -> None:
        self.path = path or settings.duckdb_path
        self._conn = duckdb.connect(str(self.path))
        self._init_schema()

    def _init_schema(self) -> None:
        for ddl in ALL_DDL:
            self._conn.execute(ddl)

    def upsert_daily(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0

        self._conn.register("daily_tmp", df)
        self._conn.execute(
            """
            INSERT OR REPLACE INTO daily_bar
            SELECT
                ts_code, trade_date,
                open, high, low, close,
                pre_close, change, pct_chg,
                vol, amount
            FROM daily_tmp
            """
        )
        self._conn.unregister("daily_tmp")

        count = len(df)
        logger.info(f"DuckDB upsert daily_bar: {count} 行")
        return count

    def upsert_adj_factor(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0

        self._conn.register("factor_tmp", df)
        self._conn.execute(
            """
            INSERT OR REPLACE INTO adj_factor
            SELECT ts_code, trade_date, adj_factor
            FROM factor_tmp
            """
        )
        self._conn.unregister("factor_tmp")

        count = len(df)
        logger.info(f"DuckDB upsert adj_factor: {count} 行")
        return count

    def get_daily_qfq(
        self,
        ts_code: str,
        start: str | None = None,
        end: str | None = None,
    ) -> pd.DataFrame:
        """返回某只股票的前复权日线。

        前复权公式：qfq[t] = raw[t] * adj_factor[t] / adj_factor[latest]
        参考因子 = 该股票 adj_factor 表中最大 trade_date 对应的因子。

        同时返回原始价和 qfq 价，方便对比核验。
        """
        params: list[str] = [ts_code]
        where = "db.ts_code = ?"
        if start:
            where += " AND db.trade_date >= ?"
            params.append(start)
        if end:
            where += " AND db.trade_date <= ?"
            params.append(end)

        sql = f"""
        WITH ref AS (
            SELECT ts_code, adj_factor AS ref_factor
            FROM adj_factor
            WHERE ts_code = ?
              AND trade_date = (
                  SELECT MAX(trade_date) FROM adj_factor WHERE ts_code = ?
              )
        )
        SELECT
            db.ts_code,
            strftime(db.trade_date, '%Y-%m-%d') AS trade_date,
            db.open  AS raw_open,
            db.close AS raw_close,
            db.open  * af.adj_factor / r.ref_factor AS qfq_open,
            db.high  * af.adj_factor / r.ref_factor AS qfq_high,
            db.low   * af.adj_factor / r.ref_factor AS qfq_low,
            db.close * af.adj_factor / r.ref_factor AS qfq_close,
            db.vol,
            af.adj_factor,
            r.ref_factor
        FROM daily_bar db
        INNER JOIN adj_factor af
            ON db.ts_code = af.ts_code AND db.trade_date = af.trade_date
        INNER JOIN ref r
            ON db.ts_code = r.ts_code
        WHERE {where}
        ORDER BY db.trade_date
        """
        ref_params = [ts_code, ts_code]
        return self._conn.execute(sql, ref_params + params).fetchdf()

    def count_adj_factor(self, ts_code: str | None = None) -> int:
        if ts_code:
            result = self._conn.execute(
                "SELECT COUNT(*) FROM adj_factor WHERE ts_code = ?", [ts_code]
            ).fetchone()
        else:
            result = self._conn.execute("SELECT COUNT(*) FROM adj_factor").fetchone()
        return result[0] if result else 0

    def upsert_indicators(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0

        self._conn.register("ind_tmp", df)
        self._conn.execute(
            """
            INSERT OR REPLACE INTO daily_indicator
            SELECT ts_code, trade_date,
                   ma5, ma10, ma20, ma60,
                   rsi6, rsi14,
                   macd, macd_signal, macd_hist,
                   kdj_k, kdj_d, kdj_j
            FROM ind_tmp
            """
        )
        self._conn.unregister("ind_tmp")

        count = len(df)
        logger.info(f"DuckDB upsert daily_indicator: {count} 行")
        return count

    def count_indicators(self, ts_code: str | None = None) -> int:
        if ts_code:
            result = self._conn.execute(
                "SELECT COUNT(*) FROM daily_indicator WHERE ts_code = ?", [ts_code]
            ).fetchone()
        else:
            result = self._conn.execute("SELECT COUNT(*) FROM daily_indicator").fetchone()
        return result[0] if result else 0

    def upsert_stock_basic(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0

        df = df.copy()
        if "list_date" in df.columns:
            df["list_date"] = pd.to_datetime(df["list_date"], format="%Y%m%d").dt.date

        self._conn.register("basic_tmp", df)
        self._conn.execute(
            """
            INSERT OR REPLACE INTO stock_basic
            (ts_code, symbol, name, area, industry, list_date, market)
            SELECT ts_code, symbol, name, area, industry, list_date, market
            FROM basic_tmp
            """
        )
        self._conn.unregister("basic_tmp")

        count = len(df)
        logger.info(f"DuckDB upsert stock_basic: {count} 行")
        return count

    def query(self, sql: str) -> pd.DataFrame:
        return self._conn.execute(sql).fetchdf()

    def count_daily(self, ts_code: str | None = None) -> int:
        if ts_code:
            result = self._conn.execute(
                "SELECT COUNT(*) FROM daily_bar WHERE ts_code = ?", [ts_code]
            ).fetchone()
        else:
            result = self._conn.execute("SELECT COUNT(*) FROM daily_bar").fetchone()
        return result[0] if result else 0

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> DuckDBStore:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
