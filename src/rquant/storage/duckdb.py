"""DuckDB 存储层：建表、upsert、查询。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
from loguru import logger

from rquant.config import settings
from rquant.storage.schema import ALL_DDL


class DuckDBStore:
    def __init__(self, path: Path | None = None, *, read_only: bool = False) -> None:
        self.path = path or settings.duckdb_path
        self._conn = duckdb.connect(str(self.path), read_only=read_only)
        if not read_only:
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

    def upsert_state(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0

        self._conn.register("state_tmp", df)
        self._conn.execute(
            """
            INSERT OR REPLACE INTO daily_state
            SELECT ts_code, trade_date,
                   is_st, is_bj, board_type, limit_pct,
                   limit_up_price, limit_down_price,
                   is_limit_up, is_limit_down,
                   is_first_limit_up, is_yiziban,
                   consecutive_limit_ups,
                   body_upper, body_lower
            FROM state_tmp
            """
        )
        self._conn.unregister("state_tmp")

        count = len(df)
        logger.info(f"DuckDB upsert daily_state: {count} 行")
        return count

    def count_state(self, ts_code: str | None = None) -> int:
        if ts_code:
            result = self._conn.execute(
                "SELECT COUNT(*) FROM daily_state WHERE ts_code = ?", [ts_code]
            ).fetchone()
        else:
            result = self._conn.execute("SELECT COUNT(*) FROM daily_state").fetchone()
        return result[0] if result else 0

    def upsert_daily_basic(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0

        self._conn.register("basic_mkt_tmp", df)
        self._conn.execute(
            """
            INSERT OR REPLACE INTO daily_basic
            SELECT ts_code, trade_date,
                   turnover_rate, volume_ratio,
                   total_mv, circ_mv
            FROM basic_mkt_tmp
            """
        )
        self._conn.unregister("basic_mkt_tmp")

        count = len(df)
        logger.info(f"DuckDB upsert daily_basic: {count} 行")
        return count

    def upsert_screen_result(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0

        self._conn.register("screen_result_tmp", df)
        self._conn.execute(
            """
            INSERT OR REPLACE INTO screen_result
            (trade_date, preset_name, ts_code, name, close, pct_chg, extra)
            SELECT trade_date, preset_name, ts_code, name, close, pct_chg, extra
            FROM screen_result_tmp
            """
        )
        self._conn.unregister("screen_result_tmp")

        count = len(df)
        logger.info(f"DuckDB upsert screen_result: {count} 行")
        return count

    def query_screen_result(
        self, trade_date: str, preset_name: str
    ) -> pd.DataFrame:
        return self._conn.execute(
            """
            SELECT ts_code, name, close, pct_chg, extra
            FROM screen_result
            WHERE strftime(trade_date, '%Y-%m-%d') = ?
              AND preset_name = ?
            ORDER BY ts_code
            """,
            [trade_date, preset_name],
        ).fetchdf()

    # ── pool2_watch ──

    def upsert_pool2_watch(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        self._conn.register("p2w_tmp", df)
        self._conn.execute(
            """
            INSERT OR REPLACE INTO pool2_watch
            (ts_code, entry_date, limit_up_date,
             body_upper, body_lower,
             level_40, level_30, level_20,
             stop_strong, stop_weak, status)
            SELECT ts_code, entry_date, limit_up_date,
                   body_upper, body_lower,
                   level_40, level_30, level_20,
                   stop_strong, stop_weak, status
            FROM p2w_tmp
            """
        )
        self._conn.unregister("p2w_tmp")
        count = len(df)
        logger.info(f"DuckDB upsert pool2_watch: {count} 行")
        return count

    def query_pool2_active(self) -> pd.DataFrame:
        return self._conn.execute(
            """
            SELECT ts_code, entry_date, limit_up_date,
                   body_upper, body_lower,
                   level_40, level_30, level_20,
                   stop_strong, stop_weak, status
            FROM pool2_watch
            WHERE status = 'active'
            ORDER BY entry_date DESC
            """
        ).fetchdf()

    def update_pool2_exit(
        self, ts_code: str, exit_date: date, exit_reason: str
    ) -> None:
        self._conn.execute(
            """
            UPDATE pool2_watch
            SET status = 'exited', exit_date = ?, exit_reason = ?
            WHERE ts_code = ?
            """,
            [exit_date, exit_reason, ts_code],
        )

    def remove_pool2(self, ts_code: str) -> None:
        self._conn.execute(
            "DELETE FROM pool2_watch WHERE ts_code = ?", [ts_code]
        )

    def query_pool2_all(self) -> pd.DataFrame:
        return self._conn.execute(
            """
            SELECT ts_code, entry_date, limit_up_date,
                   body_upper, body_lower,
                   level_40, level_30, level_20,
                   stop_strong, stop_weak,
                   status, exit_date, exit_reason
            FROM pool2_watch
            ORDER BY status, entry_date DESC
            """
        ).fetchdf()

    # ── monitor_event ──

    def upsert_monitor_event(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        self._conn.register("mev_tmp", df)
        self._conn.execute(
            """
            INSERT OR REPLACE INTO monitor_event
            (trade_date, ts_code, level, trigger_price, level_price,
             trigger_time, trigger_type, pool, body_upper, body_lower)
            SELECT trade_date, ts_code, level, trigger_price, level_price,
                   trigger_time, trigger_type, pool, body_upper, body_lower
            FROM mev_tmp
            """
        )
        self._conn.unregister("mev_tmp")
        count = len(df)
        logger.info(f"DuckDB upsert monitor_event: {count} 行")
        return count

    def query_monitor_events(
        self, trade_date: str, ts_code: str | None = None
    ) -> pd.DataFrame:
        if ts_code:
            return self._conn.execute(
                """
                SELECT * FROM monitor_event
                WHERE strftime(trade_date, '%Y-%m-%d') = ?
                  AND ts_code = ?
                ORDER BY trigger_time
                """,
                [trade_date, ts_code],
            ).fetchdf()
        return self._conn.execute(
            """
            SELECT * FROM monitor_event
            WHERE strftime(trade_date, '%Y-%m-%d') = ?
            ORDER BY trigger_time
            """,
            [trade_date],
        ).fetchdf()

    def count_daily_basic(self, ts_code: str | None = None) -> int:
        if ts_code:
            result = self._conn.execute(
                "SELECT COUNT(*) FROM daily_basic WHERE ts_code = ?", [ts_code]
            ).fetchone()
        else:
            result = self._conn.execute("SELECT COUNT(*) FROM daily_basic").fetchone()
        return result[0] if result else 0

    def get_state(
        self,
        ts_code: str,
        start: str | None = None,
        end: str | None = None,
    ) -> pd.DataFrame:
        params: list[str] = [ts_code]
        where = "ts_code = ?"
        if start:
            where += " AND trade_date >= ?"
            params.append(start)
        if end:
            where += " AND trade_date <= ?"
            params.append(end)
        sql = f"""
        SELECT ts_code,
               strftime(trade_date, '%Y-%m-%d') AS trade_date,
               is_st, is_bj, board_type, limit_pct,
               limit_up_price, limit_down_price,
               is_limit_up, is_limit_down,
               is_first_limit_up, is_yiziban,
               consecutive_limit_ups,
               body_upper, body_lower
        FROM daily_state
        WHERE {where}
        ORDER BY trade_date
        """
        return self._conn.execute(sql, params).fetchdf()

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
