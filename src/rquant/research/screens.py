# ruff: noqa: E501 - SQL and preset tables read better unwrapped
"""Research presets -> screen_result in market.duckdb (input pools for portfolio backtests).

Each preset picks up to 30 names per trading day from liquid stocks that were not ST/退 on
that day (point-in-time via namechange)
(circulating cap top 1500, listed > 1 year). `extra` carries {"score": ...}.
"""

from __future__ import annotations

from datetime import date

import duckdb

from rquant.risk.neg_filter import st_pit_sql

UNIVERSE = """
WITH b AS (
  SELECT h.ts_code, h.trade_date, h.close, h.amount, h.pct_chg,
         avg(h.close) OVER w20 AS ma20,
         h.close / lag(h.close, 20) OVER w AS r20,
         h.close / lag(h.close, 120) OVER w AS r120,
         max(h.close) OVER (w ROWS BETWEEN 54 PRECEDING AND 1 PRECEDING) AS hi55,
         stddev_samp(h.pct_chg) OVER w20 AS vol20,
         avg(h.amount) OVER w20 AS amt20
  FROM daily_bar_hfq h
  WINDOW w AS (PARTITION BY h.ts_code ORDER BY h.trade_date),
         w20 AS (PARTITION BY h.ts_code ORDER BY h.trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
), u AS (
  SELECT b.*, d.pb, d.pe_ttm, d.circ_mv, s.name,
         row_number() OVER (PARTITION BY b.trade_date ORDER BY d.circ_mv DESC NULLS LAST) AS mv_rank
  FROM b JOIN daily_basic d USING (ts_code, trade_date)
  JOIN stock_basic s USING (ts_code)
  WHERE NOT {ST_PIT} AND s.list_date <= b.trade_date - INTERVAL 365 DAY
    AND b.trade_date BETWEEN ? AND ?
)
SELECT * FROM u WHERE mv_rank <= 1500
"""
# point-in-time ST/退 (namechange) instead of today's name: no look-ahead / survivorship bias
UNIVERSE = UNIVERSE.replace("{ST_PIT}", st_pit_sql("b.ts_code", "b.trade_date"))

PRESETS = {
    "research_mom20": ("20 日动量前 30", "r20", "DESC", "r20 IS NOT NULL"),
    "research_breakout55": ("55 日新高按成交额前 30", "amt20", "DESC", "close >= hi55"),
    "research_lowvol": ("20 日低波动前 30", "vol20", "ASC", "vol20 IS NOT NULL"),
    "research_value": ("低市净率前 30", "pb", "ASC", "pb > 0"),
    "research_trend": ("站上 20 日线且 120 日动量前 30", "r120", "DESC", "close > ma20 AND r120 IS NOT NULL"),
}


def build(con: duckdb.DuckDBPyConnection, start: date, end: date, top: int = 30) -> int:
    con.execute("""CREATE TABLE IF NOT EXISTS screen_result(trade_date DATE, ts_code VARCHAR,
      preset_name VARCHAR, name VARCHAR, close DOUBLE, pct_chg DOUBLE, extra VARCHAR)""")
    con.execute(f"CREATE OR REPLACE TEMP TABLE _u AS {UNIVERSE}", [start, end])
    total = 0
    for preset, (_title, key, order, cond) in PRESETS.items():
        con.execute("DELETE FROM screen_result WHERE preset_name = ? AND trade_date BETWEEN ? AND ?",
                    [preset, start, end])
        con.execute(f"""
          INSERT INTO screen_result
          SELECT trade_date, ts_code, ?, name, close, pct_chg,
                 json_object('score', round({top} + 1 - rk, 0))::VARCHAR
          FROM (SELECT *, row_number() OVER (PARTITION BY trade_date ORDER BY {key} {order}) AS rk
                FROM _u WHERE {cond}) WHERE rk <= {top}""", [preset])
        total += con.execute("SELECT COUNT(*) FROM screen_result WHERE preset_name = ?",
                             [preset]).fetchone()[0]
    con.execute("DROP TABLE _u")
    return total
