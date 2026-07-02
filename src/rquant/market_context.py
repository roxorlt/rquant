"""市场/情绪日度特征。"""

from __future__ import annotations

from datetime import date

import pandas as pd
from loguru import logger
from pydantic import BaseModel, ConfigDict

from rquant.storage.duckdb import DuckDBStore

_HIGH_LOOKBACK_DAYS = 60
_MA_DAYS = 20
# 窗口按个股自身 K 线行数计数，停牌股相对市场日历会缺行；
# 预取两倍市场交易日的历史，避免窗口被扫描起点截断误判"数据不足"。
_HISTORY_BUFFER_DAYS = _HIGH_LOOKBACK_DAYS * 2
_TEMPERATURE_COLUMNS = ("high_60d_ratio_pct", "above_ma20_ratio_pct")


class MarketSentiment(BaseModel):
    """由全市场日线和派生状态聚合出的交易日情绪特征。"""

    model_config = ConfigDict(frozen=True)

    trade_date: date
    stock_count: int
    up_count: int
    down_count: int
    flat_count: int
    limit_up_count: int
    first_limit_up_count: int
    limit_down_count: int
    yiziban_count: int
    max_consecutive_limit_ups: int
    high_board_count: int
    up_ratio_pct: float
    limit_up_ratio_pct: float
    avg_pct_chg: float
    median_pct_chg: float
    total_amount: float
    high_60d_ratio_pct: float | None = None
    above_ma20_ratio_pct: float | None = None


def _as_date(value: object) -> date:
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, date):
        return value
    if hasattr(value, "date"):
        return value.date()
    if isinstance(value, str):
        return date.fromisoformat(value[:10])
    msg = f"无法转换为日期: {value!r}"
    raise ValueError(msg)


def _safe_float(value: object) -> float:
    return 0.0 if pd.isna(value) else float(value)


def _safe_int(value: object) -> int:
    return 0 if pd.isna(value) else int(value)


_BASE_AGGREGATE_SQL = """
SELECT
    ds.trade_date,
    COUNT(*)::INTEGER AS stock_count,
    SUM(CASE WHEN db.pct_chg > 0 THEN 1 ELSE 0 END)::INTEGER AS up_count,
    SUM(CASE WHEN db.pct_chg < 0 THEN 1 ELSE 0 END)::INTEGER AS down_count,
    SUM(CASE WHEN db.pct_chg = 0 THEN 1 ELSE 0 END)::INTEGER AS flat_count,
    SUM(CASE WHEN ds.is_limit_up THEN 1 ELSE 0 END)::INTEGER AS limit_up_count,
    SUM(CASE WHEN ds.is_first_limit_up THEN 1 ELSE 0 END)::INTEGER
        AS first_limit_up_count,
    SUM(CASE WHEN ds.is_limit_down THEN 1 ELSE 0 END)::INTEGER AS limit_down_count,
    SUM(CASE WHEN ds.is_yiziban THEN 1 ELSE 0 END)::INTEGER AS yiziban_count,
    MAX(COALESCE(ds.consecutive_limit_ups, 0))::INTEGER AS max_consecutive_limit_ups,
    SUM(CASE WHEN COALESCE(ds.consecutive_limit_ups, 0) >= 3 THEN 1 ELSE 0 END)
        ::INTEGER AS high_board_count,
    AVG(db.pct_chg) AS avg_pct_chg,
    MEDIAN(db.pct_chg) AS median_pct_chg,
    SUM(db.amount) AS total_amount
FROM daily_state ds
INNER JOIN daily_bar db
    ON ds.ts_code = db.ts_code AND ds.trade_date = db.trade_date
WHERE ds.trade_date >= ? AND ds.trade_date <= ?
GROUP BY ds.trade_date
ORDER BY ds.trade_date
"""

# 市场温度：60 日新高占比 + 20 日均线上方占比，全部在 SQL 窗口函数里算，
# 不把全市场日线拉进 pandas。窗口行数不足（新股/长期停牌）不计入分子，仍计入分母。
_MARKET_TEMPERATURE_SQL = f"""
WITH bounds AS (
    SELECT COALESCE(MIN(td), DATE '1900-01-01') AS cutoff
    FROM (
        SELECT DISTINCT trade_date AS td
        FROM daily_bar
        WHERE trade_date < ?
        ORDER BY td DESC
        LIMIT {_HISTORY_BUFFER_DAYS}
    )
),
windowed AS (
    SELECT
        db.trade_date,
        db.close,
        db.vol,
        MAX(db.close) OVER w_high AS high_window_max,
        COUNT(db.close) OVER w_high AS high_window_rows,
        AVG(db.close) OVER w_ma AS ma_close,
        COUNT(db.close) OVER w_ma AS ma_window_rows
    FROM daily_bar db, bounds
    WHERE db.trade_date >= bounds.cutoff
      AND db.trade_date <= ?
    WINDOW
        w_high AS (
            PARTITION BY db.ts_code ORDER BY db.trade_date
            ROWS BETWEEN {_HIGH_LOOKBACK_DAYS - 1} PRECEDING AND CURRENT ROW
        ),
        w_ma AS (
            PARTITION BY db.ts_code ORDER BY db.trade_date
            ROWS BETWEEN {_MA_DAYS - 1} PRECEDING AND CURRENT ROW
        )
)
SELECT
    trade_date,
    COUNT(*)::INTEGER AS traded_count,
    SUM(CASE WHEN high_window_rows >= {_HIGH_LOOKBACK_DAYS} AND close >= high_window_max
             THEN 1 ELSE 0 END)::INTEGER AS high_60d_count,
    SUM(CASE WHEN ma_window_rows >= {_MA_DAYS} AND close > ma_close
             THEN 1 ELSE 0 END)::INTEGER AS above_ma20_count
FROM windowed
WHERE trade_date >= ?
  AND trade_date <= ?
  AND close IS NOT NULL
  AND vol > 0
GROUP BY trade_date
ORDER BY trade_date
"""


def _query_market_temperature(
    store: DuckDBStore,
    start_date: date | str,
    end_date: date | str,
) -> dict[date, tuple[float | None, float | None]]:
    """按交易日返回 (high_60d_ratio_pct, above_ma20_ratio_pct)。"""
    df = store._conn.execute(
        _MARKET_TEMPERATURE_SQL,
        [start_date, end_date, start_date, end_date],
    ).fetchdf()
    out: dict[date, tuple[float | None, float | None]] = {}
    for _, row in df.iterrows():
        traded = _safe_int(row["traded_count"])
        if traded <= 0:
            out[_as_date(row["trade_date"])] = (None, None)
            continue
        out[_as_date(row["trade_date"])] = (
            round(_safe_int(row["high_60d_count"]) / traded * 100, 4),
            round(_safe_int(row["above_ma20_count"]) / traded * 100, 4),
        )
    return out


def _row_to_sentiment(
    row: pd.Series,
    temperature: tuple[float | None, float | None],
) -> MarketSentiment:
    stock_count = _safe_int(row["stock_count"])
    up_count = _safe_int(row["up_count"])
    limit_up_count = _safe_int(row["limit_up_count"])
    up_ratio_pct = up_count / stock_count * 100 if stock_count else 0.0
    limit_up_ratio_pct = limit_up_count / stock_count * 100 if stock_count else 0.0
    high_60d_ratio_pct, above_ma20_ratio_pct = temperature
    return MarketSentiment(
        trade_date=_as_date(row["trade_date"]),
        stock_count=stock_count,
        up_count=up_count,
        down_count=_safe_int(row["down_count"]),
        flat_count=_safe_int(row["flat_count"]),
        limit_up_count=limit_up_count,
        first_limit_up_count=_safe_int(row["first_limit_up_count"]),
        limit_down_count=_safe_int(row["limit_down_count"]),
        yiziban_count=_safe_int(row["yiziban_count"]),
        max_consecutive_limit_ups=_safe_int(row["max_consecutive_limit_ups"]),
        high_board_count=_safe_int(row["high_board_count"]),
        up_ratio_pct=round(up_ratio_pct, 4),
        limit_up_ratio_pct=round(limit_up_ratio_pct, 4),
        avg_pct_chg=round(_safe_float(row["avg_pct_chg"]), 4),
        median_pct_chg=round(_safe_float(row["median_pct_chg"]), 4),
        total_amount=_safe_float(row["total_amount"]),
        high_60d_ratio_pct=high_60d_ratio_pct,
        above_ma20_ratio_pct=above_ma20_ratio_pct,
    )


def build_market_sentiment_range(
    store: DuckDBStore,
    start_date: date | str,
    end_date: date | str,
) -> list[MarketSentiment]:
    """聚合区间内每个交易日的市场情绪特征（含市场温度两列）。"""
    base = store._conn.execute(_BASE_AGGREGATE_SQL, [start_date, end_date]).fetchdf()
    if base.empty:
        return []
    temperature = _query_market_temperature(store, start_date, end_date)
    return [
        _row_to_sentiment(
            row,
            temperature.get(_as_date(row["trade_date"]), (None, None)),
        )
        for _, row in base.iterrows()
    ]


def build_market_sentiment(
    store: DuckDBStore,
    trade_date: date | str,
) -> MarketSentiment | None:
    """从已入库全市场 daily_state/daily_bar 聚合当日市场情绪。"""
    sentiments = build_market_sentiment_range(store, trade_date, trade_date)
    return sentiments[0] if sentiments else None


def _sentiments_to_frame(sentiments: list[MarketSentiment]) -> pd.DataFrame:
    if not sentiments:
        return pd.DataFrame()
    return pd.DataFrame([sentiment.model_dump() for sentiment in sentiments])


def market_sentiment_to_frame(sentiment: MarketSentiment | None) -> pd.DataFrame:
    """转为存储层 upsert 可用的 DataFrame。"""
    return _sentiments_to_frame([] if sentiment is None else [sentiment])


def _has_temperature_columns(store: DuckDBStore) -> bool:
    rows = store._conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'market_sentiment_daily'
        """
    ).fetchall()
    columns = {str(row[0]) for row in rows}
    return all(column in columns for column in _TEMPERATURE_COLUMNS)


def _persist_market_sentiment(store: DuckDBStore, frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0
    count = store.upsert_market_sentiment(frame)
    # 存储层 upsert 的列清单不含温度列，这里补一次 UPDATE；
    # 表结构未迁移（缺列）时跳过并告警，避免打断日常 sync。
    if not _has_temperature_columns(store):
        logger.warning("market_sentiment_daily 缺少市场温度列，本次跳过温度指标写入")
        return count
    store._conn.register(
        "market_temperature_tmp",
        frame[["trade_date", *_TEMPERATURE_COLUMNS]],
    )
    store._conn.execute(
        """
        UPDATE market_sentiment_daily
        SET high_60d_ratio_pct = t.high_60d_ratio_pct,
            above_ma20_ratio_pct = t.above_ma20_ratio_pct
        FROM market_temperature_tmp t
        WHERE market_sentiment_daily.trade_date = t.trade_date
        """
    )
    store._conn.unregister("market_temperature_tmp")
    return count


def sync_market_sentiment(store: DuckDBStore, trade_date: date | str) -> int:
    """计算并落库一个交易日的市场情绪特征。"""
    return _persist_market_sentiment(
        store,
        market_sentiment_to_frame(build_market_sentiment(store, trade_date)),
    )


def recompute_market_sentiment_range(
    start_date: str | date,
    end_date: str | date,
    store: DuckDBStore | None = None,
) -> int:
    """重算区间内每个交易日的全部 market_sentiment_daily 字段并 upsert。

    返回写入行数；store 缺省时自建写连接并在结束后关闭。
    """
    owns_store = store is None
    store = store or DuckDBStore()
    try:
        sentiments = build_market_sentiment_range(store, start_date, end_date)
        count = _persist_market_sentiment(store, _sentiments_to_frame(sentiments))
        logger.info(f"market_sentiment_daily 重算完成: {start_date}~{end_date}, {count} 行")
        return count
    finally:
        if owns_store:
            store.close()
