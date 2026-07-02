"""市场/情绪日度特征。"""

from __future__ import annotations

from datetime import date

import pandas as pd
from pydantic import BaseModel, ConfigDict

from rquant.storage.duckdb import DuckDBStore


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


def build_market_sentiment(
    store: DuckDBStore,
    trade_date: date | str,
) -> MarketSentiment | None:
    """从已入库全市场 daily_state/daily_bar 聚合当日市场情绪。"""
    row = store._conn.execute(
        """
        SELECT
            ds.trade_date,
            COUNT(*)::INTEGER AS stock_count,
            SUM(CASE WHEN db.pct_chg > 0 THEN 1 ELSE 0 END)::INTEGER AS up_count,
            SUM(CASE WHEN db.pct_chg < 0 THEN 1 ELSE 0 END)::INTEGER AS down_count,
            SUM(CASE WHEN db.pct_chg = 0 THEN 1 ELSE 0 END)::INTEGER AS flat_count,
            SUM(CASE WHEN ds.is_limit_up THEN 1 ELSE 0 END)::INTEGER
                AS limit_up_count,
            SUM(CASE WHEN ds.is_first_limit_up THEN 1 ELSE 0 END)::INTEGER
                AS first_limit_up_count,
            SUM(CASE WHEN ds.is_limit_down THEN 1 ELSE 0 END)::INTEGER
                AS limit_down_count,
            SUM(CASE WHEN ds.is_yiziban THEN 1 ELSE 0 END)::INTEGER
                AS yiziban_count,
            MAX(COALESCE(ds.consecutive_limit_ups, 0))::INTEGER
                AS max_consecutive_limit_ups,
            SUM(CASE WHEN COALESCE(ds.consecutive_limit_ups, 0) >= 3 THEN 1 ELSE 0 END)
                ::INTEGER AS high_board_count,
            AVG(db.pct_chg) AS avg_pct_chg,
            MEDIAN(db.pct_chg) AS median_pct_chg,
            SUM(db.amount) AS total_amount
        FROM daily_state ds
        INNER JOIN daily_bar db
            ON ds.ts_code = db.ts_code AND ds.trade_date = db.trade_date
        WHERE ds.trade_date = ?
        GROUP BY ds.trade_date
        """,
        [trade_date],
    ).fetchone()
    if row is None:
        return None

    stock_count = _safe_int(row[1])
    up_count = _safe_int(row[2])
    limit_up_count = _safe_int(row[5])
    up_ratio_pct = up_count / stock_count * 100 if stock_count else 0.0
    limit_up_ratio_pct = (
        limit_up_count / stock_count * 100 if stock_count else 0.0
    )
    return MarketSentiment(
        trade_date=_as_date(row[0]),
        stock_count=stock_count,
        up_count=up_count,
        down_count=_safe_int(row[3]),
        flat_count=_safe_int(row[4]),
        limit_up_count=limit_up_count,
        first_limit_up_count=_safe_int(row[6]),
        limit_down_count=_safe_int(row[7]),
        yiziban_count=_safe_int(row[8]),
        max_consecutive_limit_ups=_safe_int(row[9]),
        high_board_count=_safe_int(row[10]),
        up_ratio_pct=round(up_ratio_pct, 4),
        limit_up_ratio_pct=round(limit_up_ratio_pct, 4),
        avg_pct_chg=round(_safe_float(row[11]), 4),
        median_pct_chg=round(_safe_float(row[12]), 4),
        total_amount=_safe_float(row[13]),
    )


def market_sentiment_to_frame(sentiment: MarketSentiment | None) -> pd.DataFrame:
    """转为存储层 upsert 可用的 DataFrame。"""
    if sentiment is None:
        return pd.DataFrame()
    return pd.DataFrame([sentiment.model_dump()])


def sync_market_sentiment(store: DuckDBStore, trade_date: date | str) -> int:
    """计算并落库一个交易日的市场情绪特征。"""
    return store.upsert_market_sentiment(
        market_sentiment_to_frame(build_market_sentiment(store, trade_date))
    )
