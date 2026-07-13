"""每日数据拉取：stock_basic → daily_bar → adj_factor → daily_basic → derive_state。

按 trade_date 模式拉全市场，适合每日自动化。
跳过 indicators（当前筛选预设不依赖）。
"""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import UTC, date, datetime
from typing import Literal, Protocol

import pandas as pd
import tushare as ts
from loguru import logger

from rquant.config import settings
from rquant.market_context import sync_market_sentiment
from rquant.security_status import (
    DailySecurityKey,
    SecurityStatusAdapter,
    prefetch_security_status,
)
from rquant.state import derive_state
from rquant.state.derive import DailyStateSeed
from rquant.storage import DuckDBStore

# Tushare 接口限流：~120ms 间隔
_API_SLEEP = 0.15
MARKET_INDEX_CODES = (
    "000001.SH",  # 上证指数
    "399001.SZ",  # 深证成指
    "399006.SZ",  # 创业板指
    "000300.SH",  # 沪深300
    "000905.SH",  # 中证500
    "000852.SH",  # 中证1000
)


class DailyIngestClient(Protocol):
    def stock_basic(self, **kwargs: object) -> pd.DataFrame: ...

    def daily(self, **kwargs: object) -> pd.DataFrame: ...

    def index_daily(self, **kwargs: object) -> pd.DataFrame: ...

    def adj_factor(self, **kwargs: object) -> pd.DataFrame: ...

    def daily_basic(self, **kwargs: object) -> pd.DataFrame: ...


def _load_daily_state_inputs(
    store: DuckDBStore,
    ts_code: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    joined = store._conn.execute(
        """
        SELECT daily.trade_date, daily.open, daily.high, daily.low,
               daily.close, daily.pre_close,
               calendar.pretrade_date AS expected_pretrade_date,
               status.ts_code AS status_ts_code,
               status.trade_date AS status_trade_date,
               status.name AS status_name,
               status.is_st AS status_is_st,
               status.available_at AS status_available_at,
               status.conflict_reason AS status_conflict_reason
        FROM daily_bar AS daily
        LEFT JOIN stock_status_daily AS status
          ON status.ts_code = daily.ts_code
         AND status.trade_date = daily.trade_date
        LEFT JOIN trade_calendar AS calendar
          ON calendar.exchange = 'SSE'
         AND calendar.cal_date = daily.trade_date
         AND calendar.is_open = TRUE
        WHERE daily.ts_code = ?
        ORDER BY daily.trade_date
        """,
        [ts_code],
    ).fetchdf()
    daily_columns = [
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "expected_pretrade_date",
    ]
    if joined.empty:
        return joined.reindex(columns=daily_columns), pd.DataFrame()
    status_source_columns = [
        "status_ts_code",
        "status_trade_date",
        "status_name",
        "status_is_st",
        "status_available_at",
        "status_conflict_reason",
    ]
    status = joined.loc[
        joined["status_ts_code"].notna(), status_source_columns
    ].rename(
        columns={
            "status_ts_code": "ts_code",
            "status_trade_date": "trade_date",
            "status_name": "name",
            "status_is_st": "is_st",
            "status_available_at": "available_at",
            "status_conflict_reason": "conflict_reason",
        }
    )
    status_columns = [
        "ts_code",
        "trade_date",
        "name",
        "is_st",
        "available_at",
        "conflict_reason",
    ]
    return joined[daily_columns].copy(), status[status_columns].copy()


def _load_target_daily_state_inputs(
    store: DuckDBStore,
    target_date: date,
    ts_codes: list[str],
) -> tuple[pd.DataFrame, dict[str, DailyStateSeed]]:
    """Load each state tail and the latest state before its first actual bar."""
    if not ts_codes:
        return pd.DataFrame(), {}
    joined = store._conn.execute(
        """
        WITH first_target AS (
            SELECT ts_code, min(trade_date) AS first_trade_date
            FROM daily_bar
            WHERE trade_date >= ?
              AND ts_code = ANY(?)
            GROUP BY ts_code
        ),
        predecessor AS (
            SELECT first.ts_code, first.first_trade_date,
                   state.trade_date, state.is_limit_up,
                   state.consecutive_limit_ups,
                   row_number() OVER (
                       PARTITION BY first.ts_code
                       ORDER BY state.trade_date DESC NULLS LAST
                   ) AS predecessor_rank
            FROM first_target AS first
            LEFT JOIN daily_state AS state
              ON state.ts_code = first.ts_code
             AND state.trade_date < first.first_trade_date
        )
        SELECT daily.ts_code, daily.trade_date,
               daily.open, daily.high, daily.low, daily.close, daily.pre_close,
               calendar.pretrade_date AS expected_pretrade_date,
               status.ts_code AS status_ts_code,
               status.name AS status_name,
               status.is_st AS status_is_st,
               status.available_at AS status_available_at,
               status.conflict_reason AS status_conflict_reason,
               predecessor.trade_date AS seed_trade_date,
               predecessor.is_limit_up AS seed_is_limit_up,
               predecessor.consecutive_limit_ups AS seed_consecutive_limit_ups
        FROM daily_bar AS daily
        LEFT JOIN trade_calendar AS calendar
          ON calendar.exchange = 'SSE'
         AND calendar.cal_date = daily.trade_date
         AND calendar.is_open = TRUE
        LEFT JOIN stock_status_daily AS status
          ON status.ts_code = daily.ts_code
         AND status.trade_date = daily.trade_date
        INNER JOIN first_target AS first
          ON first.ts_code = daily.ts_code
        LEFT JOIN predecessor
          ON predecessor.ts_code = daily.ts_code
         AND daily.trade_date = first.first_trade_date
         AND predecessor.predecessor_rank = 1
        WHERE daily.trade_date >= ?
          AND daily.ts_code = ANY(?)
        ORDER BY daily.ts_code, daily.trade_date
        """,
        [target_date, ts_codes, target_date, ts_codes],
    ).fetchdf()
    seeds: dict[str, DailyStateSeed] = {}
    for row in joined.to_dict(orient="records"):
        seed_trade_date = row["seed_trade_date"]
        if pd.isna(seed_trade_date):
            continue
        seed_is_limit_up = row["seed_is_limit_up"]
        seed_count = row["seed_consecutive_limit_ups"]
        seeds[str(row["ts_code"])] = DailyStateSeed(
            trade_date=pd.Timestamp(seed_trade_date).date(),
            is_limit_up=(
                None if pd.isna(seed_is_limit_up) else bool(seed_is_limit_up)
            ),
            consecutive_limit_ups=(
                None if pd.isna(seed_count) else int(seed_count)
            ),
        )
    return joined, seeds


def _derive_target_daily_states(
    target_rows: pd.DataFrame,
    seeds: dict[str, DailyStateSeed],
) -> pd.DataFrame:
    state_frames: list[pd.DataFrame] = []
    for ts_code_value, code_rows in target_rows.groupby("ts_code", sort=False):
        ts_code = str(ts_code_value)
        raw = code_rows[
            [
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "pre_close",
                "expected_pretrade_date",
            ]
        ].copy()
        status = code_rows.loc[
            code_rows["status_ts_code"].notna(),
            [
                "status_ts_code",
                "trade_date",
                "status_name",
                "status_is_st",
                "status_available_at",
                "status_conflict_reason",
            ],
        ].rename(
            columns={
                "status_ts_code": "ts_code",
                "status_name": "name",
                "status_is_st": "is_st",
                "status_available_at": "available_at",
                "status_conflict_reason": "conflict_reason",
            }
        )
        state_frames.append(
            derive_state(
                raw,
                ts_code=ts_code,
                status_daily=status,
                seed=seeds.get(ts_code),
            )
        )
    return (
        pd.concat(state_frames, ignore_index=True)
        if state_frames
        else pd.DataFrame()
    )


def ingest_daily(
    trade_date: str,
    *,
    pro: DailyIngestClient | None = None,
    status_adapter: SecurityStatusAdapter | None = None,
    writer_factory: Callable[[], DuckDBStore] = DuckDBStore,
    ingested_at: datetime | None = None,
    api_sleep: float = _API_SLEEP,
    sleep: Callable[[float], None] = time.sleep,
    state_mode: Literal["recompute_tail", "invalidate_tail"] = "recompute_tail",
) -> int:
    """拉取指定交易日的全市场数据并入库。

    所有远端状态事实先完成拉取与物化，再打开生产写连接。
    返回 daily_bar 行数（0 表示非交易日或数据未就绪）。
    """
    if state_mode not in {"recompute_tail", "invalidate_tail"}:
        raise ValueError(
            "state_mode must be 'recompute_tail' or 'invalidate_tail'"
        )
    pro = pro or ts.pro_api(settings.tushare_token_main)
    ds = trade_date.replace("-", "")
    target_date = datetime.strptime(trade_date, "%Y-%m-%d").date()
    resolved_ingested_at = ingested_at or datetime.now(UTC)

    logger.info("拉取 stock_basic...")
    df_basic = pro.stock_basic(
        exchange="",
        list_status="L",
        fields="ts_code,symbol,name,area,industry,list_date,market",
    )
    logger.info(f"stock_basic: {len(df_basic)} 行")
    sleep(api_sleep)

    logger.info(f"拉取 daily_bar {trade_date}...")
    df_daily = pro.daily(trade_date=ds)
    if df_daily is None or df_daily.empty:
        logger.info(f"{trade_date} 无 daily_bar 数据（非交易日或未就绪）")
        return 0
    df_daily = df_daily.copy()
    df_daily["trade_date"] = pd.to_datetime(
        df_daily["trade_date"], format="%Y%m%d"
    ).dt.date
    df_daily = df_daily.loc[df_daily["trade_date"] == target_date].copy()
    if df_daily.empty:
        logger.warning(f"{trade_date} daily_bar 响应不含请求日期，未写库")
        return 0
    bar_count = len(df_daily)
    sleep(api_sleep)

    if status_adapter is None:
        from rquant.adapter.tushare import TushareAdapter

        status_adapter = TushareAdapter()
    status_keys = [
        DailySecurityKey(ts_code=str(row.ts_code), trade_date=row.trade_date)
        for row in df_daily[["ts_code", "trade_date"]].itertuples(index=False)
    ]
    status_batch = prefetch_security_status(
        status_adapter,
        status_keys,
        ingested_at=resolved_ingested_at,
        request_interval_seconds=api_sleep,
        sleep=sleep,
    )

    logger.info(f"拉取 index_daily {trade_date}...")
    index_frames: list[pd.DataFrame] = []
    for index_code in MARKET_INDEX_CODES:
        try:
            index_df = pro.index_daily(
                ts_code=index_code,
                start_date=ds,
                end_date=ds,
            )
        except Exception as error:
            logger.warning(
                f"index_daily {index_code} {trade_date} 拉取失败: {error}"
            )
            continue
        if index_df is not None and not index_df.empty:
            index_frames.append(index_df)
        sleep(api_sleep)
    df_index = (
        pd.concat(index_frames, ignore_index=True)
        if index_frames
        else pd.DataFrame()
    )
    if not df_index.empty:
        df_index["trade_date"] = pd.to_datetime(
            df_index["trade_date"], format="%Y%m%d"
        ).dt.date

    logger.info(f"拉取 adj_factor {trade_date}...")
    try:
        df_factor = pro.adj_factor(trade_date=ds)
    except Exception as error:
        df_factor = None
        logger.warning(
            f"adj_factor {trade_date} 拉取失败，今日复权因子跳过: {error}"
        )
    if df_factor is not None and not df_factor.empty:
        factor_cols = ["ts_code", "trade_date", "adj_factor"]
        required_factor_cols = set(factor_cols)
        if required_factor_cols.issubset(df_factor.columns):
            df_factor = df_factor[factor_cols].copy()
            df_factor["trade_date"] = pd.to_datetime(
                df_factor["trade_date"], format="%Y%m%d"
            ).dt.date
        else:
            logger.warning(
                f"adj_factor {trade_date} 返回缺字段，跳过: "
                f"{sorted(required_factor_cols - set(df_factor.columns))}"
            )
            df_factor = None
    sleep(api_sleep)

    logger.info(f"拉取 daily_basic {trade_date}...")
    df_basic_mkt = pro.daily_basic(
        trade_date=ds,
        fields="ts_code,trade_date,turnover_rate,volume_ratio,total_mv,circ_mv",
    )
    if df_basic_mkt is not None and not df_basic_mkt.empty:
        df_basic_mkt = df_basic_mkt.copy()
        df_basic_mkt["trade_date"] = pd.to_datetime(
            df_basic_mkt["trade_date"], format="%Y%m%d"
        ).dt.date

    with writer_factory() as writer:
        codes = sorted(df_daily["ts_code"].astype(str).unique().tolist())
        transaction_open = False
        try:
            writer._conn.execute("BEGIN")
            transaction_open = True
            writer.upsert_stock_basic(df_basic)
            writer.upsert_daily(df_daily)
            logger.info(f"daily_bar: {bar_count} 行")
            writer.upsert_stock_status(
                status_batch.rows,
                transaction_mode="existing",
                require_daily_keys=True,
            )
            logger.info(
                f"stock_status_daily: {len(status_batch.rows)} 行 "
                f"(unknown={sum(row.is_st is None for row in status_batch.rows)}, "
                f"conflict={sum(row.conflict_reason is not None for row in status_batch.rows)})"
            )
            if not df_index.empty:
                writer.upsert_index_daily(df_index)
                logger.info(f"index_daily_bar: {len(df_index)} 行")
            else:
                logger.warning(f"index_daily {trade_date} 返回空")
            if df_factor is not None and not df_factor.empty:
                writer.upsert_adj_factor(df_factor)
                logger.info(f"adj_factor: {len(df_factor)} 行")
            else:
                logger.warning(
                    f"adj_factor {trade_date} 返回空，分钟复权将使用已有因子"
                )
            if df_basic_mkt is not None and not df_basic_mkt.empty:
                writer.upsert_daily_basic(df_basic_mkt)
                logger.info(f"daily_basic: {len(df_basic_mkt)} 行")
            else:
                logger.warning(
                    f"daily_basic {trade_date} 返回空（tushare 数据可能延迟未就绪），"
                    f"市值类筛选今日将失效；稍后可 `rquant run-daily {trade_date}` 重拉"
                )

            writer._conn.execute(
                "DELETE FROM daily_state "
                "WHERE trade_date >= ? AND ts_code = ANY(?)",
                [target_date, codes],
            )
            if state_mode == "recompute_tail":
                target_rows, seeds = _load_target_daily_state_inputs(
                    writer,
                    target_date,
                    codes,
                )
                target_state = _derive_target_daily_states(target_rows, seeds)
                total_state = writer.upsert_state(target_state)
                logger.info(f"state 完成: {total_state} 行, {len(codes)} 只")
                sentiment_rows = sync_market_sentiment(writer, trade_date)
                logger.info(f"market_sentiment_daily: {sentiment_rows} 行")
            else:
                logger.info(
                    f"state tail 已失效: {target_date}, {len(codes)} 只"
                )
            writer._conn.execute("COMMIT")
            transaction_open = False
        except BaseException as error:
            if transaction_open:
                try:
                    writer._conn.execute("ROLLBACK")
                except Exception as rollback_error:
                    error.add_note(f"daily ingest rollback failed: {rollback_error}")
            raise
    return bar_count
