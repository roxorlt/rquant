"""每日数据拉取：stock_basic → daily_bar → adj_factor → daily_basic → derive_state。

按 trade_date 模式拉全市场，适合每日自动化。
跳过 indicators（当前筛选预设不依赖）。
"""

from __future__ import annotations

import time

import pandas as pd
import tushare as ts
from loguru import logger

from rquant.config import settings
from rquant.market_context import sync_market_sentiment
from rquant.state import derive_state
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


def ingest_daily(trade_date: str, store: DuckDBStore | None = None) -> int:
    """拉取指定交易日的全市场数据并入库。

    流程：stock_basic → daily_bar → daily_basic → derive_state
    返回 daily_bar 行数（0 表示非交易日或数据未就绪）。
    """
    owns_store = store is None
    store = store or DuckDBStore()
    pro = ts.pro_api(settings.tushare_token_main)
    ds = trade_date.replace("-", "")

    try:
        # 1. stock_basic（刷新 ST 状态、新上市）
        logger.info("拉取 stock_basic...")
        df_basic = pro.stock_basic(
            exchange="", list_status="L",
            fields="ts_code,symbol,name,area,industry,list_date,market",
        )
        store.upsert_stock_basic(df_basic)
        logger.info(f"stock_basic: {len(df_basic)} 行")
        time.sleep(_API_SLEEP)

        # 2. daily_bar（全市场按日）
        logger.info(f"拉取 daily_bar {trade_date}...")
        df_daily = pro.daily(trade_date=ds)
        if df_daily is None or df_daily.empty:
            logger.info(f"{trade_date} 无 daily_bar 数据（非交易日或未就绪）")
            return 0

        df_daily["trade_date"] = pd.to_datetime(
            df_daily["trade_date"], format="%Y%m%d"
        ).dt.date
        store.upsert_daily(df_daily)
        bar_count = len(df_daily)
        logger.info(f"daily_bar: {bar_count} 行")
        time.sleep(_API_SLEEP)

        # 3. index_daily_bar（市场指数状态）
        logger.info(f"拉取 index_daily {trade_date}...")
        index_frames: list[pd.DataFrame] = []
        for index_code in MARKET_INDEX_CODES:
            try:
                index_df = pro.index_daily(
                    ts_code=index_code,
                    start_date=ds,
                    end_date=ds,
                )
            except Exception as e:
                logger.warning(f"index_daily {index_code} {trade_date} 拉取失败: {e}")
                continue
            if index_df is not None and not index_df.empty:
                index_frames.append(index_df)
            time.sleep(_API_SLEEP)
        if index_frames:
            df_index = pd.concat(index_frames, ignore_index=True)
            df_index["trade_date"] = pd.to_datetime(
                df_index["trade_date"], format="%Y%m%d"
            ).dt.date
            store.upsert_index_daily(df_index)
            logger.info(f"index_daily_bar: {len(df_index)} 行")
        else:
            logger.warning(f"index_daily {trade_date} 返回空")

        # 4. adj_factor（复权因子，供分钟价量分布跨除权/分红价位归一）
        logger.info(f"拉取 adj_factor {trade_date}...")
        try:
            df_factor = pro.adj_factor(trade_date=ds)
        except Exception as e:
            df_factor = None
            logger.warning(f"adj_factor {trade_date} 拉取失败，今日复权因子跳过: {e}")
        if df_factor is not None and not df_factor.empty:
            factor_cols = ["ts_code", "trade_date", "adj_factor"]
            required_factor_cols = set(factor_cols)
            if required_factor_cols.issubset(df_factor.columns):
                df_factor = df_factor[factor_cols].copy()
                df_factor["trade_date"] = pd.to_datetime(
                    df_factor["trade_date"], format="%Y%m%d"
                ).dt.date
                store.upsert_adj_factor(df_factor)
                logger.info(f"adj_factor: {len(df_factor)} 行")
            else:
                logger.warning(
                    f"adj_factor {trade_date} 返回缺字段，跳过: "
                    f"{sorted(required_factor_cols - set(df_factor.columns))}"
                )
        else:
            logger.warning(f"adj_factor {trade_date} 返回空，分钟复权将使用已有因子")
        time.sleep(_API_SLEEP)

        # 5. daily_basic（流通市值、换手率等）
        logger.info(f"拉取 daily_basic {trade_date}...")
        df_basic_mkt = pro.daily_basic(
            trade_date=ds,
            fields="ts_code,trade_date,turnover_rate,volume_ratio,total_mv,circ_mv",
        )
        if df_basic_mkt is not None and not df_basic_mkt.empty:
            df_basic_mkt["trade_date"] = pd.to_datetime(
                df_basic_mkt["trade_date"], format="%Y%m%d"
            ).dt.date
            store.upsert_daily_basic(df_basic_mkt)
            logger.info(f"daily_basic: {len(df_basic_mkt)} 行")
        else:
            # 5/29 真实事故：daily_bar 拉到了但 daily_basic tushare 延迟返回空，
            # 旧代码静默跳过 → screen 阶段 circ_mv_lt 引用 CIRC_MV[0] 崩 KeyError。
            # 现在 loader 会补 NaN 列不崩，但仍 WARNING 标记当天市值数据缺失，
            # 方便事后用 `rquant run-daily <date>` 重拉补全。
            logger.warning(
                f"daily_basic {trade_date} 返回空（tushare 数据可能延迟未就绪），"
                f"市值类筛选今日将失效；稍后可 `rquant run-daily {trade_date}` 重拉"
            )

        # 6. derive_state（逐只派生涨停/首板/连板等）
        basic_map = {r["ts_code"]: r["name"] for _, r in df_basic.iterrows()}
        codes = df_daily["ts_code"].unique().tolist()
        logger.info(f"派生 state: {len(codes)} 只...")

        total_state = 0
        for i, code in enumerate(codes):
            raw = store.query(
                f"SELECT trade_date, open, high, low, close, pre_close "
                f"FROM daily_bar WHERE ts_code = '{code}' ORDER BY trade_date"
            )
            if raw.empty:
                continue
            st = derive_state(raw, ts_code=code, name=basic_map.get(code))
            total_state += store.upsert_state(st)
            if (i + 1) % 1000 == 0:
                logger.info(f"  state: {i + 1}/{len(codes)}")

        logger.info(f"state 完成: {total_state} 行, {len(codes)} 只")
        sentiment_rows = sync_market_sentiment(store, trade_date)
        logger.info(f"market_sentiment_daily: {sentiment_rows} 行")
        return bar_count

    finally:
        if owns_store:
            store.close()
