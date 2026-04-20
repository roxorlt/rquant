"""每日数据拉取：stock_basic → daily_bar → daily_basic → derive_state。

按 trade_date 模式拉全市场，适合每日自动化。
跳过 adj_factor 和 indicators（当前筛选预设不依赖）。
"""

from __future__ import annotations

import time

import pandas as pd
import tushare as ts
from loguru import logger

from rquant.config import settings
from rquant.state import derive_state
from rquant.storage import DuckDBStore

# Tushare 接口限流：~120ms 间隔
_API_SLEEP = 0.15


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

        # 3. daily_basic（流通市值、换手率等）
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

        # 4. derive_state（逐只派生涨停/首板/连板等）
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
        return bar_count

    finally:
        if owns_store:
            store.close()
