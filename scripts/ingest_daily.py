"""一次性拉取历史日线，并用逐日证券状态派生 daily_state。

示例：
    uv run python scripts/ingest_daily.py \
        --codes 000001.SZ,600519.SH --start 2024-01-01 --end 2024-12-31
"""

from __future__ import annotations

import argparse
import time
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime, timedelta
from typing import Protocol

import pandas as pd
from loguru import logger

from rquant.adapter import TushareAdapter
from rquant.indicator import compute_indicators
from rquant.ingest import (
    _derive_target_daily_states,
    _load_target_daily_state_inputs,
)
from rquant.logging import setup_logging
from rquant.security_status import (
    DEFAULT_REQUEST_INTERVAL_SECONDS,
    DailySecurityKey,
    SecurityStatusAdapter,
    prefetch_security_status,
)
from rquant.storage import DuckDBStore


class HistoricalIngestAdapter(SecurityStatusAdapter, Protocol):
    def daily(
        self, *, ts_codes: list[str], start: date, end: date
    ) -> pd.DataFrame: ...

    def adj_factor(
        self, *, ts_codes: list[str], start: date, end: date
    ) -> pd.DataFrame: ...

    def daily_basic(
        self, *, ts_codes: list[str], trade_date: date
    ) -> pd.DataFrame: ...

    def stock_basic(self) -> pd.DataFrame: ...


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _filter_requested_market_rows(
    frame: pd.DataFrame,
    *,
    codes: Sequence[str],
    start: date,
    end: date,
) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    if "ts_code" not in frame.columns or "trade_date" not in frame.columns:
        raise ValueError("provider market response is missing ts_code or trade_date")
    filtered = frame.copy()
    filtered["trade_date"] = pd.to_datetime(filtered["trade_date"]).dt.date
    return filtered.loc[
        filtered["ts_code"].isin(codes)
        & filtered["trade_date"].between(start, end)
    ].copy()


def run_historical_ingest(
    ts_codes: Sequence[str],
    start: date,
    end: date,
    *,
    adapter: HistoricalIngestAdapter | None = None,
    store_factory: Callable[[], DuckDBStore] = DuckDBStore,
    ingested_at: datetime | None = None,
    status_request_interval_seconds: float = DEFAULT_REQUEST_INTERVAL_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
    compute: Callable[[pd.DataFrame], pd.DataFrame] = compute_indicators,
) -> int:
    """Write requested facts and their dependent future state tail atomically."""
    codes = list(dict.fromkeys(code.strip() for code in ts_codes if code.strip()))
    if not codes:
        raise ValueError("at least one ts_code is required")
    if start > end:
        raise ValueError("start must not be after end")
    resolved_adapter = adapter or TushareAdapter()
    resolved_ingested_at = ingested_at or datetime.now(UTC)

    df_daily = resolved_adapter.daily(ts_codes=codes, start=start, end=end)
    if df_daily.empty:
        logger.warning("daily 无数据返回，退出")
        return 1
    df_daily = _filter_requested_market_rows(
        df_daily,
        codes=codes,
        start=start,
        end=end,
    )
    if df_daily.empty:
        logger.warning("daily 未返回请求代码，退出")
        return 1

    df_factor = resolved_adapter.adj_factor(
        ts_codes=codes, start=start, end=end
    )
    df_factor = _filter_requested_market_rows(
        df_factor,
        codes=codes,
        start=start,
        end=end,
    )

    logger.info("开始拉取 daily_basic（按日查询）")
    all_basic_dfs: list[pd.DataFrame] = []
    current = start
    while current <= end:
        try:
            day_basic = resolved_adapter.daily_basic(
                ts_codes=codes, trade_date=current
            )
            if not day_basic.empty:
                all_basic_dfs.append(
                    day_basic.loc[day_basic["ts_code"].isin(codes)].copy()
                )
        except Exception as error:
            logger.warning(f"daily_basic {current} 拉取失败，跳过：{error}")
        current += timedelta(days=1)
    df_all_basic = (
        pd.concat(all_basic_dfs, ignore_index=True)
        if all_basic_dfs
        else pd.DataFrame()
    )
    df_all_basic = _filter_requested_market_rows(
        df_all_basic,
        codes=codes,
        start=start,
        end=end,
    )

    df_basic = resolved_adapter.stock_basic()
    if not df_basic.empty:
        df_basic = df_basic.loc[df_basic["ts_code"].isin(codes)].copy()

    status_keys = [
        DailySecurityKey(ts_code=str(row.ts_code), trade_date=row.trade_date)
        for row in df_daily[["ts_code", "trade_date"]].itertuples(index=False)
    ]
    status_batch = prefetch_security_status(
        resolved_adapter,
        status_keys,
        ingested_at=resolved_ingested_at,
        request_interval_seconds=status_request_interval_seconds,
        sleep=sleep,
    )
    with store_factory() as store:
        affected_codes = sorted(df_daily["ts_code"].astype(str).unique())
        transaction_open = False
        try:
            store._conn.execute("BEGIN")
            transaction_open = True
            n_daily = store.upsert_daily(df_daily)
            store.upsert_stock_status(
                status_batch.rows,
                transaction_mode="existing",
                require_daily_keys=True,
            )
            n_factor = (
                store.upsert_adj_factor(df_factor) if not df_factor.empty else 0
            )
            n_basic = (
                store.upsert_stock_basic(df_basic) if not df_basic.empty else 0
            )
            n_daily_basic = (
                store.upsert_daily_basic(df_all_basic)
                if not df_all_basic.empty
                else 0
            )
            logger.info(
                f"入库完成：daily {n_daily} / adj_factor {n_factor} "
                f"/ daily_basic {n_daily_basic} / stock_basic {n_basic} "
                f"/ stock_status_daily {len(status_batch.rows)}"
            )

            total_ind = 0
            for code in affected_codes:
                df_qfq = store.get_daily_qfq(code)
                if df_qfq.empty:
                    continue
                df_ind = compute(df_qfq)
                if not df_ind.empty:
                    tail_end = pd.to_datetime(
                        df_qfq["trade_date"]
                    ).dt.date.max()
                    scoped_indicators = _filter_requested_market_rows(
                        df_ind,
                        codes=[code],
                        start=start,
                        end=tail_end,
                    )
                    if not scoped_indicators.empty:
                        total_ind += store.upsert_indicators(scoped_indicators)
            logger.info(f"指标计算完成：{total_ind} 行")

            target_rows, seeds = _load_target_daily_state_inputs(
                store,
                start,
                affected_codes,
            )
            target_state = _derive_target_daily_states(target_rows, seeds)
            store._conn.execute(
                "DELETE FROM daily_state "
                "WHERE trade_date >= ? AND ts_code = ANY(?)",
                [start, affected_codes],
            )
            total_state = (
                store.upsert_state(target_state) if not target_state.empty else 0
            )
            logger.info(f"派生状态计算完成：{total_state} 行")

            store._conn.execute("COMMIT")
            transaction_open = False
        except BaseException as error:
            if transaction_open:
                try:
                    store._conn.execute("ROLLBACK")
                except Exception as rollback_error:
                    error.add_note(
                        f"historical ingest rollback failed: {rollback_error}"
                    )
            raise

        for code in affected_codes:
            logger.info(
                f"  {code}: daily {store.count_daily(code)} "
                f"/ adj_factor {store.count_adj_factor(code)} "
                f"/ daily_basic {store.count_daily_basic(code)} "
                f"/ indicator {store.count_indicators(code)} "
                f"/ state {store.count_state(code)}"
            )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest daily OHLCV from Tushare")
    parser.add_argument(
        "--codes",
        required=True,
        help="Tushare ts_code 列表，逗号分隔，如 000001.SZ,600519.SH",
    )
    parser.add_argument("--start", type=parse_date, required=True)
    parser.add_argument("--end", type=parse_date, required=True)
    args = parser.parse_args()

    setup_logging()
    codes = [code.strip() for code in args.codes.split(",") if code.strip()]
    logger.info(f"开始拉取 {len(codes)} 只股票的日线 [{args.start} → {args.end}]")
    return run_historical_ingest(codes, args.start, args.end)


if __name__ == "__main__":
    raise SystemExit(main())
