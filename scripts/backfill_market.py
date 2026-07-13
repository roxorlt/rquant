"""回填全市场历史数据：找出缺失的交易日，逐日 ingest。

用法：
    uv run python scripts/backfill_market.py [--start 2025-04-21] [--sleep 2]

限频策略：每日 ingest 之间 sleep N 秒（默认 2），避免 Tushare 风控。
"""

from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Callable, Sequence
from datetime import date

from loguru import logger
from tqdm import tqdm

from rquant.config import settings
from rquant.storage.duckdb import DuckDBStore


def get_dates_to_backfill(store: DuckDBStore, start: str) -> list[str]:
    df = store._conn.execute(
        """
        SELECT
            strftime(trade_date, '%Y-%m-%d') as d,
            COUNT(DISTINCT ts_code) as stocks
        FROM daily_bar
        WHERE trade_date >= ?
        GROUP BY trade_date
        ORDER BY trade_date
        """,
        [start],
    ).fetchdf()
    need = [row["d"] for _, row in df.iterrows() if row["stocks"] < 100]
    need.reverse()  # 从新到旧回填，最近的数据优先可用
    return need


def ingest_backfill_date(
    trade_date: str,
    *,
    ingest: Callable[..., int] | None = None,
) -> int:
    """Write one date while leaving its dependent state tail invalidated."""
    if ingest is None:
        from rquant.ingest import ingest_daily

        ingest = ingest_daily
    return ingest(trade_date, state_mode="invalidate_tail")


def finalize_backfill_state(
    successful_dates: Sequence[str],
    *,
    store_factory: Callable[[], DuckDBStore] = DuckDBStore,
    recompute: Callable[..., int] | None = None,
    recompute_sentiment: Callable[..., int] | None = None,
) -> tuple[int, int]:
    """Rebuild affected state once, then refresh its sentiment dependency."""
    parsed_dates = sorted({date.fromisoformat(value) for value in successful_dates})
    if not parsed_dates:
        return 0, 0
    if recompute is None:
        from rquant.market_backfill import recompute_daily_state

        recompute = recompute_daily_state
    if recompute_sentiment is None:
        from rquant.market_context import recompute_market_sentiment_range

        recompute_sentiment = recompute_market_sentiment_range

    with store_factory() as store:
        codes = [
            str(row[0])
            for row in store._conn.execute(
                """
                SELECT DISTINCT ts_code
                FROM daily_bar
                WHERE trade_date = ANY(?)
                ORDER BY ts_code
                """,
                [parsed_dates],
            ).fetchall()
        ]
        if not codes:
            return 0, 0
        state_rows = recompute(
            store,
            codes=codes,
            status_mode="verified_no_fetch",
        )
        last_date = store._conn.execute(
            "SELECT max(trade_date) FROM daily_bar"
        ).fetchone()[0]
        sentiment_rows = (
            recompute_sentiment(
                parsed_dates[0],
                last_date,
                store=store,
            )
            if last_date is not None
            else 0
        )
    return state_rows, sentiment_rows


def main() -> int:
    parser = argparse.ArgumentParser(description="回填全市场历史数据")
    parser.add_argument("--start", type=str, default="2025-04-21")
    parser.add_argument("--sleep", type=float, default=2.0)
    args = parser.parse_args()

    # 日志只写文件，终端留给进度条
    logger.remove()
    logger.add(
        settings.log_dir / "backfill_{time:YYYY-MM-DD}.log",
        level="INFO", rotation="00:00", enqueue=True,
    )

    print("正在扫描需要回填的交易日...")
    with DuckDBStore() as store:
        dates = get_dates_to_backfill(store, args.start)

    if not dates:
        print("✅ 无需回填，全部数据已完整")
        return 0

    total = len(dates)
    print(f"待回填: {total} 个交易日 ({dates[0]} ~ {dates[-1]})\n")

    success = 0
    fail = 0
    successful_dates: list[str] = []
    t0 = time.time()

    pbar = tqdm(total=total, desc="总进度", unit="天", ncols=70,
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]")

    for i, d in enumerate(dates):
        pbar.set_description(f"回填 {d}")
        t1 = time.time()

        try:
            count = ingest_backfill_date(d)
            dt = time.time() - t1
            if count > 0:
                success += 1
                successful_dates.append(d)
                tqdm.write(f"  ✓ {d}  {count} 行  {dt:.0f}s")
            else:
                fail += 1
                tqdm.write(f"  ✗ {d}  无数据（非交易日?）")
        except Exception as e:
            fail += 1
            tqdm.write(f"  ✗ {d}  失败: {e}")
            logger.exception(f"{d} 失败")

        pbar.update(1)

        if i < total - 1:
            time.sleep(args.sleep)

    pbar.close()
    if successful_dates:
        try:
            state_rows, sentiment_rows = finalize_backfill_state(
                successful_dates
            )
            print(
                f"状态尾部统一重算: state {state_rows} 行 / "
                f"sentiment {sentiment_rows} 行"
            )
        except Exception as error:
            fail += 1
            print(f"状态尾部统一重算失败: {error}")
            logger.exception("状态尾部统一重算失败")
    elapsed = (time.time() - t0) / 60
    print(f"\n✅ 回填完成: 成功 {success}, 跳过/失败 {fail}, 耗时 {elapsed:.0f} 分钟")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
