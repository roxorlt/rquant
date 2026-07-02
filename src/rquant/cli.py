"""CLI 入口：rquant serve / rquant run-daily / rquant ingest。"""

from __future__ import annotations

import argparse
import signal
import sys
import time
from datetime import date
from datetime import time as dtime
from pathlib import Path

from loguru import logger

from rquant.logging import setup_logging
from rquant.storage.duckdb import DuckDBStore, open_readonly_store

# 重试配置
_RETRY_COUNT = 3
_RETRY_INTERVAL = 900  # 数据未就绪：15 分钟（等 tushare 数据出来）
_NETWORK_RETRY_INTERVAL = 60  # 网络异常：1 分钟（tushare 抖动通常很快恢复）


def _ingest_with_retry(trade_date: str) -> int:
    """拉取数据，最多重试 _RETRY_COUNT 次。

    两类可重试情况：
    - ingest 抛异常（短间隔重试）：覆盖两种 tushare 故障——
      a) 网络层异常（ReadTimeout / ConnectionError，6/4 事故）；
      b) 服务端业务错误（限频 / 接口临时故障，tushare 客户端抛**裸 Exception**
         而非 RequestException）。两者都该短重试。故用 `except Exception`——
         真正的代码 bug 也会被重试，但重试耗尽后 `raise` 抛出不吞（daily 非实时，
         延迟暴露可接受），换取对 tushare 抖动的鲁棒性。
    - 数据未就绪（bar_count == 0）：非交易日或 tushare 数据当天还没出，长间隔重试。
    """
    from rquant.ingest import ingest_daily

    for attempt in range(1, _RETRY_COUNT + 1):
        try:
            bar_count = ingest_daily(trade_date)
        except Exception as e:
            if attempt < _RETRY_COUNT:
                logger.warning(
                    f"ingest 异常 {type(e).__name__}，"
                    f"{_NETWORK_RETRY_INTERVAL}s 后重试 "
                    f"({attempt}/{_RETRY_COUNT}): {e}"
                )
                time.sleep(_NETWORK_RETRY_INTERVAL)
                continue
            logger.error(
                f"{trade_date} ingest 重试 {_RETRY_COUNT} 次仍失败: {e}"
            )
            raise

        if bar_count > 0:
            return bar_count
        if attempt < _RETRY_COUNT:
            logger.warning(
                f"数据未就绪，{_RETRY_INTERVAL // 60} 分钟后重试 "
                f"({attempt}/{_RETRY_COUNT})"
            )
            time.sleep(_RETRY_INTERVAL)

    logger.error(f"{trade_date} 数据拉取失败（重试 {_RETRY_COUNT} 次后仍无数据）")
    return 0


def _bridge_apscheduler_logging() -> None:
    """将 APScheduler 的标准 logging 桥接到 loguru。"""
    import logging

    class LoguruHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            level = record.levelname
            logger.opt(depth=6, exception=record.exc_info).log(level, record.getMessage())

    logging.getLogger("apscheduler").handlers = [LoguruHandler()]
    logging.getLogger("apscheduler").setLevel(logging.INFO)


def _parse_hhmm(value: str) -> dtime:
    """解析 CLI 的 HH:MM 时间。"""
    try:
        hour, minute = value.split(":", 1)
        return dtime(int(hour), int(minute))
    except ValueError as e:
        msg = f"时间格式应为 HH:MM: {value}"
        raise argparse.ArgumentTypeError(msg) from e


def cmd_serve(args: argparse.Namespace) -> int:
    """启动 APScheduler 常驻进程。"""
    from apscheduler.schedulers.blocking import BlockingScheduler

    from rquant.pipeline import run_daily_pipeline

    setup_logging()
    _bridge_apscheduler_logging()

    scheduler = BlockingScheduler()

    @scheduler.scheduled_job(
        "cron", hour=args.hour, minute=0, day_of_week="mon-fri",
        misfire_grace_time=7200,  # 允许延迟 2 小时仍执行
        coalesce=True,            # 多次 misfire 合并为一次执行
    )
    def daily_job() -> None:
        trade_date = date.today().isoformat()
        logger.info(f"=== 每日任务开始 {trade_date} ===")

        try:
            bar_count = _ingest_with_retry(trade_date)
            if bar_count == 0:
                logger.warning(f"{trade_date} 非交易日或数据未就绪，跳过筛选")
                return

            logger.info(f"数据就绪（{bar_count} 行），开始筛选...")
            summary = run_daily_pipeline(trade_date)
            logger.info(f"=== 每日任务完成: {summary} ===")
        except Exception as e:
            logger.exception(f"=== 每日任务异常 {trade_date} ===")
            from rquant.notify import notify
            notify("error", component="daily_job", exc=e)

    def handle_signal(signum: int, frame: object) -> None:
        logger.info("收到退出信号，正在关闭调度器...")
        scheduler.shutdown(wait=False)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    logger.info(f"调度器启动，每日 {args.hour}:00 (Mon-Fri) 执行")
    scheduler.start()
    return 0


def cmd_run_daily(args: argparse.Namespace) -> int:
    """一次性执行 ingest + 全流水线。"""
    from rquant.pipeline import run_daily_pipeline

    setup_logging()
    trade_date = args.date or date.today().isoformat()
    preset_names = [args.preset] if args.preset else None

    if not args.no_ingest:
        logger.info(f"拉取数据: {trade_date}")
        bar_count = _ingest_with_retry(trade_date)
        if bar_count == 0:
            logger.warning("无数据（非交易日或数据未就绪）")
            return 1

    logger.info(f"执行流水线: {trade_date}")
    summary = run_daily_pipeline(
        trade_date,
        preset_names=preset_names,
        minute_backfill=not args.skip_minute_backfill,
        minute_backfill_lookback_days=args.minute_lookback_days,
    )

    if not summary:
        logger.warning("无结果")
        return 1

    for name, count in summary.items():
        logger.info(f"  {name}: {count} 命中")
    return 0


def cmd_ingest(args: argparse.Namespace) -> int:
    """仅拉取数据，不跑筛选。"""
    setup_logging()
    trade_date = args.date or date.today().isoformat()

    logger.info(f"拉取数据: {trade_date}")
    bar_count = _ingest_with_retry(trade_date)

    if bar_count == 0:
        logger.warning("无数据（非交易日或数据未就绪）")
        return 1

    logger.info(f"完成: {bar_count} 行 daily_bar")
    return 0


def cmd_monitor(args: argparse.Namespace) -> int:
    """启动盘中实时监控。"""
    from rquant.monitor import run_monitor

    setup_logging()
    return run_monitor(interval=args.interval)


def _split_ts_codes(values: list[str]) -> list[str]:
    codes: list[str] = []
    for value in values:
        codes.extend(code.strip() for code in value.split(",") if code.strip())
    return codes


def cmd_rt_minute_fetch(args: argparse.Namespace) -> int:
    """拉取 Tushare 实时分钟最新 K 线并写入 minute_bar。"""
    from rquant.adapter.tushare import TushareAdapter

    setup_logging()
    ts_codes = _split_ts_codes(args.ts_code)
    if not ts_codes:
        logger.warning("未提供 ts_code，跳过")
        return 0

    df = TushareAdapter().rt_min(ts_codes, freq=args.freq)
    if df.empty:
        logger.warning("rt_min 返回空，未写入 minute_bar")
        return 0

    with DuckDBStore() as store:
        rows = store.upsert_minute_bars(df)
    latest_time = df["trade_time"].max()
    logger.info(
        f"rt_min 写入 minute_bar: rows={rows}, codes={len(ts_codes)}, latest={latest_time}"
    )
    return 0


def cmd_rt_minute_daily_fetch(args: argparse.Namespace) -> int:
    """拉取 Tushare 当日累计实时分钟 K 线并写入 minute_bar。"""
    from rquant.adapter.tushare import TushareAdapter

    setup_logging()
    ts_codes = _split_ts_codes(args.ts_code)
    if not ts_codes:
        logger.warning("未提供 ts_code，跳过")
        return 0

    df = TushareAdapter().rt_min_daily(ts_codes, freq=args.freq)
    if df.empty:
        logger.warning("rt_min_daily 返回空，未写入 minute_bar")
        return 0

    with DuckDBStore() as store:
        rows = store.upsert_minute_bars(df)
    latest_time = df["trade_time"].max()
    logger.info(
        "rt_min_daily 写入 minute_bar: "
        f"rows={rows}, codes={len(ts_codes)}, latest={latest_time}"
    )
    return 0


def cmd_research_sync(args: argparse.Namespace) -> int:
    """云端备份合并进本地研究库 / 从旧库恢复研究表。"""
    from rquant.research_sync import restore_research_tables, sync_from_backup

    setup_logging()
    refresh = not args.no_refresh_replica

    if args.restore_from:
        tables = args.tables.split(",") if args.tables else None
        report = restore_research_tables(
            Path(args.restore_from), tables=tables, refresh_replica=refresh
        )
    else:
        backup = Path(args.backup) if args.backup else None
        report = sync_from_backup(backup, refresh_replica=refresh)

    for t in report.tables:
        mark = {"replace": "替换", "merge": "合并", "skipped": "跳过", "error": "失败"}[t.mode]
        logger.info(f"  {t.table}: {mark} {t.rows:,} 行 {t.detail}")
    logger.info(
        f"replica: {'已刷新' if report.replica_refreshed else report.replica_detail}"
    )
    return 1 if report.has_errors else 0


def cmd_moneyflow_backfill(args: argparse.Namespace) -> int:
    """拉取 Tushare 日级资金流并写入 moneyflow_daily。"""
    from rquant.adapter.tushare import TushareAdapter

    setup_logging()
    trade_date = date.fromisoformat(args.date)
    df = TushareAdapter().moneyflow(trade_date)
    if df.empty:
        logger.warning("moneyflow 返回空，未写入 moneyflow_daily")
        return 0

    with DuckDBStore() as store:
        rows = store.upsert_moneyflow_daily(df)
    logger.info(f"moneyflow 写入 moneyflow_daily: rows={rows}, date={args.date}")
    return 0


def cmd_minute_backfill(args: argparse.Namespace) -> int:
    """回补 Pool 命中标的历史分钟线。"""
    from rquant.adapter.tushare import TushareAdapter
    from rquant.intraday_backfill import backfill_pool1_minute_context
    from rquant.storage.duckdb import DuckDBStore

    setup_logging()
    with DuckDBStore() as store:
        summary = backfill_pool1_minute_context(
            store,
            TushareAdapter(),
            screen_date=args.date,
            lookback_days=args.lookback_days,
            freq=args.freq,
            preset_name=args.preset,
            ts_code=args.ts_code,
            dry_run=args.dry_run,
        )
    logger.info(summary.model_dump())
    return 0


def cmd_minute_replay_backfill(args: argparse.Namespace) -> int:
    """回补分钟 replay 所需的 B 日到退出窗口分钟线。"""
    from rquant.adapter.tushare import TushareAdapter
    from rquant.intraday_backfill import backfill_minute_replay_window
    from rquant.storage.duckdb import DuckDBStore

    setup_logging()
    with DuckDBStore() as store:
        summary = backfill_minute_replay_window(
            store,
            TushareAdapter(),
            start_date=args.start_date,
            end_date=args.end_date,
            max_hold_days=args.max_hold_days,
            freq=args.freq,
            preset_name=args.preset,
            ts_code=args.ts_code,
            dry_run=args.dry_run,
        )
    logger.info(summary.model_dump())
    return 0


def cmd_auction_backfill(args: argparse.Namespace) -> int:
    """回补 Tushare 集合竞价数据。"""
    from rquant.adapter.tushare import TushareAdapter
    from rquant.auction_backfill import backfill_stk_auction
    from rquant.storage.duckdb import DuckDBStore

    setup_logging()
    with DuckDBStore() as store:
        summary = backfill_stk_auction(
            store,
            TushareAdapter(),
            start_date=args.start_date,
            end_date=args.end_date,
            dry_run=args.dry_run,
        )
    logger.info(summary.model_dump())
    return 1 if summary.failed_requests else 0


def cmd_auction_minute_fallback(args: argparse.Namespace) -> int:
    """用 09:30 分钟线补齐集合竞价缺行。"""
    from rquant.auction_backfill import synthesize_open_auction_from_minute

    setup_logging()
    with DuckDBStore() as store:
        summary = synthesize_open_auction_from_minute(
            store,
            args.date,
            dry_run=args.dry_run,
        )
    logger.info(summary.model_dump())
    return 0


def cmd_auction_gap_replay(args: argparse.Namespace) -> int:
    """回测集合竞价跳空策略。"""
    from rquant.auction_gap_strategy import (
        AuctionGapConfig,
        run_auction_gap_replay,
        summarize_auction_gap_replay,
    )

    setup_logging()
    config = AuctionGapConfig(
        start_date=args.start_date,
        end_date=args.end_date,
        gap_mode=args.gap_mode,
        min_auction_vol_ratio_5d=args.min_ratio,
        max_auction_vol_ratio_5d=args.max_ratio,
        st_filter=args.st_filter,
    )
    with open_readonly_store(
        required_tables=["auction_bar", "daily_bar", "daily_state"]
    ) as store:
        trades = run_auction_gap_replay(store, config)

    summary = summarize_auction_gap_replay(trades)
    logger.info(summary.model_dump())

    if args.output:
        output = Path(args.output).expanduser().resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        trades.to_csv(output, index=False)
        logger.info(f"auction gap replay 结果已写出: {output}")

    if trades.empty:
        logger.warning("auction gap replay 无候选")
        return 0

    preview_cols = [
        "signal_date", "ts_code", "name", "entry_price",
        "auction_vol_ratio_5d", "gap_pct_close", "gap_pct_high",
        "hit_limit_up_today", "intraday_high_ret_pct",
        "next_trade_date", "next_open_ret_pct", "next_close_ret_pct",
    ]
    available_cols = [col for col in preview_cols if col in trades.columns]
    logger.info("\n" + trades[available_cols].tail(20).to_string(index=False))
    return 0


def cmd_auction_gap_minute_replay(args: argparse.Namespace) -> int:
    """回测集合竞价候选 + 分钟 B/S 策略。"""
    from rquant.auction_gap_strategy import (
        AuctionGapMinuteReplayConfig,
        run_auction_gap_minute_replay,
        run_auction_gap_replay,
        summarize_auction_gap_minute_replay,
    )

    setup_logging()
    config = AuctionGapMinuteReplayConfig(
        start_date=args.start_date,
        end_date=args.end_date,
        gap_mode=args.gap_mode,
        min_auction_vol_ratio_5d=args.min_ratio,
        max_auction_vol_ratio_5d=args.max_ratio,
        st_filter=args.st_filter,
        max_hold_days=args.max_hold_days,
    )
    required_tables = ["auction_bar", "daily_bar", "daily_state", "minute_bar"]
    with open_readonly_store(required_tables=required_tables) as store:
        candidates = run_auction_gap_replay(store, config.auction_config())
        trades = run_auction_gap_minute_replay(store, config)

    summary = summarize_auction_gap_minute_replay(
        trades,
        candidates_count=len(candidates),
    )
    logger.info(summary.model_dump())

    if args.output:
        output = Path(args.output).expanduser().resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        trades.to_csv(output, index=False)
        logger.info(f"auction gap minute replay 结果已写出: {output}")

    if trades.empty:
        logger.warning("auction gap minute replay 无成交")
        return 0

    preview_cols = [
        "signal_date", "ts_code", "name", "auction_price",
        "entry_time", "entry_price", "b_first_limit_up_time",
        "b_close_at_limit_up", "exit_time", "exit_price",
        "exit_reason", "ret_pct",
    ]
    available_cols = [col for col in preview_cols if col in trades.columns]
    logger.info("\n" + trades[available_cols].tail(20).to_string(index=False))
    return 0


def cmd_auction_gap_minute_backfill(args: argparse.Namespace) -> int:
    """按集合竞价跳空候选回补分钟 replay 窗口。"""
    from rquant.adapter.tushare import TushareAdapter
    from rquant.intraday_backfill import backfill_auction_gap_minute_replay_window

    setup_logging()
    with DuckDBStore() as store:
        summary = backfill_auction_gap_minute_replay_window(
            store,
            TushareAdapter(),
            start_date=args.start_date,
            end_date=args.end_date,
            max_hold_days=args.max_hold_days,
            freq=args.freq,
            gap_mode=args.gap_mode,
            st_filter=args.st_filter,
            min_ratio=args.min_ratio,
            max_ratio=args.max_ratio,
            ts_code=args.ts_code,
            dry_run=args.dry_run,
        )
    logger.info(summary.model_dump())
    return 1 if summary.failed_requests else 0


def cmd_minute_replay(args: argparse.Namespace) -> int:
    """基于已入库历史分钟线跑强承接/突破模拟回放。"""
    from rquant.minute_replay import run_minute_strong_carry_replay
    from rquant.storage.duckdb import DuckDBStore
    from rquant.volume_profile import VolumeProfileRuleConfig

    setup_logging()
    volume_profile_config = VolumeProfileRuleConfig(
        enabled=args.volume_profile,
        lookback_days=tuple(args.volume_profile_lookbacks),
    )
    with DuckDBStore() as store:
        trades = run_minute_strong_carry_replay(
            store,
            start_date=args.start_date,
            end_date=args.end_date,
            preset_name=args.preset,
            freq=args.freq,
            entry_mode=args.entry_mode,
            max_hold_days=args.max_hold_days,
            volume_profile_config=volume_profile_config,
        )

    if args.output:
        output = Path(args.output).expanduser().resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        trades.to_csv(output, index=False)
        logger.info(f"minute replay 结果已写出: {output}")

    if trades.empty:
        logger.warning("minute replay 无成交记录")
        return 0

    win_rate = (trades["ret_pct"] > 0).mean() * 100
    logger.info(
        "minute replay: "
        f"n={len(trades)}, "
        f"mean={trades['ret_pct'].mean():.2f}%, "
        f"median={trades['ret_pct'].median():.2f}%, "
        f"win={win_rate:.1f}%"
    )
    preview_cols = [
        "signal_date", "ts_code", "name", "entry_time", "entry_price_raw",
        "entry_price", "exit_time", "exit_price", "exit_reason",
        "holding_trading_days", "ret_pct",
    ]
    available_cols = [col for col in preview_cols if col in trades.columns]
    logger.info("\n" + trades[available_cols].tail(20).to_string(index=False))
    return 0


def cmd_growth_board_surge_replay(args: argparse.Namespace) -> int:
    """回测科创/创业板盘中放量追击策略。"""
    from rquant.growth_board_surge_strategy import (
        GrowthBoardSurgeConfig,
        run_growth_board_surge_replay,
    )

    setup_logging()
    config = GrowthBoardSurgeConfig(
        freq=args.freq,
        min_signal_time=_parse_hhmm(args.min_signal_time),
        lookback_days=args.lookback_days,
        min_hist_days=args.min_hist_days,
        min_cum_amount_ratio=args.min_cum_amount_ratio,
        min_same_minute_amount_ratio=args.min_same_minute_amount_ratio,
        max_hold_days=args.max_hold_days,
    )
    required_tables = [
        "daily_bar",
        "daily_indicator",
        "daily_state",
        "stock_basic",
        "minute_bar",
    ]
    with open_readonly_store(required_tables=required_tables) as store:
        trades = run_growth_board_surge_replay(
            store,
            start_date=args.start_date,
            end_date=args.end_date,
            config=config,
        )

    if args.output:
        output = Path(args.output).expanduser().resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        trades.to_csv(output, index=False)
        logger.info(f"growth board surge replay 结果已写出: {output}")

    if trades.empty:
        logger.warning("growth board surge replay 无成交记录")
        return 0

    win_rate = (trades["ret_pct"] > 0).mean() * 100
    logger.info(
        "growth board surge replay: "
        f"n={len(trades)}, "
        f"mean={trades['ret_pct'].mean():.2f}%, "
        f"median={trades['ret_pct'].median():.2f}%, "
        f"win={win_rate:.1f}%"
    )
    preview_cols = [
        "signal_date", "ts_code", "name", "board_type",
        "entry_time", "entry_price", "limit_up_price",
        "hit_limit_up_today", "exit_time", "exit_price",
        "exit_reason", "ret_pct",
    ]
    available_cols = [col for col in preview_cols if col in trades.columns]
    logger.info("\n" + trades[available_cols].tail(20).to_string(index=False))
    return 0


def cmd_notify_test(args: argparse.Namespace) -> int:
    """推测试消息验证所有配置的通道（PushDeer + PushPlus）。"""
    from datetime import datetime

    from rquant.config import settings
    from rquant.notify.client import PushDeerClient, PushPlusClient

    setup_logging()
    keys = settings.pushdeer_key_list
    tokens = settings.pushplus_token_list
    if not keys and not tokens:
        logger.error("PUSHDEER_KEYS 和 PUSHPLUS_TOKENS 都未配置，请检查 .env")
        return 1

    title = "✅ rQuant 通道测试"
    body = (
        f"时间：{datetime.now():%Y-%m-%d %H:%M:%S}\n"
        f"PushDeer keys: {len(keys)} 个 / PushPlus tokens: {len(tokens)} 个\n"
        f"这是测试消息，忽略即可"
    )

    total_success = 0
    total_count = 0

    if keys:
        pd_results = PushDeerClient(keys, settings.pushdeer_endpoint).push(title, body)
        for i, (s, err) in enumerate(pd_results):
            label = "PushDeer " + keys[i][:8] + "…"
            (logger.info if s else logger.error)(
                f"  {'✅' if s else '❌'} {label}{'' if s else ': ' + str(err)}"
            )
        total_success += sum(1 for s, _ in pd_results if s)
        total_count += len(pd_results)

    if tokens:
        pp_results = PushPlusClient(tokens, settings.pushplus_endpoint).push(title, body)
        for i, (s, err) in enumerate(pp_results):
            label = "PushPlus " + tokens[i][:8] + "…"
            (logger.info if s else logger.error)(
                f"  {'✅' if s else '❌'} {label}{'' if s else ': ' + str(err)}"
            )
        total_success += sum(1 for s, _ in pp_results if s)
        total_count += len(pp_results)

    logger.info(f"测试发送完成: {total_success}/{total_count} 成功")
    return 0 if total_success > 0 else 1


def cmd_daily_report(args: argparse.Namespace) -> int:
    """生成 + 推送当日健康摘要（systemd timer 15:30 自动跑）。"""
    from rquant.health import generate_and_send_daily_report

    setup_logging()
    n = generate_and_send_daily_report(dry_run=args.dry_run)
    return 0 if (args.dry_run or n > 0) else 1


def cmd_alert(args: argparse.Namespace) -> int:
    """发一条运维告警（用于 systemd OnFailure / watchdog 等场景）。

    刻意做成最小接口：subject 必填，body 可选。直接走 PushDeer + PushPlus，
    不依赖 notify scene 体系，避免被新增字段牵连。
    """
    from rquant.config import settings
    from rquant.notify.client import PushDeerClient, PushPlusClient

    setup_logging()
    keys = settings.pushdeer_key_list
    tokens = settings.pushplus_token_list
    if not keys and not tokens:
        logger.error("PUSHDEER_KEYS 和 PUSHPLUS_TOKENS 都未配置，alert 无处可发")
        return 1

    body = args.body or ""
    success = 0
    total = 0
    if keys:
        for s, err in PushDeerClient(keys, settings.pushdeer_endpoint).push(args.subject, body):
            total += 1
            success += int(s)
            if not s:
                logger.error(f"PushDeer 失败: {err}")
    if tokens:
        for s, err in PushPlusClient(tokens, settings.pushplus_endpoint).push(args.subject, body):
            total += 1
            success += int(s)
            if not s:
                logger.error(f"PushPlus 失败: {err}")

    logger.info(f"alert 发送: {success}/{total} 成功 (subject={args.subject!r})")
    return 0 if success > 0 else 1


def cmd_preflight(args: argparse.Namespace) -> int:
    """全家服务深度体检（手动触发，dry-run，不重启服务）。

    跑 5 项深度检查（unit verify / 服务详情 / DuckDB 锁布局 / 数据新鲜度 /
    screen smoke），打印 markdown 报告到 stdout。--notify 推 PushDeer 摘要。

    任何 fail → 退出码 1。
    """
    from rquant.config import settings
    from rquant.notify.client import PushDeerClient, PushPlusClient
    from rquant.preflight import format_pushdeer_summary, format_report, run_all_checks

    setup_logging()
    results = run_all_checks()
    print(format_report(results))

    if args.notify:
        subject, body = format_pushdeer_summary(results)
        keys = settings.pushdeer_key_list
        tokens = settings.pushplus_token_list
        if not keys and not tokens:
            logger.warning("PUSHDEER_KEYS / PUSHPLUS_TOKENS 都未配置，跳过推送")
        else:
            if keys:
                for s, err in PushDeerClient(keys, settings.pushdeer_endpoint).push(subject, body):
                    if not s:
                        logger.error(f"PushDeer 失败: {err}")
            if tokens:
                client = PushPlusClient(tokens, settings.pushplus_endpoint)
                for s, err in client.push(subject, body):
                    if not s:
                        logger.error(f"PushPlus 失败: {err}")

    fails = [r for r in results if r.status == "fail"]
    return 1 if fails else 0


def cmd_pre_market_check(args: argparse.Namespace) -> int:
    """开盘前主动体检（systemd timer Mon..Fri 09:00 触发）。

    跑 pre_market_check.run_all_checks() → format_summary → PushDeer 推一条摘要。
    任何 fail 项视为命令失败，触发 systemd OnFailure 兜底告警；warn 不触发 OnFailure
    （已经在 PushDeer 里说明）。
    """
    from rquant.config import settings
    from rquant.notify.client import PushDeerClient, PushPlusClient
    from rquant.pre_market_check import format_summary, run_all_checks

    setup_logging()
    results = run_all_checks()
    subject, body = format_summary(results)

    print(subject)
    print(body)

    if not args.dry_run:
        keys = settings.pushdeer_key_list
        tokens = settings.pushplus_token_list
        if not keys and not tokens:
            logger.warning("PUSHDEER_KEYS / PUSHPLUS_TOKENS 都未配置，跳过推送")
        else:
            if keys:
                for s, err in PushDeerClient(keys, settings.pushdeer_endpoint).push(subject, body):
                    if not s:
                        logger.error(f"PushDeer 失败: {err}")
            if tokens:
                client = PushPlusClient(tokens, settings.pushplus_endpoint)
                for s, err in client.push(subject, body):
                    if not s:
                        logger.error(f"PushPlus 失败: {err}")

    fails = [r for r in results if r.status == "fail"]
    return 1 if fails else 0


def cmd_pool2(args: argparse.Namespace) -> int:
    """管理 Pool 2 持久池。"""
    from rquant.storage.duckdb import DuckDBStore

    setup_logging()
    with DuckDBStore() as store:
        if args.pool2_action == "list":
            df = store.query_pool2_all()
            if df.empty:
                logger.info("Pool 2 持久池为空")
                return 0
            for _, row in df.iterrows():
                status_mark = "🟢" if row["status"] == "active" else "⬜"
                logger.info(
                    f"  {status_mark} {row['ts_code']} "
                    f"入池 {row['entry_date']} "
                    f"涨停 {row['limit_up_date']} "
                    f"body ¥{row['body_lower']:.2f}-¥{row['body_upper']:.2f} "
                    f"[{row['status']}]"
                )
            return 0

        elif args.pool2_action == "remove":
            store.remove_pool2(args.ts_code)
            logger.info(f"已从 Pool 2 移除: {args.ts_code}")
            return 0

    return 0


def cmd_blacklist(args: argparse.Namespace) -> int:
    """管理风险黑名单。"""
    from datetime import date
    from pathlib import Path

    from rquant.risk.blacklist import (
        export_blacklist_parquet,
        import_blacklist,
        load_active_blacklist,
        load_blacklist_parquet,
        parse_blacklist_pdf,
    )
    from rquant.storage.duckdb import DuckDBStore

    setup_logging()

    if args.blacklist_action == "import":
        pdf = Path(args.pdf).expanduser()
        if not pdf.exists():
            logger.error(f"PDF 不存在: {pdf}")
            return 1
        entries = parse_blacklist_pdf(pdf)
        with DuckDBStore() as store:
            n = import_blacklist(
                entries,
                list_label=args.label,
                source_file=pdf.name,
                store=store,
                validity_days=args.validity,
            )
        logger.info(f"导入完成：{n} 只 → '{args.label}'")
        return 0

    elif args.blacklist_action == "load-parquet":
        parquet = Path(args.parquet).expanduser().resolve()
        if not parquet.exists():
            logger.error(f"parquet 不存在: {parquet}")
            return 1
        with DuckDBStore() as store:
            n = load_blacklist_parquet(
                parquet, store, list_label=args.label,
            )
        logger.info(
            f"parquet 落库完成：{n} 行 → "
            f"{'list_label=' + args.label if args.label else '全表覆盖'}"
        )
        return 0

    elif args.blacklist_action == "export-parquet":
        out = Path(args.output).expanduser().resolve()
        with DuckDBStore() as store:
            n = export_blacklist_parquet(
                store, out, list_label=args.label,
            )
        logger.info(f"parquet 导出完成：{n} 行 → {out}")
        return 0

    elif args.blacklist_action == "list":
        with DuckDBStore() as store:
            today = date.today()
            blacklist = load_active_blacklist(
                store,
                list_label=args.label,
                include_expired=args.include_expired,
            )
            if not blacklist:
                logger.info("无活动黑名单")
                return 0
            for code, hit in sorted(blacklist.items()):
                expired_mark = " [已过期]" if hit.is_expired else ""
                days_left = (hit.expires_at - today).days
                logger.info(
                    f"  {code} {hit.name:8s} {hit.list_label} "
                    f"(剩 {days_left}d, {len(hit.sub_categories)} 类){expired_mark}"
                )
            logger.info(f"共 {len(blacklist)} 只")
        return 0

    elif args.blacklist_action == "check":
        with DuckDBStore() as store:
            blacklist = load_active_blacklist(store, include_expired=True)
        hit = blacklist.get(args.ts_code)
        if hit is None:
            logger.info(f"{args.ts_code} 不在任何黑名单中")
            return 0
        expired_mark = " [已过期]" if hit.is_expired else ""
        logger.info(
            f"{args.ts_code} {hit.name} 在 '{hit.list_label}'{expired_mark}\n"
            f"  类别: {hit.sub_categories}\n"
            f"  导入: {hit.imported_at} → 失效: {hit.expires_at}"
        )
        return 0

    elif args.blacklist_action == "remove":
        with DuckDBStore() as store:
            n = store._conn.execute(
                "DELETE FROM risk_blacklist WHERE list_label = ?", [args.label]
            ).fetchone()
        logger.info(f"已删除 list_label='{args.label}' 的所有条目")
        return 0

    return 0


def build_parser() -> argparse.ArgumentParser:
    """构建 CLI 参数解析器。"""
    parser = argparse.ArgumentParser(
        prog="rquant", description="rQuant 量化选股平台"
    )
    sub = parser.add_subparsers(dest="command")

    serve_p = sub.add_parser("serve", help="启动 APScheduler 常驻进程")
    serve_p.add_argument(
        "--hour", type=int, default=17, help="每日触发小时 (默认 17)"
    )

    run_p = sub.add_parser("run-daily", help="拉取数据 + 执行全流水线")
    run_p.add_argument(
        "--date", type=str, default=None,
        help="交易日期 YYYY-MM-DD (默认今天)",
    )
    run_p.add_argument(
        "--preset", type=str, default=None,
        help="只跑指定预设 (默认全部)",
    )
    run_p.add_argument(
        "--no-ingest", action="store_true",
        help="跳过数据拉取，只跑筛选",
    )
    run_p.add_argument(
        "--skip-minute-backfill", action="store_true",
        help="跳过日终 Pool1 90 日分钟上下文回补",
    )
    run_p.add_argument(
        "--minute-lookback-days", type=int, default=90,
        help="日终分钟上下文回补交易日数量 (默认 90)",
    )

    ingest_p = sub.add_parser("ingest", help="仅拉取数据（不跑筛选）")
    ingest_p.add_argument(
        "--date", type=str, default=None,
        help="交易日期 YYYY-MM-DD (默认今天)",
    )

    monitor_p = sub.add_parser("monitor", help="启动盘中实时监控")
    monitor_p.add_argument(
        "--interval", type=int, default=5,
        help="轮询间隔秒数 (默认 5)",
    )

    rt_min_p = sub.add_parser(
        "rt-minute-fetch",
        help="拉取 Tushare 实时分钟最新 K 线并写入 minute_bar",
    )
    rt_min_p.add_argument(
        "--ts-code",
        action="append",
        required=True,
        help="股票代码，支持逗号分隔或重复传参",
    )
    rt_min_p.add_argument(
        "--freq",
        type=str,
        default="1min",
        choices=["1min", "5min", "15min", "30min", "60min"],
        help="分钟频度 (默认 1min)",
    )

    rt_min_daily_p = sub.add_parser(
        "rt-minute-daily-fetch",
        help="拉取 Tushare 当日累计实时分钟 K 线并写入 minute_bar",
    )
    rt_min_daily_p.add_argument(
        "--ts-code",
        action="append",
        required=True,
        help="股票代码，支持逗号分隔或重复传参",
    )
    rt_min_daily_p.add_argument(
        "--freq",
        type=str,
        default="1min",
        choices=["1min", "5min", "15min", "30min", "60min"],
        help="分钟频度 (默认 1min)",
    )

    rs_p = sub.add_parser(
        "research-sync",
        help="云端备份(cloud_backup.duckdb)合并进本地研究库，或从旧库恢复研究表",
    )
    rs_p.add_argument(
        "--backup", type=str, default=None,
        help="云端备份文件路径（默认 data/cloud_backup.duckdb）",
    )
    rs_p.add_argument(
        "--restore-from", type=str, default=None,
        help="恢复模式：从指定旧库/旧副本按主键合并研究表",
    )
    rs_p.add_argument(
        "--tables", type=str, default=None,
        help="恢复模式下只处理这些表（逗号分隔，默认全部研究表）",
    )
    rs_p.add_argument(
        "--no-refresh-replica", action="store_true",
        help="跳过只读副本刷新",
    )

    moneyflow_p = sub.add_parser(
        "moneyflow-backfill",
        help="拉取 Tushare 日级个股资金流并写入 moneyflow_daily",
    )
    moneyflow_p.add_argument(
        "--date",
        type=str,
        required=True,
        help="交易日期 YYYY-MM-DD",
    )

    minute_p = sub.add_parser(
        "minute-backfill", help="回补 Pool 命中标的历史分钟线"
    )
    minute_p.add_argument(
        "--date", type=str, required=True,
        help="Pool 筛选日期 YYYY-MM-DD",
    )
    minute_p.add_argument(
        "--lookback-days", type=int, default=90,
        help="向前回补交易日数量 (默认 90)",
    )
    minute_p.add_argument(
        "--freq", type=str, default="1min",
        choices=["1min", "5min", "15min", "30min", "60min"],
        help="分钟频度 (默认 1min)",
    )
    minute_p.add_argument(
        "--preset", type=str, default="n-shape-pool1",
        help="筛选 preset (默认 n-shape-pool1)",
    )
    minute_p.add_argument(
        "--ts-code", type=str, default=None,
        help="只回补单只股票，调试用",
    )
    minute_p.add_argument(
        "--dry-run", action="store_true",
        help="只估算请求数，不调用 Tushare、不写库",
    )

    replay_p = sub.add_parser(
        "minute-replay", help="基于历史分钟线跑强承接/突破模拟回放"
    )
    replay_p.add_argument(
        "--start-date", type=str, required=True,
        help="Pool 筛选开始日期 YYYY-MM-DD",
    )
    replay_p.add_argument(
        "--end-date", type=str, required=True,
        help="Pool 筛选结束日期 YYYY-MM-DD",
    )
    replay_p.add_argument(
        "--preset", type=str, default="n-shape-pool1",
        help="筛选 preset (默认 n-shape-pool1)",
    )
    replay_p.add_argument(
        "--freq", type=str, default="1min",
        choices=["1min", "5min", "15min", "30min", "60min"],
        help="分钟频度 (默认 1min)",
    )
    replay_p.add_argument(
        "--entry-mode", type=str, default="first_break",
        choices=[
            "first_break",
            "break_retest",
            "late_confirm",
            "vwap_confirm",
            "amount_surge",
        ],
        help="入场模式 (默认 first_break)",
    )
    replay_p.add_argument(
        "--max-hold-days", type=int, default=5,
        help="最多持有交易日数量 (默认 5)",
    )
    replay_p.add_argument(
        "--volume-profile", action="store_true",
        help="启用 90 日价量分布入场过滤与动态风控",
    )
    replay_p.add_argument(
        "--volume-profile-lookbacks", type=int, nargs="+", default=[90],
        help="价量分布 lookback 交易日列表 (默认 90)",
    )
    replay_p.add_argument(
        "--output", type=str, default=None,
        help="CSV 输出路径（可选）",
    )

    growth_replay_p = sub.add_parser(
        "growth-board-surge-replay",
        help="回测科创/创业板盘中放量追击策略",
    )
    growth_replay_p.add_argument(
        "--start-date", type=str, required=True,
        help="回测开始日期 YYYY-MM-DD",
    )
    growth_replay_p.add_argument(
        "--end-date", type=str, required=True,
        help="回测结束日期 YYYY-MM-DD",
    )
    growth_replay_p.add_argument(
        "--freq", type=str, default="1min",
        choices=["1min", "5min", "15min", "30min", "60min"],
        help="分钟频度 (默认 1min)",
    )
    growth_replay_p.add_argument(
        "--min-signal-time", type=str, default="09:33",
        help="最早 B 信号时间 HH:MM (默认 09:33)",
    )
    growth_replay_p.add_argument(
        "--lookback-days", type=int, default=20,
        help="分钟历史基准 lookback 交易日数量 (默认 20)",
    )
    growth_replay_p.add_argument(
        "--min-hist-days", type=int, default=10,
        help="至少需要的历史分钟样本交易日数量 (默认 10)",
    )
    growth_replay_p.add_argument(
        "--min-cum-amount-ratio", type=float, default=1.4,
        help="截至当前累计成交额相对历史同时间中位数倍数 (默认 1.4)",
    )
    growth_replay_p.add_argument(
        "--min-same-minute-amount-ratio", type=float, default=2.0,
        help="当前分钟成交额相对历史同分钟中位数倍数 (默认 2.0)",
    )
    growth_replay_p.add_argument(
        "--max-hold-days", type=int, default=1,
        help="最多持有交易日数量，T+1 默认次日收盘前退出 (默认 1)",
    )
    growth_replay_p.add_argument(
        "--output", type=str, default=None,
        help="CSV 输出路径（可选）",
    )

    replay_backfill_p = sub.add_parser(
        "minute-replay-backfill",
        help="回补分钟 replay 所需的 B 日到退出窗口分钟线",
    )
    replay_backfill_p.add_argument(
        "--start-date", type=str, required=True,
        help="Pool 筛选开始日期 YYYY-MM-DD",
    )
    replay_backfill_p.add_argument(
        "--end-date", type=str, required=True,
        help="Pool 筛选结束日期 YYYY-MM-DD",
    )
    replay_backfill_p.add_argument(
        "--preset", type=str, default="n-shape-pool1",
        help="筛选 preset (默认 n-shape-pool1)",
    )
    replay_backfill_p.add_argument(
        "--freq", type=str, default="1min",
        choices=["1min", "5min", "15min", "30min", "60min"],
        help="分钟频度 (默认 1min)",
    )
    replay_backfill_p.add_argument(
        "--max-hold-days", type=int, default=5,
        help="最多持有交易日数量 (默认 5)",
    )
    replay_backfill_p.add_argument(
        "--ts-code", type=str, default=None,
        help="只回补单只股票，调试用",
    )
    replay_backfill_p.add_argument(
        "--dry-run", action="store_true",
        help="只估算请求数，不调用 Tushare、不写库",
    )

    auction_p = sub.add_parser(
        "auction-backfill",
        help="回补 Tushare 集合竞价数据",
    )
    auction_p.add_argument(
        "--start-date", type=str, required=True,
        help="开始日期 YYYY-MM-DD",
    )
    auction_p.add_argument(
        "--end-date", type=str, required=True,
        help="结束日期 YYYY-MM-DD",
    )
    auction_p.add_argument(
        "--dry-run", action="store_true",
        help="只估算交易日请求数，不调用 Tushare、不写库",
    )

    auction_fallback_p = sub.add_parser(
        "auction-minute-fallback",
        help="用 09:30 分钟线补齐 Tushare 集合竞价缺行",
    )
    auction_fallback_p.add_argument(
        "--date",
        type=str,
        required=True,
        help="交易日期 YYYY-MM-DD",
    )
    auction_fallback_p.add_argument(
        "--dry-run",
        action="store_true",
        help="只估算补齐行数，不写库",
    )

    auction_gap_p = sub.add_parser(
        "auction-gap-replay",
        help="回测集合竞价跳空高开策略",
    )
    auction_gap_p.add_argument(
        "--start-date", type=str, required=True,
        help="开始日期 YYYY-MM-DD",
    )
    auction_gap_p.add_argument(
        "--end-date", type=str, required=True,
        help="结束日期 YYYY-MM-DD",
    )
    auction_gap_p.add_argument(
        "--gap-mode", type=str, default="close",
        choices=["close", "strict_high"],
        help="跳空定义：close=竞价价高于昨收；strict_high=竞价价高于昨高",
    )
    auction_gap_p.add_argument(
        "--st-filter", type=str, default="case_insensitive",
        choices=["case_insensitive", "literal_lower", "none"],
        help="ST 过滤：默认大小写不敏感过滤 ST/*ST",
    )
    auction_gap_p.add_argument(
        "--min-ratio", type=float, default=0.15,
        help="竞价量/近5日均量下限 (默认 0.15)",
    )
    auction_gap_p.add_argument(
        "--max-ratio", type=float, default=5.0,
        help="竞价量/近5日均量上限 (默认 5)",
    )
    auction_gap_p.add_argument(
        "--output", type=str, default=None,
        help="CSV 输出路径（可选）",
    )

    auction_gap_minute_p = sub.add_parser(
        "auction-gap-minute-replay",
        help="回测集合竞价候选 + 分钟 B/S 策略",
    )
    auction_gap_minute_p.add_argument(
        "--start-date", type=str, required=True,
        help="开始日期 YYYY-MM-DD",
    )
    auction_gap_minute_p.add_argument(
        "--end-date", type=str, required=True,
        help="结束日期 YYYY-MM-DD",
    )
    auction_gap_minute_p.add_argument(
        "--gap-mode", type=str, default="close",
        choices=["close", "strict_high"],
        help="跳空定义：close=竞价价高于昨收；strict_high=竞价价高于昨高",
    )
    auction_gap_minute_p.add_argument(
        "--st-filter", type=str, default="case_insensitive",
        choices=["case_insensitive", "literal_lower", "none"],
        help="ST 过滤：默认大小写不敏感过滤 ST/*ST",
    )
    auction_gap_minute_p.add_argument(
        "--min-ratio", type=float, default=0.15,
        help="竞价量/近5日均量下限 (默认 0.15)",
    )
    auction_gap_minute_p.add_argument(
        "--max-ratio", type=float, default=5.0,
        help="竞价量/近5日均量上限 (默认 5)",
    )
    auction_gap_minute_p.add_argument(
        "--max-hold-days", type=int, default=1,
        help="最多持有交易日数量 (默认 1)",
    )
    auction_gap_minute_p.add_argument(
        "--output", type=str, default=None,
        help="CSV 输出路径（可选）",
    )

    auction_gap_minute_backfill_p = sub.add_parser(
        "auction-gap-minute-backfill",
        help="回补集合竞价跳空候选的分钟 replay 窗口",
    )
    auction_gap_minute_backfill_p.add_argument(
        "--start-date", type=str, required=True,
        help="开始日期 YYYY-MM-DD",
    )
    auction_gap_minute_backfill_p.add_argument(
        "--end-date", type=str, required=True,
        help="结束日期 YYYY-MM-DD",
    )
    auction_gap_minute_backfill_p.add_argument(
        "--gap-mode", type=str, default="close",
        choices=["close", "strict_high"],
        help="跳空定义：close=竞价价高于昨收；strict_high=竞价价高于昨高",
    )
    auction_gap_minute_backfill_p.add_argument(
        "--st-filter", type=str, default="case_insensitive",
        choices=["case_insensitive", "literal_lower", "none"],
        help="ST 过滤：默认大小写不敏感过滤 ST/*ST",
    )
    auction_gap_minute_backfill_p.add_argument(
        "--min-ratio", type=float, default=0.15,
        help="竞价量/近5日均量下限 (默认 0.15)",
    )
    auction_gap_minute_backfill_p.add_argument(
        "--max-ratio", type=float, default=5.0,
        help="竞价量/近5日均量上限 (默认 5)",
    )
    auction_gap_minute_backfill_p.add_argument(
        "--max-hold-days", type=int, default=1,
        help="最多持有交易日数量 (默认 1)",
    )
    auction_gap_minute_backfill_p.add_argument(
        "--freq", type=str, default="1min",
        choices=["1min", "5min", "15min", "30min", "60min"],
        help="分钟频度 (默认 1min)",
    )
    auction_gap_minute_backfill_p.add_argument(
        "--ts-code", type=str, default=None,
        help="只回补单只股票，调试用",
    )
    auction_gap_minute_backfill_p.add_argument(
        "--dry-run", action="store_true",
        help="只估算请求数，不调用 Tushare、不写库",
    )

    pool2_p = sub.add_parser("pool2", help="管理 Pool 2 持久池")
    pool2_sub = pool2_p.add_subparsers(dest="pool2_action")
    pool2_sub.add_parser("list", help="列出 Pool 2 标的")
    pool2_rm = pool2_sub.add_parser("remove", help="移除标的")
    pool2_rm.add_argument("ts_code", type=str, help="股票代码 (如 002415.SZ)")

    bl_p = sub.add_parser("blacklist", help="管理风险黑名单")
    bl_sub = bl_p.add_subparsers(dest="blacklist_action")

    bl_imp = bl_sub.add_parser("import", help="从 PDF 导入黑名单（mac 端用）")
    bl_imp.add_argument("pdf", type=str, help="PDF 路径")
    bl_imp.add_argument(
        "--label", type=str, default="430黑名单",
        help="名单标签 (默认 430黑名单)",
    )
    bl_imp.add_argument(
        "--validity", type=int, default=365,
        help="有效期天数 (默认 365)",
    )

    bl_load = bl_sub.add_parser(
        "load-parquet", help="从 parquet 加载到 DuckDB（云端推送后用）"
    )
    bl_load.add_argument("parquet", type=str, help="parquet 文件路径")
    bl_load.add_argument(
        "--label", type=str, default=None,
        help="只替换该 label 的行（默认全表覆盖）",
    )

    bl_exp = bl_sub.add_parser(
        "export-parquet", help="导出黑名单到 parquet（mac 端推云前用）"
    )
    bl_exp.add_argument(
        "--output", type=str, default="data/risk_blacklist.parquet",
        help="输出路径 (默认 data/risk_blacklist.parquet)",
    )
    bl_exp.add_argument(
        "--label", type=str, default=None,
        help="只导出该 label（默认全表导出）",
    )

    bl_ls = bl_sub.add_parser("list", help="列出黑名单")
    bl_ls.add_argument("--label", type=str, default=None, help="过滤 label")
    bl_ls.add_argument(
        "--include-expired", action="store_true", help="包含已过期条目"
    )

    bl_chk = bl_sub.add_parser("check", help="查询某只股票是否在黑名单")
    bl_chk.add_argument("ts_code", type=str, help="股票代码 (如 600340.SH)")

    bl_rm = bl_sub.add_parser("remove", help="删除整个 list_label")
    bl_rm.add_argument("--label", type=str, required=True, help="要删除的 label")

    sub.add_parser("notify-test", help="推一条 PushDeer 测试消息")
    dr_p = sub.add_parser("daily-report", help="生成并推送当日健康摘要（systemd timer 自动跑）")
    dr_p.add_argument(
        "--dry-run", action="store_true",
        help="只打印不推送（mac 本地 smoke 测试用）",
    )

    pmc_p = sub.add_parser(
        "pre-market-check", help="开盘前主动健康体检（systemd timer Mon..Fri 09:00 自动跑）",
    )
    pmc_p.add_argument(
        "--dry-run", action="store_true",
        help="只打印不推送（mac 本地 smoke 测试用）",
    )

    pf_p = sub.add_parser(
        "preflight", help="全家服务深度体检（手动触发，dry-run，不重启服务）",
    )
    pf_p.add_argument(
        "--notify", action="store_true",
        help="跑完推一条摘要到 PushDeer（默认只 stdout）",
    )

    alert_p = sub.add_parser("alert", help="发运维告警（systemd OnFailure / watchdog 用）")
    alert_p.add_argument("--subject", required=True, help="告警主题")
    alert_p.add_argument("--body", default="", help="告警正文（可选）")

    return parser


def main() -> int:
    """CLI 入口函数。一次性命令的异常顶层捕获后推 PushDeer。

    serve 内的 daily_job 自有 try/except + notify，main 不重复抓。
    """
    parser = build_parser()
    args = parser.parse_args()

    dispatch = {
        "serve": cmd_serve,
        "run-daily": cmd_run_daily,
        "ingest": cmd_ingest,
        "monitor": cmd_monitor,
        "rt-minute-fetch": cmd_rt_minute_fetch,
        "rt-minute-daily-fetch": cmd_rt_minute_daily_fetch,
        "research-sync": cmd_research_sync,
        "moneyflow-backfill": cmd_moneyflow_backfill,
        "minute-backfill": cmd_minute_backfill,
        "minute-replay-backfill": cmd_minute_replay_backfill,
        "auction-backfill": cmd_auction_backfill,
        "auction-minute-fallback": cmd_auction_minute_fallback,
        "auction-gap-replay": cmd_auction_gap_replay,
        "auction-gap-minute-replay": cmd_auction_gap_minute_replay,
        "auction-gap-minute-backfill": cmd_auction_gap_minute_backfill,
        "minute-replay": cmd_minute_replay,
        "growth-board-surge-replay": cmd_growth_board_surge_replay,
        "pool2": cmd_pool2,
        "blacklist": cmd_blacklist,
        "notify-test": cmd_notify_test,
        "alert": cmd_alert,
        "daily-report": cmd_daily_report,
        "pre-market-check": cmd_pre_market_check,
        "preflight": cmd_preflight,
    }
    handler = dispatch.get(args.command)
    if handler is None:
        parser.print_help()
        return 0

    # alert / daily-report 自身就是日常运维路径，main 不该再吞它的异常包一层
    if args.command in (
        "serve", "notify-test", "alert",
        "daily-report", "pre-market-check", "preflight",
    ):
        return handler(args)

    try:
        return handler(args)
    except Exception as e:
        logger.exception(f"=== {args.command} 异常 ===")
        from rquant.notify import notify
        notify("error", component=f"cli:{args.command}", exc=e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
