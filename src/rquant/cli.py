"""CLI 入口：rquant serve / rquant run-daily / rquant ingest。"""

from __future__ import annotations

import argparse
import signal
import sys
import time
from datetime import date

from loguru import logger

from rquant.logging import setup_logging

# 数据未就绪时的重试配置
_RETRY_COUNT = 3
_RETRY_INTERVAL = 900  # 15 分钟


def _ingest_with_retry(trade_date: str) -> int:
    """拉取数据，未就绪时最多重试 _RETRY_COUNT 次。"""
    from rquant.ingest import ingest_daily

    for attempt in range(1, _RETRY_COUNT + 1):
        bar_count = ingest_daily(trade_date)
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
    summary = run_daily_pipeline(trade_date, preset_names=preset_names)

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
                for s, err in PushPlusClient(tokens, settings.pushplus_endpoint).push(subject, body):
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
        "pool2": cmd_pool2,
        "blacklist": cmd_blacklist,
        "notify-test": cmd_notify_test,
        "alert": cmd_alert,
        "daily-report": cmd_daily_report,
        "pre-market-check": cmd_pre_market_check,
    }
    handler = dispatch.get(args.command)
    if handler is None:
        parser.print_help()
        return 0

    # alert / daily-report 自身就是日常运维路径，main 不该再吞它的异常包一层
    if args.command in ("serve", "notify-test", "alert", "daily-report", "pre-market-check"):
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
