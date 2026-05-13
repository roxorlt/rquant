"""每日健康摘要（cloud 端 systemd timer 15:30 自动跑 → PushDeer）。

数据源：
- `systemctl show <unit>`：service 最新一次 start/exit 时间戳 + 状态码
- `logs/watchdog-YYYY-MM-DD.log`：watchdog 每次调用的 tag（active/skip/alert）
- DuckDB：今日 monitor_event + screen_result 行数
- akshare：is_trading_day(today)

不依赖 journalctl（避免 lighthouse 用户读不了系统 journal 的权限问题）。
"""

from __future__ import annotations

import subprocess
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from loguru import logger

from rquant.config import settings
from rquant.storage.duckdb import DuckDBStore


# ---------- systemd 状态 ----------


@dataclass
class ServiceSnapshot:
    """单个 systemd unit 今天的状态摘要。"""
    unit: str
    active_state: str            # active | inactive | failed | activating | ...
    sub_state: str               # running | dead | exited | failed | ...
    start_today: datetime | None # 今天的 ExecMainStart（不是今天的就 None）
    exit_today: datetime | None  # 今天的 ExecMainExit（不是今天的就 None）
    exit_status: int | None      # 今天的 ExecMainStatus（数字）
    duration_sec: int | None     # exit - start 秒数（如有）


def _parse_systemd_ts(value: str) -> datetime | None:
    """systemd 时间戳格式 'Thu 2026-04-30 11:31:03 CST' → datetime。"""
    if not value or value in ("0", "n/a"):
        return None
    try:
        # date -d 跨平台不可靠，python 直接解析
        # systemd format: '%a %Y-%m-%d %H:%M:%S %Z'
        # CST 时区 abbr Python 不认，drop 掉
        parts = value.rsplit(" ", 1)
        ts = parts[0] if len(parts) == 2 else value
        return datetime.strptime(ts, "%a %Y-%m-%d %H:%M:%S")
    except (ValueError, IndexError):
        return None


def get_service_snapshot(unit: str, today: date | None = None) -> ServiceSnapshot:
    """systemctl show 拿单个 unit 的当日状态。"""
    today = today or date.today()
    try:
        result = subprocess.run(
            [
                "systemctl", "show", unit,
                "--property=ActiveState,SubState,ExecMainStartTimestamp,"
                "ExecMainExitTimestamp,ExecMainStatus",
            ],
            capture_output=True, text=True, timeout=10,
        )
        props = dict(
            line.split("=", 1) for line in result.stdout.strip().split("\n")
            if "=" in line
        )
    except Exception as e:
        logger.error(f"systemctl show {unit} 失败: {e}")
        return ServiceSnapshot(
            unit=unit, active_state="unknown", sub_state="",
            start_today=None, exit_today=None,
            exit_status=None, duration_sec=None,
        )

    start = _parse_systemd_ts(props.get("ExecMainStartTimestamp", ""))
    end = _parse_systemd_ts(props.get("ExecMainExitTimestamp", ""))
    status_str = props.get("ExecMainStatus", "").strip()
    status = int(status_str) if status_str.isdigit() else None

    # 只保留"今天的" timestamp
    start_today = start if start and start.date() == today else None
    exit_today = end if end and end.date() == today else None
    duration = int((end - start).total_seconds()) if start and end and end > start else None

    return ServiceSnapshot(
        unit=unit,
        active_state=props.get("ActiveState", "unknown"),
        sub_state=props.get("SubState", ""),
        start_today=start_today,
        exit_today=exit_today,
        exit_status=status,
        duration_sec=duration,
    )


# ---------- watchdog 日志解析 ----------


def read_watchdog_log(log_dir: Path, today: date | None = None) -> dict[str, int]:
    """读 logs/watchdog-YYYY-MM-DD.log，统计 tag 分布。

    格式：每行 "<ISO ts> <tag>"。tag ∈ {active, skip-clean-exit, alert-restart}。
    """
    today = today or date.today()
    log_path = log_dir / f"watchdog-{today.isoformat()}.log"
    counts: Counter[str] = Counter()
    if not log_path.exists():
        return dict(counts)

    try:
        for line in log_path.read_text().splitlines():
            parts = line.strip().split()
            if len(parts) >= 2:
                tag = parts[-1]
                counts[tag] += 1
    except Exception as e:
        logger.warning(f"读 watchdog log 失败 {log_path}: {e}")

    return dict(counts)


# ---------- DuckDB 业务数据 ----------


def count_today_business_data(store: DuckDBStore, today: date) -> dict[str, int]:
    """今日业务数据：price_level events + screen_results 各 preset 命中数。"""
    today_str = today.isoformat()
    out: dict[str, int] = {}

    # monitor_event 触发数
    row = store._conn.execute(
        "SELECT COUNT(*) FROM monitor_event WHERE trade_date = ?",
        [today_str],
    ).fetchone()
    out["price_level_events"] = int(row[0]) if row else 0

    # 各 preset 今日命中数
    rows = store._conn.execute(
        """
        SELECT preset_name, COUNT(*) FROM screen_result
        WHERE trade_date = ?
        GROUP BY preset_name
        """,
        [today_str],
    ).fetchall()
    for preset, n in rows:
        out[f"screen_{preset}"] = int(n)

    return out


# ---------- 报文构造 ----------


def _fmt_time(dt: datetime | None) -> str:
    return dt.strftime("%H:%M:%S") if dt else "—"


def _fmt_duration(sec: int | None) -> str:
    if sec is None:
        return "—"
    if sec < 60:
        return f"{sec}s"
    if sec < 3600:
        return f"{sec // 60}m{sec % 60}s"
    return f"{sec // 3600}h{(sec % 3600) // 60}m"


def build_daily_report(
    today: date,
    is_trading_day_flag: bool,
    monitor: ServiceSnapshot,
    daily: ServiceSnapshot,
    watchdog_counts: dict[str, int],
    business: dict[str, int],
) -> tuple[str, str]:
    """拼报文：(subject, body)。subject 短，body 多行细节。"""
    weekday_zh = "一二三四五六日"[today.weekday()]
    trading_label = "交易日" if is_trading_day_flag else "非交易日"
    subject = f"[RQ] 📊 日报 {today.isoformat()}（周{weekday_zh}·{trading_label}）"

    lines: list[str] = []

    # === Monitor ===
    if not is_trading_day_flag:
        # 期望：09:25 启动几秒内 exit 0
        if monitor.start_today and monitor.exit_today and monitor.exit_status == 0:
            if monitor.duration_sec is not None and monitor.duration_sec < 60:
                lines.append(
                    f"✅ monitor: {_fmt_time(monitor.start_today)} 启动 → "
                    f"{_fmt_duration(monitor.duration_sec)} 内自检退（非交易日符合预期）"
                )
            else:
                lines.append(
                    f"⚠️ monitor: 启动后跑了 {_fmt_duration(monitor.duration_sec)} "
                    f"（非交易日应秒退，需检查）"
                )
        elif not monitor.start_today:
            lines.append("❌ monitor: 今天未触发（timer 09:25 应触发，需排查）")
        else:
            lines.append(
                f"❌ monitor: status={monitor.exit_status} "
                f"({_fmt_time(monitor.start_today)} → {_fmt_time(monitor.exit_today)})"
            )
    else:
        # 交易日期望：09:25 启动 → 持续到 15:00 后 exit 0，时长 ~5h
        if monitor.start_today and monitor.exit_today and monitor.exit_status == 0:
            dur = monitor.duration_sec or 0
            # 交易日全程时长应 ≥ 5h（300 min）；< 5h 提示可能跨午休 bug 复发
            if dur >= 5 * 3600:
                lines.append(
                    f"✅ monitor: {_fmt_time(monitor.start_today)} → "
                    f"{_fmt_time(monitor.exit_today)} 跑足 {_fmt_duration(dur)}（含跨午休）"
                )
            else:
                lines.append(
                    f"⚠️ monitor: 时长仅 {_fmt_duration(dur)}，疑似跨午休 bug 复发"
                )
        elif monitor.start_today and not monitor.exit_today:
            lines.append(
                f"⏳ monitor: {_fmt_time(monitor.start_today)} 启动后仍 {monitor.active_state}（未退）"
            )
        elif not monitor.start_today:
            lines.append("❌ monitor: 今天未触发（timer 09:25 应触发，需排查）")
        else:
            lines.append(
                f"❌ monitor: 异常退出 status={monitor.exit_status} "
                f"({_fmt_time(monitor.exit_today)})"
            )

    # === Watchdog ===
    # in-window: timer 09..14 范围中真正落在 09:30-15:00 交易时段的触发
    # out-of-window: 09:00-09:28 / 11:30 不算（脚本自检后静默退，仅记录）
    active_n = watchdog_counts.get("active", 0)
    skip_n = watchdog_counts.get("skip-clean-exit", 0)
    alert_n = watchdog_counts.get("alert-restart", 0)
    oow_n = watchdog_counts.get("out-of-window", 0)
    in_window_total = active_n + skip_n + alert_n

    if in_window_total == 0:
        if is_trading_day_flag:
            lines.append("❌ watchdog: 交易时段 0 次触发（timer 应每 2min 触发）")
        else:
            lines.append(
                f"ℹ️ watchdog: 交易时段 0 次触发"
                + (f"（盘外 {oow_n} 次自检退）" if oow_n else "（非交易日不打扰）")
            )
    elif alert_n == 0:
        lines.append(
            f"✅ watchdog: 交易时段 {in_window_total} 次"
            f"（active={active_n} skip={skip_n}），无告警"
        )
    else:
        lines.append(
            f"⚠️ watchdog: 交易时段 {in_window_total} 次，**alert {alert_n} 次** "
            f"(active={active_n} skip={skip_n})——需查 journalctl"
        )

    # === Daily pipeline ===
    if daily.start_today and daily.exit_today and daily.exit_status == 0:
        lines.append(
            f"✅ daily: {_fmt_time(daily.start_today)} → {_fmt_time(daily.exit_today)} "
            f"({_fmt_duration(daily.duration_sec)})"
        )
    elif daily.exit_status is not None and daily.exit_status != 0 and daily.exit_today:
        lines.append(
            f"❌ daily: status={daily.exit_status} ({_fmt_time(daily.exit_today)}) — 已推 OnFailure"
        )
    else:
        # 15:30 报告时 daily 17:00 还没跑——正常
        lines.append("⏰ daily: 17:00 还未触发")

    # === 业务数据 ===
    pl = business.get("price_level_events", 0)
    pool1 = business.get("screen_n-shape-pool1", 0)
    pool2 = business.get("screen_n-shape-pool2", 0)
    if is_trading_day_flag:
        lines.append(
            f"📈 业务: price_level 触发 {pl} 条 · "
            f"pool1 待筛选(17:00 后) · pool2 待筛选"
        )
    else:
        lines.append("📈 业务: 非交易日，无业务数据更新")

    body = "\n".join(lines)
    return subject, body


# ---------- 顶层入口 ----------


def generate_and_send_daily_report(
    today: date | None = None,
    dry_run: bool = False,
) -> int:
    """生成 + 发送日报。返回发送成功的通道数（0 = 全失败 / dry_run）。

    :param dry_run: True 时只打印不推送，给 mac 本地 smoke 测试用，
                    避免误推到刘哥手机。
    """
    from rquant.monitor import is_trading_day
    from rquant.notify.client import PushDeerClient, PushPlusClient

    today = today or date.today()
    trading_day = is_trading_day(today)

    monitor_snap = get_service_snapshot("rquant-monitor.service", today)
    daily_snap = get_service_snapshot("rquant-daily.service", today)
    watchdog_counts = read_watchdog_log(settings.log_dir, today)

    # 5/13 复盘 Bug A：daily-report 不写 db（count_today_business_data 是纯 SELECT），
    # 改 read_only=True 才能跟 monitor / nl-screen 共存。原默认写模式在 5/1 节假日
    # 撞了 nl-screen 旧版持的写锁（PID 2597296），daily-report fatal exit。
    # 套路同 v0.12.1 nl-screen hotfix。
    with DuckDBStore(read_only=True) as store:
        business = count_today_business_data(store, today)

    subject, body = build_daily_report(
        today, trading_day, monitor_snap, daily_snap, watchdog_counts, business,
    )

    logger.info(f"日报 subject: {subject}")
    logger.info(f"日报 body:\n{body}")

    if dry_run:
        logger.info("[dry-run] 跳过推送")
        return 0

    keys = settings.pushdeer_key_list
    tokens = settings.pushplus_token_list
    if not keys and not tokens:
        logger.warning("无 PushDeer / PushPlus 配置，仅打印日报")
        return 0

    success = 0
    if keys:
        for s, err in PushDeerClient(keys, settings.pushdeer_endpoint).push(subject, body):
            success += int(s)
            if not s:
                logger.error(f"PushDeer 推送失败: {err}")
    if tokens:
        for s, err in PushPlusClient(tokens, settings.pushplus_endpoint).push(subject, body):
            success += int(s)
            if not s:
                logger.error(f"PushPlus 推送失败: {err}")

    logger.info(f"日报推送: {success} 通道成功")
    return success
