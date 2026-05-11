"""rquant preflight — 大改动 / 节后第一天的全家服务深度体检。

跟 pre-market-check 的差别：
- pre-market-check：systemd timer 09:00 定时跑，**被动**轻量（剩余 < 30s），重点在
  「快速发现已知 5 类问题」
- preflight：**手动**触发的「全面 dry-run」，深度检查 unit 文件 / 锁布局细节 /
  数据新鲜度 / smoke 跑一次 screen()，重点在「大改动后/节后能不能开盘」

不重启服务，不动数据，纯 dry-run。

输出：markdown 报告到 stdout。--notify 推一条摘要到 PushDeer。

典型场景：
1. 节后第一个交易日 09:00 前手动跑（pre-market-check 之前/之后）
2. 大 PR merge 后 deploy.sh 跑完手动跑
3. 怀疑系统状态时随手跑
"""

from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Literal

from loguru import logger

from rquant.config import settings


CheckStatus = Literal["ok", "warn", "fail", "skip"]


@dataclass
class CheckResult:
    name: str
    status: CheckStatus
    summary: str
    details: list[str] = field(default_factory=list)


# 跟 pre_market_check.SERVICES_TO_CHECK 保持一致（这俩 module 共享同一份服务清单 ground truth）
SERVICES_TO_CHECK = [
    "rquant-monitor.service",
    "rquant-monitor.timer",
    "rquant-monitor-watchdog.timer",
    "rquant-daily.timer",
    "rquant-dashboard.service",
    "rquant-nl-screen.service",
    "rquant-backup.timer",
    "rquant-daily-report.timer",
    "rquant-pre-market-check.timer",
]

# 业务表 → 期望最新数据「不该比今天落后超过 N 天」（节假日除外）
TABLE_FRESHNESS_DAYS = {
    "daily_bar": 5,        # 含周末/节假日缓冲
    "screen_result": 5,
    "monitor_event": 30,   # monitor_event 只有交易日有，宽松一点
}


# ---------- 1. systemd unit 文件验证 ----------


def verify_unit_files(systemd_dir: Path) -> CheckResult:
    """对 deploy/systemd/*.{service,timer} 跑 systemd-analyze verify。"""
    if not shutil.which("systemd-analyze"):
        return CheckResult(
            "unit_files", "skip", "无 systemd-analyze（mac 本地）",
        )
    if not systemd_dir.exists():
        return CheckResult(
            "unit_files", "fail", f"systemd 目录不存在: {systemd_dir}",
        )
    units = sorted(
        list(systemd_dir.glob("*.service")) + list(systemd_dir.glob("*.timer"))
    )
    if not units:
        return CheckResult("unit_files", "warn", f"{systemd_dir} 下无 unit 文件")

    failed: list[str] = []
    details: list[str] = []
    for unit in units:
        try:
            r = subprocess.run(
                ["systemd-analyze", "verify", str(unit)],
                capture_output=True, text=True, timeout=10,
            )
        except subprocess.TimeoutExpired:
            failed.append(f"{unit.name}: timeout")
            continue
        # systemd-analyze verify 退出码 != 0 才是真错；stderr 可能含其他系统 unit
        # 的 warning（如 tat_agent.service 的 PIDFile legacy 提示），不该归到我们头上。
        # 但若 stderr 里出现「我们这个 unit 名」的具体错误行，也算 fail。
        own_errors = [
            ln for ln in r.stderr.splitlines()
            if unit.name in ln and ":" in ln
        ]
        if r.returncode == 0 and not own_errors:
            details.append(f"  ✓ {unit.name}")
        else:
            msg = "; ".join(own_errors)[:160] if own_errors else f"exit={r.returncode}"
            failed.append(f"{unit.name}: {msg}")
            details.append(f"  ✗ {unit.name}")

    if failed:
        return CheckResult(
            "unit_files", "fail",
            f"{len(failed)}/{len(units)} unit 验证失败",
            details + [""] + [f"  失败详情: {f}" for f in failed],
        )
    return CheckResult(
        "unit_files", "ok",
        f"{len(units)} 个 unit 全部 verify 通过",
        details,
    )


# ---------- 2. systemd 服务状态详情 ----------


def detail_systemd_state(units: list[str]) -> CheckResult:
    """列出每个 unit 的详细状态：active / sub_state / 最近 start / restart count。"""
    if not shutil.which("systemctl"):
        return CheckResult("systemd_state", "skip", "无 systemctl（mac 本地）")

    details: list[str] = []
    has_failed = False
    for unit in units:
        try:
            r = subprocess.run(
                [
                    "systemctl", "show", unit,
                    "--property=ActiveState,SubState,NRestarts,"
                    "ExecMainStartTimestamp,Result",
                ],
                capture_output=True, text=True, timeout=5,
            )
        except subprocess.TimeoutExpired:
            details.append(f"  ⏱ {unit}: systemctl 超时")
            continue
        props = dict(
            line.split("=", 1) for line in r.stdout.strip().split("\n")
            if "=" in line
        )
        active = props.get("ActiveState", "?")
        sub = props.get("SubState", "?")
        n_restart = props.get("NRestarts", "0")
        start_ts = props.get("ExecMainStartTimestamp", "").strip() or "—"
        result = props.get("Result", "?")

        # 判级
        if active == "failed" or result == "failed":
            icon = "✗"
            has_failed = True
        elif active in ("activating", "deactivating"):
            icon = "⚠"
        else:
            icon = "✓"

        line = f"  {icon} {unit}: {active}/{sub}"
        if int(n_restart) > 0:
            line += f", restarts={n_restart}"
        if start_ts != "—":
            line += f", started={start_ts}"
        details.append(line)

    if has_failed:
        return CheckResult(
            "systemd_state", "fail",
            f"{len(units)} 个 unit 中有 failed 状态", details,
        )
    return CheckResult(
        "systemd_state", "ok",
        f"{len(units)} 个 unit 全部 active/正常 inactive", details,
    )


# ---------- 3. DuckDB 锁布局详情（pre-market-check 简版的扩展） ----------


def detail_duckdb_lock(path: Path) -> CheckResult:
    """lsof 看 DuckDB 文件，输出每个持有者的 PID + COMMAND + FD 模式（u/r/w）。"""
    if not shutil.which("lsof"):
        return CheckResult("duckdb_lock_detail", "skip", "lsof 不可用")
    if not path.exists():
        return CheckResult(
            "duckdb_lock_detail", "warn", f"DuckDB 文件不存在: {path}",
        )
    try:
        r = subprocess.run(
            ["lsof", str(path)], capture_output=True, text=True, timeout=5,
        )
    except subprocess.TimeoutExpired:
        return CheckResult("duckdb_lock_detail", "fail", "lsof 5s 超时")

    if r.returncode not in (0, 1):
        return CheckResult(
            "duckdb_lock_detail", "fail",
            f"lsof exit={r.returncode}: {r.stderr.strip()[:80]}",
        )

    rw_holders: list[tuple[str, str]] = []  # (pid, "COMMAND(rw)")
    ro_holders: list[tuple[str, str]] = []
    other: list[str] = []

    for line in r.stdout.splitlines()[1:]:
        parts = line.split()
        if len(parts) < 4:
            continue
        cmd, pid, _user, fd = parts[0], parts[1], parts[2], parts[3]
        # FD 末尾 u/w = 写，r = 读
        if fd.endswith(("u", "w")):
            rw_holders.append((pid, f"{cmd} (FD={fd})"))
        elif fd.endswith("r"):
            ro_holders.append((pid, f"{cmd} (FD={fd})"))
        else:
            other.append(f"{cmd} pid={pid} FD={fd}")

    details = [f"  RW (写锁) 持有者: {len(rw_holders)}"]
    for pid, desc in rw_holders:
        details.append(f"    pid={pid} {desc}")
    details.append(f"  RO (读锁) 持有者: {len(ro_holders)}")
    for pid, desc in ro_holders[:5]:  # 最多列 5 个 RO
        details.append(f"    pid={pid} {desc}")
    if len(ro_holders) > 5:
        details.append(f"    ... 还有 {len(ro_holders) - 5} 个")
    if other:
        details.append(f"  其他 FD 类型: {len(other)}")

    if len(rw_holders) > 1:
        return CheckResult(
            "duckdb_lock_detail", "fail",
            f"{len(rw_holders)} 个写锁持有者 — 监控启动会撞锁（5/6 incident 重现）",
            details,
        )
    if len(rw_holders) == 1:
        return CheckResult(
            "duckdb_lock_detail", "ok",
            f"单写锁正常 + {len(ro_holders)} 个读锁",
            details,
        )
    return CheckResult(
        "duckdb_lock_detail", "ok",
        f"无写锁（monitor 当前未跑），{len(ro_holders)} 个读锁",
        details,
    )


# ---------- 4. 数据新鲜度 ----------


def check_data_freshness(
    table_max_age: dict[str, int] = None,
) -> CheckResult:
    """各核心表的最新 trade_date / row count / 距今天数。"""
    table_max_age = table_max_age or TABLE_FRESHNESS_DAYS
    try:
        from rquant.storage.duckdb import DuckDBStore
        store = DuckDBStore(read_only=True)
    except Exception as e:
        return CheckResult(
            "data_freshness", "fail",
            f"DuckDB 打开失败: {type(e).__name__}: {e}",
        )

    today = date.today()
    details: list[str] = []
    has_warn = False
    has_fail = False

    for table, max_days in table_max_age.items():
        try:
            row = store._conn.execute(
                f"SELECT MAX(trade_date), COUNT(*) FROM {table}"
            ).fetchone()
        except Exception as e:
            details.append(f"  ✗ {table}: 查询失败 {type(e).__name__}")
            has_fail = True
            continue
        latest, total = row
        if latest is None:
            details.append(f"  ⚠ {table}: 空表")
            has_warn = True
            continue
        # latest 可能是 date 或 str 视 DuckDB 版本而定
        latest_date = (
            latest if isinstance(latest, date) else datetime.fromisoformat(str(latest)).date()
        )
        age_days = (today - latest_date).days
        line = f"  {table}: latest={latest_date} ({age_days}d ago), total={total:,} 行"
        if age_days > max_days:
            details.append(f"  ⚠ {line}")
            details.append(f"    阈值 {max_days}d，落后 {age_days - max_days}d")
            has_warn = True
        else:
            details.append(f"  ✓ {line}")

    status = "fail" if has_fail else ("warn" if has_warn else "ok")
    n = len(table_max_age)
    if status == "ok":
        summary = f"{n} 表新鲜"
    elif status == "warn":
        summary = f"{n} 表中部分落后于阈值"
    else:
        summary = f"{n} 表中部分查询失败"
    return CheckResult("data_freshness", status, summary, details)


# ---------- 5. screen() smoke ----------


def smoke_screen() -> CheckResult:
    """跑一个最简 PRESET_SCREENS 端到端，确认 screen 流水线还活着。"""
    try:
        from rquant.presets import PRESET_SCREENS
        from rquant.screen.core import screen
    except Exception as e:
        return CheckResult(
            "smoke_screen", "fail",
            f"import 失败: {type(e).__name__}: {e}",
        )

    if not PRESET_SCREENS:
        return CheckResult(
            "smoke_screen", "warn", "PRESET_SCREENS 为空",
        )

    # 选一个 baseline preset（有就用 n-shape-pool1，否则取第一个）
    name = "n-shape-pool1" if "n-shape-pool1" in PRESET_SCREENS else next(iter(PRESET_SCREENS))
    preset = PRESET_SCREENS[name]

    try:
        from rquant.storage.duckdb import DuckDBStore
        store = DuckDBStore(read_only=True)
        latest_row = store._conn.execute(
            "SELECT MAX(trade_date) FROM screen_result WHERE preset_name = ?",
            [name],
        ).fetchone()
    except Exception as e:
        return CheckResult(
            "smoke_screen", "fail",
            f"DuckDB 探查失败: {type(e).__name__}: {e}",
        )

    latest = latest_row[0] if latest_row else None
    if latest is None:
        return CheckResult(
            "smoke_screen", "warn",
            f"preset {name} 在 screen_result 中无历史数据，跳过 smoke",
        )

    trade_date_str = (
        latest if isinstance(latest, str) else latest.isoformat()
    )

    try:
        start = datetime.now()
        df = screen(
            trade_date_str, preset.rules,
            include_columns=preset.include_columns or None,
            store=store,
        )
        elapsed = (datetime.now() - start).total_seconds()
    except Exception as e:
        return CheckResult(
            "smoke_screen", "fail",
            f"screen() 抛异常: {type(e).__name__}: {e}",
        )

    summary = f"preset={name} trade_date={trade_date_str} hits={len(df)} 用时 {elapsed:.2f}s"
    if elapsed > 30:
        return CheckResult("smoke_screen", "warn", summary + "（>30s 偏慢）")
    return CheckResult("smoke_screen", "ok", summary)


# ---------- 聚合 + 输出 ----------


def run_all_checks(systemd_dir: Path | None = None) -> list[CheckResult]:
    """跑全部体检。systemd_dir 默认从项目根推断。"""
    project_root = Path(__file__).resolve().parents[2]
    systemd_dir = systemd_dir or (project_root / "deploy" / "systemd")

    results: list[CheckResult] = []
    results.append(verify_unit_files(systemd_dir))
    results.append(detail_systemd_state(SERVICES_TO_CHECK))
    results.append(detail_duckdb_lock(settings.duckdb_path))
    results.append(check_data_freshness())
    results.append(smoke_screen())
    return results


_ICON = {"ok": "✓", "warn": "⚠️", "fail": "❌", "skip": "—"}


def format_report(results: list[CheckResult]) -> str:
    """Markdown 报告（多行，stdout 友好）。"""
    fails = sum(1 for r in results if r.status == "fail")
    warns = sum(1 for r in results if r.status == "warn")
    oks = sum(1 for r in results if r.status == "ok")
    skips = sum(1 for r in results if r.status == "skip")

    if fails == 0 and warns == 0:
        header = "# [RQ] ✅ Preflight 全部通过"
    elif fails == 0:
        header = f"# [RQ] ⚠️ Preflight: {warns} 项预警"
    else:
        header = f"# [RQ] ❌ Preflight: {fails} 项失败 + {warns} 项预警"

    lines = [
        header,
        f"_{datetime.now():%Y-%m-%d %H:%M:%S} | ok={oks} warn={warns} fail={fails} skip={skips}_",
        "",
    ]
    for r in results:
        lines.append(f"## {_ICON[r.status]} {r.name}")
        lines.append(f"  {r.summary}")
        if r.details:
            lines.append("")
            for d in r.details:
                lines.append(d)
        lines.append("")

    return "\n".join(lines)


def format_pushdeer_summary(results: list[CheckResult]) -> tuple[str, str]:
    """PushDeer 用的紧凑摘要（subject + body）。"""
    fails = [r for r in results if r.status == "fail"]
    warns = [r for r in results if r.status == "warn"]
    issues = len(fails) + len(warns)

    if issues == 0:
        subject = "[RQ] ✅ Preflight 通过"
    elif fails:
        subject = f"[RQ] ❌ Preflight: {len(fails)} 失败 + {len(warns)} 预警"
    else:
        subject = f"[RQ] ⚠️ Preflight: {len(warns)} 项预警"

    lines = [
        f"{_ICON[r.status]} {r.name}: {r.summary}" for r in results
    ]
    body = "\n".join(lines)
    return subject, body
