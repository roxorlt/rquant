"""开盘前主动健康体检（systemd timer Mon..Fri 09:00 触发）。

每个交易日开盘前 30 分钟跑一次，主动发现：
- DuckDB 文件锁状态异常（如多个写锁持有者会撞 monitor，5/6 incident 那种）
- 磁盘剩余不足（DuckDB 200MB+ 仍在涨）
- Tushare 积分余额不足以支撑当日 daily
- 各 systemd 服务状态异常
- 近 24h ERROR 日志条数超阈

输出经 PushDeer 推一条摘要：
- 全 ok          → "[RQ] ✅ 9:00 体检通过"
- 有 warn / fail → "[RQ] ⚠️ 9:00 体检发现 N 项"

非交易日由 systemd timer (Mon..Fri) 自然过滤；不在模块内做日历检查（保持模块纯函数）。
"""

from __future__ import annotations

import os
import shutil
import subprocess
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from loguru import logger

from rquant.config import settings
from rquant.daily_receipt_socket_authority import (
    probe_daily_receipt_authority_identity,
)
from rquant.runtime_deployment_preflight import (
    RuntimeDeploymentInspection,
    inspect_runtime_deployment,
)

CheckStatus = Literal["ok", "warn", "fail", "skip"]


@dataclass
class CheckResult:
    name: str
    status: CheckStatus
    message: str


# rQuant 当前部署的 systemd unit 全集（Linux only；mac 上 skip 整段）
SERVICES_TO_CHECK = [
    "rquant-monitor.service",
    "rquant-monitor.timer",
    "rquant-monitor-watchdog.timer",
    "rquant-daily.timer",
    "rquant-dashboard.service",
    "rquant-page-control.service",
    "rquant-nl-screen.service",
    "rquant-backup.timer",
    "rquant-daily-report.timer",
    "rquant-daily-receipt-signer.socket",
    "rquant-daily-receipt-signer.service",
]


# ---------- 单项检查 ----------


def check_duckdb_lock(path: Path) -> CheckResult:
    """lsof 看 DuckDB 文件谁开着 + RW/RO 模式。

    期望：≤1 个写锁持有者。多个写锁就是 5/6 incident 那种状态，monitor 启动会撞死。
    """
    if not shutil.which("lsof"):
        return CheckResult("duckdb_lock", "skip", "lsof 不可用")
    if not path.exists():
        return CheckResult("duckdb_lock", "warn", f"DuckDB 文件不存在: {path}")
    try:
        r = subprocess.run(
            ["lsof", str(path)],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except subprocess.TimeoutExpired:
        return CheckResult("duckdb_lock", "fail", "lsof 5s 超时")

    # lsof 无持有者时退出码 1 + 空 stdout，正常
    if r.returncode not in (0, 1):
        return CheckResult(
            "duckdb_lock",
            "fail",
            f"lsof exit={r.returncode}: {r.stderr.strip()[:80]}",
        )

    rw_holders: list[str] = []
    ro_holders: list[str] = []
    # lsof 头一行是表头，跳过
    for line in r.stdout.splitlines()[1:]:
        parts = line.split()
        if len(parts) < 4:
            continue
        cmd, pid, _user, fd = parts[0], parts[1], parts[2], parts[3]
        # FD 列结尾字符：u=RW, r=RO, w=WO
        if fd.endswith(("u", "w")):
            rw_holders.append(f"{cmd}({pid})")
        elif fd.endswith("r"):
            ro_holders.append(f"{cmd}({pid})")
        # 其他字符（如 cwd / txt）不算

    if len(rw_holders) > 1:
        return CheckResult(
            "duckdb_lock",
            "fail",
            f"{len(rw_holders)} 个写锁持有者：{', '.join(rw_holders)} — monitor 启动会撞锁",
        )
    if len(rw_holders) == 1:
        return CheckResult(
            "duckdb_lock",
            "ok",
            f"RW: {rw_holders[0]} + RO: {len(ro_holders)} 个",
        )
    return CheckResult(
        "duckdb_lock",
        "ok",
        f"无写锁（monitor 未跑），RO 持有: {len(ro_holders)} 个",
    )


def check_disk_space(path: Path, warn_gb: float = 5.0) -> CheckResult:
    """path 所在分区剩余空间，<warn_gb 报警。"""
    target = path if path.exists() else path.parent
    if not target.exists():
        return CheckResult("disk", "fail", f"路径不存在: {path}")
    usage = shutil.disk_usage(target)
    free_gb = usage.free / (1024**3)
    total_gb = usage.total / (1024**3)
    pct_used = usage.used / usage.total * 100
    msg = f"剩余 {free_gb:.1f}GB / {total_gb:.0f}GB ({pct_used:.0f}% 已用)"
    if free_gb < warn_gb:
        return CheckResult("disk", "warn", f"{msg} — 阈值 {warn_gb}GB")
    return CheckResult("disk", "ok", msg)


def check_tushare_credits(token: str | None, warn_threshold: int = 500) -> CheckResult:
    """Tushare Pro 积分余额，<warn_threshold 报警。

    本检查失败统一降级为 warn 而非 fail：
    - 5/25 真实事故：tushare 服务端把内部 `user` 接口禁用（"请指定正确的接口名"），
      没有公开替代品，但 token 整体可用（trade_cal 等业务接口仍正常）
    - 积分查询挂掉对业务无影响（业务 ingest 用 daily / pro_bar 等接口）
    - 真的 token 失效会在 daily pipeline 立即暴露并 fail，由那里触发 alert
    - 体检每天 9:00 跑，如果 tushare 偶发故障 fail 会每天推一条噪音 alert
    """
    if not token:
        return CheckResult("tushare", "skip", "TUSHARE_TOKEN_MAIN 未配置")
    try:
        import tushare as ts
    except ImportError:
        return CheckResult("tushare", "skip", "tushare 未安装")
    try:
        pro = ts.pro_api(token)
        df = pro.user(token=token)
    except Exception as e:
        return CheckResult("tushare", "warn", f"{type(e).__name__}: {str(e)[:80]}")
    if df is None or df.empty:
        return CheckResult("tushare", "warn", "user API 返回空")
    # Tushare user API 一个 user_id 可能返回多行（基础积分 + 付费包），需要 sum
    # 字段名：到期积分（中文）/ points（英文）/ 兼容老版本「积分」
    for col in ("到期积分", "积分", "points"):
        if col in df.columns:
            try:
                pts = int(df[col].sum())
            except (ValueError, TypeError):
                return CheckResult("tushare", "warn", f"{col} 字段无法 sum: {df[col].tolist()!r}")
            # 取最早的到期时间（多行付费包可能不同到期）
            expire = ""
            if "到期时间" in df.columns:
                with suppress(Exception):
                    expire = str(df["到期时间"].min())[:10]
            expire_msg = f"，{expire} 到期" if expire else ""
            n_rows = len(df)
            rows_msg = f"（{n_rows} 个积分包合计）" if n_rows > 1 else ""
            if pts < warn_threshold:
                return CheckResult(
                    "tushare",
                    "warn",
                    f"{pts} 积分{rows_msg} (低于阈值 {warn_threshold}){expire_msg}",
                )
            return CheckResult("tushare", "ok", f"{pts} 积分{rows_msg}{expire_msg}")
    return CheckResult("tushare", "warn", f"无法识别积分字段：columns={list(df.columns)}")


def check_systemd_services(units: list[str]) -> list[CheckResult]:
    """各 unit 的 systemctl is-active 状态。

    - active            → ok
    - inactive (oneshot/timer 正常退出后状态) → ok
    - activating/deactivating → warn（瞬态）
    - failed / 其他     → fail
    """
    if not shutil.which("systemctl"):
        return [CheckResult("services", "skip", "无 systemctl（mac 本地 dev）")]
    results: list[CheckResult] = []
    for unit in units:
        try:
            r = subprocess.run(
                ["systemctl", "is-active", unit],
                capture_output=True,
                text=True,
                timeout=5,
            )
        except subprocess.TimeoutExpired:
            results.append(CheckResult(f"svc:{unit}", "fail", "systemctl 超时"))
            continue
        state = r.stdout.strip() or "unknown"
        if state == "active":
            results.append(CheckResult(f"svc:{unit}", "ok", state))
        elif state == "inactive":
            # oneshot / timer 退出后正常状态；timer 自身一直 active
            results.append(CheckResult(f"svc:{unit}", "ok", "inactive (正常)"))
        elif state in ("activating", "deactivating"):
            results.append(CheckResult(f"svc:{unit}", "warn", state))
        else:
            results.append(CheckResult(f"svc:{unit}", "fail", state))
    return results


def check_daily_receipt_authority_identity() -> CheckResult:
    """Authenticate the source SHA served by the fixed root signer socket."""

    if not shutil.which("systemctl"):
        return CheckResult(
            "daily_receipt_authority_identity",
            "skip",
            "无 systemctl（mac 本地 dev）",
        )
    try:
        identity = probe_daily_receipt_authority_identity(
            timeout_seconds=2.0,
            max_attempts=1,
        )
    except Exception as exc:
        return CheckResult(
            "daily_receipt_authority_identity",
            "fail",
            f"identity probe 失败: {type(exc).__name__}: {str(exc)[:160]}",
        )
    return CheckResult(
        "daily_receipt_authority_identity",
        "ok",
        f"running_sha={identity.source_sha256} key={identity.key_id}",
    )


def runtime_deployment_service_checks(
    runtime_root: Path,
) -> tuple[RuntimeDeploymentInspection, CheckResult]:
    inspection = inspect_runtime_deployment(runtime_root)
    return (
        inspection,
        CheckResult(
            "watchlist_quote_runtime",
            inspection.status,
            "{}; inventory={}".format(
                inspection.summary,
                ",".join(inspection.inventory_units) or "none",
            ),
        ),
    )


def check_recent_errors(
    units: list[str],
    hours: int = 24,
    warn_count: int = 10,
) -> CheckResult:
    """journalctl 扫近 hours 小时各 unit 的 priority>=err 条数。"""
    if not shutil.which("journalctl"):
        return CheckResult("recent_errors", "skip", "无 journalctl")
    total = 0
    for unit in units:
        try:
            r = subprocess.run(
                [
                    "journalctl",
                    "-u",
                    unit,
                    f"--since={hours} hours ago",
                    "-p",
                    "err",
                    "--no-pager",
                    "--quiet",
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )
        except subprocess.TimeoutExpired:
            logger.warning(f"journalctl -u {unit} 超时，跳过此 unit")
            continue
        # journalctl 无条目时 stdout 完全空（--quiet）；有条目时每行一条
        lines = [ln for ln in r.stdout.splitlines() if ln.strip()]
        total += len(lines)
    if total >= warn_count:
        return CheckResult(
            "recent_errors",
            "warn",
            f"近 {hours}h ERROR {total} 条 (阈值 {warn_count})",
        )
    return CheckResult("recent_errors", "ok", f"近 {hours}h ERROR {total} 条")


# ---------- 聚合 + 输出 ----------


def run_all_checks(*, runtime_root: Path | None = None) -> list[CheckResult]:
    """跑所有体检，返回结果列表。"""
    resolved_runtime_root = runtime_root or Path(
        os.environ.get("RQUANT_RUNTIME_ROOT", str(settings.data_dir / "runtime"))
    )
    runtime_inspection, runtime_result = runtime_deployment_service_checks(resolved_runtime_root)
    systemd_units = list(
        dict.fromkeys((*SERVICES_TO_CHECK, *runtime_inspection.strict_authority_units))
    )
    journal_units = list(dict.fromkeys((*SERVICES_TO_CHECK, *runtime_inspection.inventory_units)))
    results: list[CheckResult] = []
    results.append(check_duckdb_lock(settings.duckdb_path))
    results.append(check_disk_space(settings.duckdb_path.parent))
    results.append(check_tushare_credits(settings.tushare_token_main))
    results.extend(check_systemd_services(systemd_units))
    results.append(runtime_result)
    results.append(check_daily_receipt_authority_identity())
    results.append(check_recent_errors(journal_units))
    return results


_ICON = {"ok": "✓", "warn": "⚠️", "fail": "❌", "skip": "—"}


def format_summary(results: list[CheckResult]) -> tuple[str, str]:
    """构造 (subject, body)。

    subject 含项目前缀 [RQ]，与 alert@.service 的 OnFailure 模板风格统一。
    body 一行一项，icon + name + message。
    """
    fails = [r for r in results if r.status == "fail"]
    warns = [r for r in results if r.status == "warn"]
    issues = len(fails) + len(warns)

    if issues == 0:
        subject = "[RQ] ✅ 9:00 体检通过"
    else:
        parts = []
        if fails:
            parts.append(f"{len(fails)} 项失败")
        if warns:
            parts.append(f"{len(warns)} 项预警")
        subject = f"[RQ] ⚠️ 9:00 体检：{' + '.join(parts)}"

    lines = [f"{_ICON[r.status]} {r.name}: {r.message}" for r in results]
    body = "\n".join(lines)
    return subject, body
