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

import os
import re
import shutil
import sqlite3
import stat
import subprocess
import zipfile
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from typing import Literal

from rquant.config import settings
from rquant.daily_receipt_socket_authority import (
    DAILY_RECEIPT_SOCKET_ENDPOINT,
    DAILY_RECEIPT_TRUSTED_KEYRING_PATH,
    probe_daily_receipt_authority_identity,
)
from rquant.data_contracts import (
    CONTRACTS_BY_ID,
    EXCHANGE_TIMEZONE,
    DatasetContract,
    VisibilityRule,
)
from rquant.runtime_deployment_preflight import (
    RuntimeDeploymentInspection,
    inspect_runtime_deployment,
)
from rquant.runtime_recovery_artifacts import (
    RecoveryPayloadVerifier,
    RecoveryVerificationBudget,
    load_verified_real_recovery_receipt,
)
from rquant.runtime_recovery_backup import load_recovery_backup_generation
from rquant.runtime_recovery_service import load_verified_recovery_service_receipts
from rquant.workload_isolation import (
    WORKLOAD_ARBITER_HASH_PATH,
    WORKLOAD_ARBITER_PATH,
    ReceiptBoundWorkloadAdvisories,
    WorkloadCheck,
    check_workload_capacity_baseline,
    check_workload_high_water_evidence,
    check_workload_runtime,
    verify_workload_unit_declarations,
)

CheckStatus = Literal["ok", "warn", "fail", "skip"]


@dataclass
class CheckResult:
    name: str
    status: CheckStatus
    summary: str
    details: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class WorkloadRuntimeProbeConfig:
    cgroup_root: Path = Path("/sys/fs/cgroup")
    platform_name: str | None = None
    systemctl_path: Path = Path("/usr/bin/systemctl")
    strict: bool = False
    arbiter_path: Path = Path(WORKLOAD_ARBITER_PATH)
    arbiter_hash_path: Path = Path(WORKLOAD_ARBITER_HASH_PATH)
    arbiter_expected_uid: int = 0


def _workload_check_result(result: WorkloadCheck) -> CheckResult:
    return CheckResult(result.name, result.status, result.summary, list(result.details))


def _safe_workload_probe(
    name: str,
    probe: Callable[[], WorkloadCheck],
) -> WorkloadCheck:
    try:
        return probe()
    except (OSError, subprocess.TimeoutExpired) as exc:
        return WorkloadCheck(
            name,
            "fail",
            f"{type(exc).__name__}: {str(exc)[:160]}",
        )


@dataclass(frozen=True)
class RuntimeRecoveryPreflightConfig:
    publication_root: Path
    service_state_path: Path
    service_receipt_root: Path
    restore_root: Path
    expected_profile_generation: str
    expected_manifest_id: str | None
    max_rpo: timedelta
    max_rehearsal_age: timedelta
    max_rto: timedelta
    verification_budget: RecoveryVerificationBudget = field(
        default_factory=RecoveryVerificationBudget
    )
    trusted_backup_verifiers: Mapping[str, RecoveryPayloadVerifier] | None = field(
        default=None,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        for name in ("max_rpo", "max_rehearsal_age", "max_rto"):
            if getattr(self, name) <= timedelta(0):
                raise ValueError(f"{name} must be positive")


def verify_runtime_recovery(
    config: RuntimeRecoveryPreflightConfig,
    *,
    as_of: datetime | None = None,
) -> CheckResult:
    """Fail closed unless the current generation has a fresh successful rehearsal."""

    observed_at = (as_of or datetime.now(UTC)).astimezone(UTC)
    try:
        pointer, backup, target, _tool, _expectations = load_recovery_backup_generation(
            config.publication_root,
            trusted_verifiers=config.trusted_backup_verifiers,
        )
    except Exception as exc:
        return CheckResult(
            "runtime_recovery",
            "fail",
            f"recovery backup/外部 head 验证失败: {type(exc).__name__}",
        )
    if (
        pointer.profile_generation != config.expected_profile_generation
        or target.target_profile_generation != config.expected_profile_generation
    ):
        return CheckResult("runtime_recovery", "fail", "recovery profile generation 已过期")
    expected_manifest_id = config.expected_manifest_id or pointer.manifest_id
    if pointer.manifest_id != expected_manifest_id:
        return CheckResult("runtime_recovery", "fail", "recovery manifest generation 已过期")

    backup_age = observed_at - backup.completed_at.astimezone(UTC)
    if backup_age < timedelta(0) or backup_age > config.max_rpo:
        return CheckResult(
            "runtime_recovery",
            "fail",
            f"recovery RPO 超限: age={backup_age.total_seconds():.0f}s",
        )
    try:
        service_receipts = load_verified_recovery_service_receipts(
            state_path=config.service_state_path,
            receipt_root=config.service_receipt_root,
        )
    except Exception as exc:
        return CheckResult(
            "runtime_recovery",
            "fail",
            f"recovery rehearsal receipt 验证失败: {type(exc).__name__}",
        )
    if not service_receipts:
        return CheckResult("runtime_recovery", "fail", "recovery rehearsal receipt 缺失")
    latest = max(service_receipts, key=lambda item: (item.completed_at, str(item.receipt_id)))
    if latest.status == "failed":
        return CheckResult("runtime_recovery", "fail", "latest recovery rehearsal failed")
    if (
        latest.status != "succeeded"
        or latest.verification_level != "full"
        or latest.recovery_receipt_id is None
    ):
        return CheckResult(
            "runtime_recovery",
            "fail",
            "latest recovery receipt 不是成功的 full rehearsal",
        )
    rehearsal_age = observed_at - latest.completed_at.astimezone(UTC)
    if rehearsal_age < timedelta(0) or rehearsal_age > config.max_rehearsal_age:
        return CheckResult(
            "runtime_recovery",
            "fail",
            f"recovery rehearsal 已过期: age={rehearsal_age.total_seconds():.0f}s",
        )
    try:
        restored_pointer, recovery = load_verified_real_recovery_receipt(
            restore_root=config.restore_root,
            receipt_id=str(latest.recovery_receipt_id),
            target=target,
            verification_budget=config.verification_budget,
        )
    except Exception as exc:
        return CheckResult(
            "runtime_recovery",
            "fail",
            f"recovery restore receipt 验证失败: {type(exc).__name__}",
        )
    if (
        recovery.manifest_id != expected_manifest_id
        or restored_pointer.generation_id != expected_manifest_id
        or recovery.target_profile_generation != config.expected_profile_generation
    ):
        return CheckResult(
            "runtime_recovery",
            "fail",
            "recovery rehearsal 使用了旧 generation",
        )
    rto = recovery.completed_at.astimezone(UTC) - recovery.started_at.astimezone(UTC)
    if rto > config.max_rto:
        return CheckResult(
            "runtime_recovery",
            "fail",
            f"recovery RTO 超限: duration={rto.total_seconds():.0f}s",
        )
    return CheckResult(
        "runtime_recovery",
        "ok",
        "current recovery rehearsal、RPO/RTO 与外部 head 均有效",
        [
            f"  ✓ manifest={expected_manifest_id}",
            f"  ✓ profile={config.expected_profile_generation}",
            f"  ✓ RPO={backup_age.total_seconds():.0f}s/{config.max_rpo.total_seconds():.0f}s",
            f"  ✓ rehearsal_age={rehearsal_age.total_seconds():.0f}s",
            f"  ✓ RTO={rto.total_seconds():.0f}s/{config.max_rto.total_seconds():.0f}s",
            f"  ✓ paper_ledger_head={backup.paper_ledger_head.head_id}",
        ],
    )


def verify_source_quota_ledger(
    path: Path,
    *,
    source: str,
    profile: Literal["production", "candidate"] = "production",
) -> CheckResult:
    """Read-only audit of the durable provider-attempt ledger."""

    ledger_path = Path(path)
    if ledger_path.is_symlink():
        return CheckResult("source_quota", "fail", f"{source} quota ledger 路径不安全")
    if not ledger_path.exists():
        status: Literal["fail", "warn"] = "fail" if profile == "production" else "warn"
        return CheckResult(
            "source_quota",
            status,
            f"{source} quota ledger 尚未初始化（profile={profile}）",
        )
    if not ledger_path.is_file():
        return CheckResult("source_quota", "fail", f"{source} quota ledger 路径不安全")
    try:
        connection = sqlite3.connect(f"file:{ledger_path}?mode=ro", uri=True)
        try:
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if version < 3:
                return CheckResult(
                    "source_quota",
                    "fail",
                    f"{source} quota ledger schema 已过期",
                )
            required_columns = {
                "attempt_id",
                "source",
                "prepared_at",
                "dispatched_at",
                "outcome",
                "committed_at",
                "boot_id",
                "last_monotonic_ns",
                "lifecycle_sequence",
                "clock_rollback_count",
            }
            actual_columns = {
                str(row[1]) for row in connection.execute("PRAGMA table_info(quota_attempt)")
            }
            if not required_columns <= actual_columns:
                return CheckResult(
                    "source_quota",
                    "fail",
                    f"{source} quota ledger v3 schema 不完整",
                )
            invalid_lifecycle = int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM quota_attempt
                    WHERE dispatched_at < prepared_at
                       OR (committed_at IS NOT NULL AND committed_at < prepared_at)
                       OR (committed_at IS NOT NULL AND dispatched_at IS NOT NULL
                           AND committed_at < dispatched_at)
                       OR (outcome = 'pending' AND committed_at IS NOT NULL)
                       OR (outcome != 'pending' AND committed_at IS NULL)
                       OR lifecycle_sequence < 1
                       OR last_monotonic_ns < 0
                       OR clock_rollback_count < 0
                    """
                ).fetchone()[0]
            )
            if invalid_lifecycle:
                return CheckResult(
                    "source_quota",
                    "fail",
                    f"{source} quota ledger 生命周期记录无效: count={invalid_lifecycle}",
                )
            rows = connection.execute(
                """
                SELECT outcome, COUNT(*)
                FROM quota_attempt
                WHERE source = ?
                GROUP BY outcome
                """,
                (source,),
            ).fetchall()
        finally:
            connection.close()
    except (OSError, sqlite3.DatabaseError) as exc:
        return CheckResult(
            "source_quota",
            "fail",
            f"{source} quota ledger 不可审计: {type(exc).__name__}",
        )
    counts = {str(outcome): int(count) for outcome, count in rows}
    pending = counts.get("pending", 0)
    if pending:
        return CheckResult(
            "source_quota",
            "fail",
            f"{source} quota ledger 有未恢复 attempt: pending={pending}",
        )
    summary = ", ".join(
        f"{outcome}={counts.get(outcome, 0)}" for outcome in ("success", "failure", "unknown")
    )
    return CheckResult("source_quota", "ok", f"{source} quota ledger 可审计: {summary}")


# 跟 pre_market_check.SERVICES_TO_CHECK 保持一致（这俩 module 共享同一份服务清单 ground truth）
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
    "rquant-pre-market-check.timer",
    "rquant-daily-receipt-signer.socket",
    "rquant-daily-receipt-signer.service",
]

PRODUCTION_FRESHNESS_DATASET_IDS = (
    "daily_bar",
    "stock_status_daily",
    "minute_bar",
    "adj_factor",
    "stock_suspend_coverage",
)
RESEARCH_FRESHNESS_DATASET_IDS = tuple(CONTRACTS_BY_ID)
READONLY_REPLICA_MAX_SOURCE_LAG = timedelta(minutes=12)
FreshnessProfile = Literal["production", "research"]
DAILY_RECEIPT_AUTHORITY_ROOT = Path("/usr/local/libexec/rquant-daily-receipt-authority")
_DAILY_AUTHORITY_RELEASE_SHA = re.compile(r"^[0-9a-f]{64}$")

SMOKE_SCREEN_TABLES = (
    "screen_result",
    "daily_bar",
    "daily_indicator",
    "daily_state",
    "daily_basic",
    "stock_basic",
)


def verify_runtime_dependencies(
    *,
    ssh_keygen_path: Path = Path("/usr/bin/ssh-keygen"),
    platform_name: str | None = None,
    rpm_path: Path = Path("/usr/bin/rpm"),
) -> CheckResult:
    """Verify external binaries required by isolated runtime services."""

    if not ssh_keygen_path.is_absolute():
        return CheckResult("runtime_dependencies", "fail", "ssh-keygen 路径不是绝对路径")
    try:
        observed = ssh_keygen_path.lstat()
    except OSError:
        return CheckResult("runtime_dependencies", "fail", "ssh-keygen 不可用")
    if (
        not stat.S_ISREG(observed.st_mode)
        or observed.st_uid not in {0, os.geteuid()}
        or observed.st_nlink != 1
        or stat.S_IMODE(observed.st_mode) & 0o022
        or not os.access(ssh_keygen_path, os.X_OK)
    ):
        return CheckResult("runtime_dependencies", "fail", "ssh-keygen 安全属性不合格")

    resolved_platform = platform_name or ("Linux" if Path("/proc").is_dir() else "Darwin")
    details = [f"  ✓ ssh-keygen: {ssh_keygen_path}"]
    if resolved_platform == "Linux":
        try:
            completed = subprocess.run(
                [str(rpm_path), "-q", "openssh-clients"],
                check=False,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                timeout=5,
            )
        except (OSError, subprocess.TimeoutExpired):
            return CheckResult(
                "runtime_dependencies",
                "fail",
                "openssh-clients 包检查失败",
                details,
            )
        if completed.returncode != 0:
            return CheckResult(
                "runtime_dependencies",
                "fail",
                "openssh-clients 未安装",
                details,
            )
        details.append("  ✓ rpm: openssh-clients")
    return CheckResult("runtime_dependencies", "ok", "运行时外部依赖可用", details)


def verify_daily_receipt_authority_runtime(
    *,
    root: Path = DAILY_RECEIPT_AUTHORITY_ROOT,
    expected_uid: int = 0,
    platform_name: str | None = None,
    probe_identity: bool | None = None,
    identity_socket_path: Path = DAILY_RECEIPT_SOCKET_ENDPOINT,
    trusted_keyring_path: Path = DAILY_RECEIPT_TRUSTED_KEYRING_PATH,
    identity_timeout_seconds: float = 2.0,
) -> CheckResult:
    """Verify the root-only Daily signer release selected by ``current``.

    The service must use an immutable stdlib zipapp, never a runtime user's
    checkout or virtualenv.  The production path also probes the already-running
    socket authority and compares its in-memory source SHA with the selected
    release.  It never starts the signer or accesses private key material.
    """

    resolved_platform = platform_name or ("Linux" if Path("/proc").is_dir() else "Darwin")
    if resolved_platform != "Linux":
        return CheckResult(
            "daily_receipt_authority",
            "skip",
            "非 Linux，跳过 root signer runtime 校验",
        )
    should_probe_identity = (
        root == DAILY_RECEIPT_AUTHORITY_ROOT if probe_identity is None else probe_identity
    )

    class UnsafeDailyAuthorityError(RuntimeError):
        pass

    descriptors: list[int] = []

    def unsafe(detail: str) -> CheckResult:
        return CheckResult("daily_receipt_authority", "fail", detail)

    def open_directory(
        parent_fd: int,
        name: str,
        *,
        label: str,
    ) -> int:
        try:
            named = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            descriptor = os.open(
                name,
                os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=parent_fd,
            )
        except OSError as exc:
            raise UnsafeDailyAuthorityError(
                f"Daily root signer {label} ancestor 不安全或不存在"
            ) from exc
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(named.st_mode)
            or stat.S_ISLNK(named.st_mode)
            or named.st_uid != expected_uid
            or named.st_mode & 0o022
            or (named.st_dev, named.st_ino) != (opened.st_dev, opened.st_ino)
        ):
            os.close(descriptor)
            raise UnsafeDailyAuthorityError(f"Daily root signer {label} ancestor 不安全")
        descriptors.append(descriptor)
        return descriptor

    def read_regular_child(
        parent_fd: int,
        name: str,
        *,
        mode: int,
        label: str,
    ) -> bytes:
        try:
            named = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            descriptor = os.open(
                name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=parent_fd,
            )
        except OSError as exc:
            raise UnsafeDailyAuthorityError(f"Daily root signer {label} 不存在") from exc
        try:
            opened = os.fstat(descriptor)
            if (
                not stat.S_ISREG(named.st_mode)
                or stat.S_ISLNK(named.st_mode)
                or named.st_uid != expected_uid
                or stat.S_IMODE(named.st_mode) != mode
                or named.st_nlink != 1
                or (named.st_dev, named.st_ino) != (opened.st_dev, opened.st_ino)
            ):
                raise UnsafeDailyAuthorityError(f"Daily root signer {label} 不安全")
            chunks: list[bytes] = []
            while chunk := os.read(descriptor, 1024 * 1024):
                chunks.append(chunk)
            after = os.fstat(descriptor)
            if (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns) != (
                after.st_dev,
                after.st_ino,
                after.st_size,
                after.st_mtime_ns,
            ):
                raise UnsafeDailyAuthorityError(f"Daily root signer {label} 读取期间发生变化")
            return b"".join(chunks)
        finally:
            os.close(descriptor)

    suffix = ("usr", "local", "libexec", "rquant-daily-receipt-authority")
    if tuple(root.parts[-len(suffix) :]) != suffix:
        return unsafe("Daily root signer authority root 路径不安全")
    is_production_root = root == DAILY_RECEIPT_AUTHORITY_ROOT
    anchor = Path("/") if is_production_root else root.parents[len(suffix) - 1]
    try:
        anchor_fd = os.open(
            anchor,
            os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0),
        )
        descriptors.append(anchor_fd)
        if is_production_root:
            observed_anchor = os.fstat(anchor_fd)
            if (
                not stat.S_ISDIR(observed_anchor.st_mode)
                or observed_anchor.st_uid != expected_uid
                or observed_anchor.st_mode & 0o022
            ):
                raise UnsafeDailyAuthorityError("Daily root signer / ancestor 不安全")

        authority_fd = anchor_fd
        for component in suffix:
            authority_fd = open_directory(
                authority_fd,
                component,
                label=component,
            )
        releases_fd = open_directory(authority_fd, "releases", label="releases")

        try:
            current_before = os.stat("current", dir_fd=authority_fd, follow_symlinks=False)
            target = os.readlink("current", dir_fd=authority_fd)
            current_after = os.stat("current", dir_fd=authority_fd, follow_symlinks=False)
        except OSError as exc:
            raise UnsafeDailyAuthorityError("Daily root signer current 指针不可读") from exc
        if not stat.S_ISLNK(current_before.st_mode) or (
            current_before.st_dev,
            current_before.st_ino,
        ) != (current_after.st_dev, current_after.st_ino):
            raise UnsafeDailyAuthorityError("Daily root signer current 指针读取期间发生变化")
        if not target.startswith("releases/") or target.count("/") != 1:
            raise UnsafeDailyAuthorityError("Daily root signer current 指针不安全")
        release_sha = target.removeprefix("releases/")
        if _DAILY_AUTHORITY_RELEASE_SHA.fullmatch(release_sha) is None:
            raise UnsafeDailyAuthorityError("Daily root signer release SHA 非法")

        try:
            links = tuple(
                sorted(
                    entry
                    for entry in os.listdir(authority_fd)
                    if stat.S_ISLNK(
                        os.stat(entry, dir_fd=authority_fd, follow_symlinks=False).st_mode
                    )
                )
            )
        except OSError as exc:
            raise UnsafeDailyAuthorityError("Daily root signer authority root 不可枚举") from exc
        if links != ("current",):
            raise UnsafeDailyAuthorityError("Daily root signer authority 必须只有 current 一个链接")

        release_fd = open_directory(releases_fd, release_sha, label="release")
        source_hash_bytes = read_regular_child(
            release_fd,
            "source.sha256",
            mode=0o444,
            label="source.sha256",
        )
        artifact_bytes = read_regular_child(
            release_fd,
            "authority.pyz",
            mode=0o555,
            label="authority.pyz",
        )
        if source_hash_bytes != f"{release_sha}\n".encode("ascii"):
            raise UnsafeDailyAuthorityError("Daily root signer release SHA 未绑定 source hash")
        try:
            with zipfile.ZipFile(BytesIO(artifact_bytes)) as bundle:
                if bundle.namelist() != ["__main__.py"]:
                    raise UnsafeDailyAuthorityError("Daily root signer zipapp 内容不符合最小运行时")
                source_bytes = bundle.read("__main__.py")
            source = source_bytes.decode("utf-8")
        except (UnicodeDecodeError, zipfile.BadZipFile) as exc:
            raise UnsafeDailyAuthorityError("Daily root signer zipapp 不可验证") from exc
        if sha256(source_bytes).hexdigest() != release_sha:
            raise UnsafeDailyAuthorityError("Daily root signer release SHA 与 zipapp 源码不匹配")
        if any(
            token in source for token in ("import rquant", "pydantic", "/home/lighthouse/rquant")
        ):
            raise UnsafeDailyAuthorityError("Daily root signer zipapp 依赖可写 checkout 或 venv")
        if should_probe_identity:
            try:
                identity = probe_daily_receipt_authority_identity(
                    keyring_path=trusted_keyring_path,
                    socket_path=identity_socket_path,
                    timeout_seconds=identity_timeout_seconds,
                    max_attempts=1,
                )
            except Exception as exc:
                return unsafe(
                    f"Daily root signer identity probe 失败: {type(exc).__name__}: {str(exc)[:160]}"
                )
            if identity.source_sha256 != release_sha:
                return unsafe(
                    "Daily root signer runtime identity mismatch: "
                    f"current={identity.source_sha256} selected={release_sha}"
                )
            return CheckResult(
                "daily_receipt_authority",
                "ok",
                (
                    "root-owned stdlib zipapp release="
                    f"{release_sha}, running={identity.source_sha256}, key={identity.key_id}"
                ),
            )
        return CheckResult(
            "daily_receipt_authority",
            "ok",
            f"root-owned stdlib zipapp release={release_sha} (identity probe skipped)",
        )
    except UnsafeDailyAuthorityError as exc:
        return unsafe(str(exc))
    except OSError:
        return unsafe("Daily root signer authority root 不安全或不存在")
    finally:
        for descriptor in reversed(descriptors):
            with suppress(OSError):
                os.close(descriptor)


# ---------- 1. systemd unit 文件验证 ----------


def verify_unit_files(systemd_dir: Path) -> CheckResult:
    """对 deploy/systemd/*.{service,timer,socket} 跑 systemd-analyze verify。"""
    if not shutil.which("systemd-analyze"):
        return CheckResult(
            "unit_files",
            "skip",
            "无 systemd-analyze（mac 本地）",
        )
    if not systemd_dir.exists():
        return CheckResult(
            "unit_files",
            "fail",
            f"systemd 目录不存在: {systemd_dir}",
        )
    units = sorted(
        list(systemd_dir.glob("*.service"))
        + list(systemd_dir.glob("*.timer"))
        + list(systemd_dir.glob("*.socket"))
    )
    if not units:
        return CheckResult("unit_files", "warn", f"{systemd_dir} 下无 unit 文件")

    failed: list[str] = []
    details: list[str] = []
    for unit in units:
        try:
            r = subprocess.run(
                ["systemd-analyze", "verify", str(unit)],
                capture_output=True,
                text=True,
                timeout=10,
            )
        except subprocess.TimeoutExpired:
            failed.append(f"{unit.name}: timeout")
            continue
        # systemd-analyze verify 退出码 != 0 才是真错；stderr 可能含其他系统 unit
        # 的 warning（如 tat_agent.service 的 PIDFile legacy 提示），不该归到我们头上。
        # 但若 stderr 里出现「我们这个 unit 名」的具体错误行，也算 fail。
        own_errors = [ln for ln in r.stderr.splitlines() if unit.name in ln and ":" in ln]
        if r.returncode == 0 and not own_errors:
            details.append(f"  ✓ {unit.name}")
        else:
            msg = "; ".join(own_errors)[:160] if own_errors else f"exit={r.returncode}"
            failed.append(f"{unit.name}: {msg}")
            details.append(f"  ✗ {unit.name}")

    if failed:
        return CheckResult(
            "unit_files",
            "fail",
            f"{len(failed)}/{len(units)} unit 验证失败",
            details + [""] + [f"  失败详情: {f}" for f in failed],
        )
    return CheckResult(
        "unit_files",
        "ok",
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
                    "systemctl",
                    "show",
                    unit,
                    "--property=ActiveState,SubState,NRestarts,ExecMainStartTimestamp,Result",
                ],
                capture_output=True,
                text=True,
                timeout=5,
            )
        except subprocess.TimeoutExpired:
            details.append(f"  ⏱ {unit}: systemctl 超时")
            continue
        props = dict(line.split("=", 1) for line in r.stdout.strip().split("\n") if "=" in line)
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
            "systemd_state",
            "fail",
            f"{len(units)} 个 unit 中有 failed 状态",
            details,
        )
    return CheckResult(
        "systemd_state",
        "ok",
        f"{len(units)} 个 unit 全部 active/正常 inactive",
        details,
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
            inspection.summary,
            [f"  inventory unit: {unit}" for unit in inspection.inventory_units]
            + [f"  advisory unit: {unit}" for unit in inspection.watchlist_quote_units],
        ),
    )


# ---------- 3. DuckDB 锁布局详情（pre-market-check 简版的扩展） ----------


def detail_duckdb_lock(path: Path) -> CheckResult:
    """lsof 看 DuckDB 文件，输出每个持有者的 PID + COMMAND + FD 模式（u/r/w）。"""
    if not shutil.which("lsof"):
        return CheckResult("duckdb_lock_detail", "skip", "lsof 不可用")
    if not path.exists():
        return CheckResult(
            "duckdb_lock_detail",
            "warn",
            f"DuckDB 文件不存在: {path}",
        )
    try:
        r = subprocess.run(
            ["lsof", str(path)],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except subprocess.TimeoutExpired:
        return CheckResult("duckdb_lock_detail", "fail", "lsof 5s 超时")

    if r.returncode not in (0, 1):
        return CheckResult(
            "duckdb_lock_detail",
            "fail",
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
        for desc in other[:5]:
            details.append(f"    {desc}")
        if len(other) > 5:
            details.append(f"    ... 还有 {len(other) - 5} 个")

    if len(rw_holders) > 1:
        return CheckResult(
            "duckdb_lock_detail",
            "fail",
            f"{len(rw_holders)} 个写锁持有者 — 监控启动会撞锁（5/6 incident 重现）",
            details,
        )
    if len(rw_holders) == 1:
        return CheckResult(
            "duckdb_lock_detail",
            "ok",
            f"单写锁正常 + {len(ro_holders)} 个读锁",
            details,
        )
    if other:
        return CheckResult(
            "duckdb_lock_detail",
            "warn",
            f"未识别到可分类写锁，但有 {len(other)} 个其他 FD；不能判断 monitor 未运行",
            details,
        )
    return CheckResult(
        "duckdb_lock_detail",
        "ok",
        f"无写锁（monitor 当前未跑），{len(ro_holders)} 个读锁",
        details,
    )


# ---------- 4. 数据新鲜度 ----------


def _normalize_watermark_datetime(value: object) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return parsed.replace(tzinfo=EXCHANGE_TIMEZONE)
    return parsed.astimezone(EXCHANGE_TIMEZONE)


def _normalize_watermark_date(value: object) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _latest_expected_session(
    store: object,
    contract: DatasetContract,
    as_of: datetime,
) -> date:
    anchor = store.latest_trading_day(as_of.date())
    if contract.visibility is VisibilityRule.PANEL_CLOSE_NEXT_SESSION and anchor == as_of.date():
        return store.previous_trading_day(as_of.date())
    if contract.visibility is VisibilityRule.AUCTION_0925 and anchor == as_of.date():
        source_cutoff = min(item.available_at for item in contract.source_availability)
        if as_of.timetz().replace(tzinfo=None) < source_cutoff:
            return store.previous_trading_day(as_of.date())
    return anchor


def _trading_session_lag(
    store: object,
    watermark: date,
    expected: date,
) -> int:
    if not store.is_trading_day("SSE", watermark):
        raise ValueError(f"watermark {watermark.isoformat()} 落在非交易日")
    if watermark > expected:
        raise ValueError(f"watermark {watermark.isoformat()} 晚于可见交易日 {expected.isoformat()}")
    if watermark == expected:
        return 0
    rows = store.list_trade_calendar("SSE", watermark + timedelta(days=1), expected)
    return sum(1 for row in rows if row.is_open)


def _expected_intraday_time(store: object, as_of: datetime) -> datetime:
    local_time = as_of.timetz().replace(tzinfo=None)
    if not store.is_trading_day("SSE", as_of.date()) or local_time < time(9, 30):
        session = store.latest_trading_day(as_of.date())
        if session == as_of.date():
            session = store.previous_trading_day(as_of.date())
        return datetime.combine(session, time(15), tzinfo=EXCHANGE_TIMEZONE)
    if local_time <= time(11, 30):
        return as_of
    if local_time < time(13):
        return datetime.combine(as_of.date(), time(11, 30), tzinfo=EXCHANGE_TIMEZONE)
    if local_time <= time(15):
        return as_of
    return datetime.combine(as_of.date(), time(15), tzinfo=EXCHANGE_TIMEZONE)


def _replica_freshness_detail(
    replica_path: Path | None,
    *,
    primary_path: Path | None,
    as_of: datetime,
    max_source_lag: timedelta,
) -> tuple[str | None, bool]:
    if replica_path is None:
        return None, False
    if not replica_path.exists():
        return f"  ⚠ 只读副本不存在: {replica_path}", True
    replica_modified = datetime.fromtimestamp(replica_path.stat().st_mtime, tz=EXCHANGE_TIMEZONE)
    wall_age = max(as_of - replica_modified, timedelta(0))
    wall_age_minutes = int(wall_age.total_seconds() // 60)
    source_paths = (
        ()
        if primary_path is None
        else (
            primary_path,
            primary_path.with_name(primary_path.name + ".wal"),
        )
    )
    source_mtimes = [path.stat().st_mtime for path in source_paths if path.exists()]
    if not source_mtimes:
        return f"  ✓ 只读副本文件年龄 {wall_age_minutes} 分钟（主库工件不可比较）", False
    source_modified = datetime.fromtimestamp(max(source_mtimes), tz=EXCHANGE_TIMEZONE)
    source_lag = max(source_modified - replica_modified, timedelta(0))
    source_lag_minutes = int(source_lag.total_seconds() // 60)
    threshold_minutes = int(max_source_lag.total_seconds() // 60)
    if source_lag > max_source_lag:
        return (
            f"  ⚠ 只读副本落后主库工件 {source_lag_minutes} 分钟"
            f"（阈值 {threshold_minutes} 分钟；文件年龄 {wall_age_minutes} 分钟）",
            True,
        )
    return (
        f"  ✓ 只读副本落后主库工件 {source_lag_minutes} 分钟（文件年龄 {wall_age_minutes} 分钟）",
        False,
    )


def check_data_freshness(
    contracts: Sequence[DatasetContract] | None = None,
    *,
    profile: FreshnessProfile = "production",
    as_of: datetime | None = None,
    replica_path: Path | None = settings.duckdb_readonly_path_resolved,
    primary_path: Path | None = settings.duckdb_path,
    replica_max_source_lag: timedelta = READONLY_REPLICA_MAX_SOURCE_LAG,
) -> CheckResult:
    """按数据契约检查交易日/分钟水位、空表和只读副本年龄。"""
    profile_ids = (
        PRODUCTION_FRESHNESS_DATASET_IDS
        if profile == "production"
        else RESEARCH_FRESHNESS_DATASET_IDS
    )
    selected = tuple(contracts or (CONTRACTS_BY_ID[item] for item in profile_ids))
    local_as_of = as_of or datetime.now(EXCHANGE_TIMEZONE)
    if local_as_of.tzinfo is None or local_as_of.utcoffset() is None:
        raise ValueError("as_of must be timezone-aware")
    local_as_of = local_as_of.astimezone(EXCHANGE_TIMEZONE)
    try:
        from rquant.storage.duckdb import open_readonly_store

        store = open_readonly_store()
    except Exception as e:
        return CheckResult(
            "data_freshness",
            "fail",
            f"只读副本/主库打开失败: {type(e).__name__}: {e}",
        )

    details: list[str] = []
    has_warn = False
    has_fail = False

    replica_detail, replica_warn = _replica_freshness_detail(
        replica_path,
        primary_path=primary_path,
        as_of=local_as_of,
        max_source_lag=replica_max_source_lag,
    )
    if replica_detail is not None:
        details.append(replica_detail)
        has_fail = replica_warn

    with store:
        for contract in selected:
            table = contract.table_name
            watermark_column = contract.freshness.watermark_column
            try:
                if contract.visibility is VisibilityRule.PANEL_CLOSE_NEXT_SESSION:
                    event_date_column = contract.event_date_column
                    assert event_date_column is not None
                    row = store._conn.execute(
                        f"SELECT MAX({watermark_column}), COUNT(*) FROM {table} "
                        f"WHERE {event_date_column} < ?",
                        [local_as_of.date()],
                    ).fetchone()
                else:
                    row = store._conn.execute(
                        f"SELECT MAX({watermark_column}), COUNT(*) FROM {table}"
                    ).fetchone()
            except Exception as e:
                details.append(
                    f"  ✗ {contract.dataset_id}/{table}: 查询失败 {type(e).__name__}: {e}"
                )
                has_fail = True
                continue
            latest, total = row
            if latest is None:
                if contract.freshness.required_on_open_day:
                    details.append(f"  ✗ {contract.dataset_id}/{table}: 必需数据为空")
                    has_fail = True
                else:
                    details.append(f"  ⚠ {contract.dataset_id}/{table}: 可选数据为空")
                    has_warn = True
                continue

            rule = contract.freshness
            if rule.event_driven:
                details.append(
                    f"  — {contract.dataset_id}/{table}: latest={latest}, total={total:,} 行；"
                    "事件驱动数据不声明固定时效阈值"
                )
                continue
            if not rule.has_known_lag:
                details.append(
                    f"  ⚠ {contract.dataset_id}/{table}: latest={latest}, total={total:,} 行；"
                    "契约未声明时效阈值"
                )
                has_warn = True
                continue

            try:
                if rule.max_trading_session_lag is not None:
                    watermark_date = _normalize_watermark_date(latest)
                    expected = _latest_expected_session(store, contract, local_as_of)
                    lag = _trading_session_lag(store, watermark_date, expected)
                    threshold = rule.max_trading_session_lag
                    lag_text = f"{lag} 个交易日"
                    threshold_text = f"{threshold} 个交易日"
                else:
                    assert rule.max_wall_clock_lag is not None
                    watermark_time = _normalize_watermark_datetime(latest)
                    if (
                        time(9, 30) <= local_as_of.time() < time(9, 35)
                        and watermark_time.date() < local_as_of.date()
                    ):
                        expected_time = datetime.combine(
                            store.previous_trading_day(local_as_of.date()),
                            time(15),
                            tzinfo=EXCHANGE_TIMEZONE,
                        )
                    else:
                        expected_time = _expected_intraday_time(store, local_as_of)
                    if watermark_time > expected_time:
                        raise ValueError(
                            f"watermark {watermark_time.isoformat()} 晚于应有时点 "
                            f"{expected_time.isoformat()}"
                        )
                    lag_delta = expected_time - watermark_time
                    lag = int(lag_delta.total_seconds() // 60)
                    threshold = int(rule.max_wall_clock_lag.total_seconds() // 60)
                    lag_text = f"{lag} 分钟"
                    threshold_text = f"{threshold} 分钟"
            except Exception as e:
                details.append(
                    f"  ✗ {contract.dataset_id}/{table}: freshness 无法判定 {type(e).__name__}: {e}"
                )
                has_fail = True
                continue

            line = (
                f"{contract.dataset_id}/{table}: latest={latest}, {lag_text}，"
                f"阈值 {threshold_text}，total={total:,} 行"
            )
            if lag > threshold:
                if rule.required_on_open_day:
                    details.append(f"  ✗ {line}")
                    has_fail = True
                else:
                    details.append(f"  ⚠ {line}")
                    has_warn = True
            else:
                details.append(f"  ✓ {line}")

    status = "fail" if has_fail else ("warn" if has_warn else "ok")
    n = len(selected)
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
            "smoke_screen",
            "fail",
            f"import 失败: {type(e).__name__}: {e}",
        )

    if not PRESET_SCREENS:
        return CheckResult(
            "smoke_screen",
            "warn",
            "PRESET_SCREENS 为空",
        )

    # 选一个 baseline preset（有就用 n-shape-pool1，否则取第一个）
    name = "n-shape-pool1" if "n-shape-pool1" in PRESET_SCREENS else next(iter(PRESET_SCREENS))
    preset = PRESET_SCREENS[name]

    try:
        from rquant.storage.duckdb import open_readonly_store

        store = open_readonly_store(required_tables=SMOKE_SCREEN_TABLES)
    except Exception as e:
        return CheckResult(
            "smoke_screen",
            "fail",
            f"只读副本/主库打开失败: {type(e).__name__}: {e}",
        )

    with store:
        try:
            latest_row = store._conn.execute(
                "SELECT MAX(trade_date) FROM screen_result WHERE preset_name = ?",
                [name],
            ).fetchone()
        except Exception as e:
            return CheckResult(
                "smoke_screen",
                "fail",
                f"DuckDB 探查失败: {type(e).__name__}: {e}",
            )

        latest = latest_row[0] if latest_row else None
        if latest is None:
            return CheckResult(
                "smoke_screen",
                "warn",
                f"preset {name} 在 screen_result 中无历史数据，跳过 smoke",
            )

        trade_date_str = latest if isinstance(latest, str) else latest.isoformat()

        try:
            start = datetime.now()
            df = screen(
                trade_date_str,
                preset.rules,
                include_columns=preset.include_columns or None,
                store=store,
            )
            elapsed = (datetime.now() - start).total_seconds()
        except Exception as e:
            return CheckResult(
                "smoke_screen",
                "fail",
                f"screen() 抛异常: {type(e).__name__}: {e}",
            )

    summary = f"preset={name} trade_date={trade_date_str} hits={len(df)} 用时 {elapsed:.2f}s"
    if elapsed > 30:
        return CheckResult("smoke_screen", "warn", summary + "（>30s 偏慢）")
    return CheckResult("smoke_screen", "ok", summary)


def verify_resource_authority_services(
    external_root_config_path: Path | None,
    resource_authority_config_path: Path | None,
    *,
    required: bool = False,
    systemd_dir: Path = Path("/etc/systemd/system"),
    external_environment_path: Path = Path("/etc/rquant/external-root.env"),
    resource_environment_path: Path = Path("/etc/rquant/resource-authority.env"),
    authority_runtime_root: Path = Path("/usr/local/libexec/rquant-authority-runtime"),
    authority_runtime_public_key_path: Path = Path(
        "/etc/rquant/keys/authority-runtime/runtime.public.pem"
    ),
    authority_expected_uid: int = 0,
    authority_expected_gid: int = 0,
) -> CheckResult:
    """Probe both closed Unix authorities and their cross-file trust binding."""
    if external_root_config_path is None and resource_authority_config_path is None:
        return CheckResult(
            "resource_authority_services",
            "fail" if required else "skip",
            (
                "production V2 worker 缺少 external root/resource authority 服务配置"
                if required
                else "external root/resource authority 尚未配置"
            ),
        )
    if external_root_config_path is None or resource_authority_config_path is None:
        return CheckResult(
            "resource_authority_services",
            "fail",
            "external root 与 resource authority 配置必须成对提供",
        )
    try:
        from rquant.external_monotonic_root_service import (
            ClosedExternalMonotonicRootVerifier,
            probe_external_monotonic_root_service,
        )
        from rquant.resource_authority_service import (
            EXTERNAL_ROOT_ENVIRONMENT_KEYS,
            RESOURCE_AUTHORITY_ENVIRONMENT_KEYS,
            load_closed_authority_environment,
            load_external_monotonic_root_daemon_configuration,
            load_resource_authority_daemon_configuration,
            probe_resource_authority_service,
            verify_authority_os_isolation,
        )

        deployment_details: tuple[str, ...] = ()
        if required:
            from rquant.authority_runtime_release import (
                AUTHORITY_RUNTIME_PUBLISHER_SHA256,
                AUTHORITY_RUNTIME_PUBLISHER_VERSION,
                verify_authority_runtime_release,
            )

            external_environment = load_closed_authority_environment(
                external_environment_path,
                allowed_keys=EXTERNAL_ROOT_ENVIRONMENT_KEYS,
                required_keys=EXTERNAL_ROOT_ENVIRONMENT_KEYS,
                expected_uid=authority_expected_uid,
                expected_gid=authority_expected_gid,
            )
            resource_environment = load_closed_authority_environment(
                resource_environment_path,
                allowed_keys=RESOURCE_AUTHORITY_ENVIRONMENT_KEYS,
                required_keys=RESOURCE_AUTHORITY_ENVIRONMENT_KEYS,
                expected_uid=authority_expected_uid,
                expected_gid=authority_expected_gid,
            )
            expected_units = {
                "rquant-external-monotonic-root.service": (
                    external_environment_path,
                    "external-monotonic-root-serve",
                ),
                "rquant-resource-authority.service": (
                    resource_environment_path,
                    "resource-authority-serve",
                ),
            }
            runtime_executable = (
                "/usr/local/libexec/rquant-authority-runtime/current/venv/bin/rquant"
            )
            for unit_name, (environment_path, command) in expected_units.items():
                unit = (systemd_dir / unit_name).read_text(encoding="utf-8")
                environment_lines = tuple(
                    line for line in unit.splitlines() if line.startswith("EnvironmentFile=")
                )
                exec_lines = tuple(
                    line for line in unit.splitlines() if line.startswith("ExecStart=")
                )
                if environment_lines != (f"EnvironmentFile={environment_path}",) or (
                    len(exec_lines) != 1
                    or not exec_lines[0].startswith(f"ExecStart={runtime_executable} {command} ")
                    or "/home/lighthouse/rquant" in unit
                ):
                    raise ValueError("authority unit runtime or EnvironmentFile is not closed")
            if (
                external_environment["RQUANT_EXTERNAL_MONOTONIC_ROOT_SERVICE_CONFIG_PATH"]
                != os.fspath(external_root_config_path)
                or resource_environment["RQUANT_RESOURCE_AUTHORITY_SERVICE_CONFIG_PATH"]
                != os.fspath(resource_authority_config_path)
                or resource_environment["APP_ENV"] != "prod"
                or external_environment["APP_ENV"] != "prod"
                or resource_environment["RQUANT_RESOURCE_AUTHORITY_STATE_DIR"]
                != "/var/lib/rquant-resource-authority"
            ):
                raise ValueError("authority EnvironmentFile conflicts with production paths")
            verified_runtime = verify_authority_runtime_release(
                root=authority_runtime_root,
                signing_public_key_path=authority_runtime_public_key_path,
                expected_release_sha=resource_environment["RQUANT_CODE_COMMIT"],
                expected_uid=authority_expected_uid,
                expected_gid=authority_expected_gid,
                signing_key_uid=authority_expected_uid,
                signing_key_gid=authority_expected_gid,
                expected_publisher_sha256=AUTHORITY_RUNTIME_PUBLISHER_SHA256,
                expected_publisher_version=AUTHORITY_RUNTIME_PUBLISHER_VERSION,
            )
            deployment_details = (
                f"runtime release={verified_runtime.release_sha}",
                f"runtime publisher={verified_runtime.publisher_version}",
                f"runtime files={verified_runtime.file_count}",
                "authority EnvironmentFile registries are exact",
            )

        root_daemon = load_external_monotonic_root_daemon_configuration(
            external_root_config_path,
            expected_uid=authority_expected_uid if required else None,
            expected_gid=authority_expected_gid if required else None,
        )
        resource_daemon = load_resource_authority_daemon_configuration(
            resource_authority_config_path,
            expected_uid=authority_expected_uid if required else None,
            expected_gid=authority_expected_gid if required else None,
        )
        resource_service = resource_daemon.service_configuration
        manifest = resource_service.external_root_manifest
        root_service = root_daemon.service_configuration
        adapter = resource_service.adapter_configuration
        root_binding = adapter.external_root_config
        if root_binding is None:
            raise ValueError("resource authority external root binding is missing")
        if (
            (
                root_service.socket_path,
                root_service.socket_uid,
                root_service.socket_gid,
                root_service.socket_mode,
                root_service.service_uid,
                root_service.service_gid,
                root_service.role,
                root_service.authority_id,
                root_service.store_id,
                root_service.rollback_domain_id,
                root_service.transport_manifest_hash,
            )
            != (
                manifest.socket_path,
                manifest.socket_uid,
                manifest.socket_gid,
                manifest.socket_mode,
                manifest.peer_uid,
                manifest.peer_gid,
                manifest.role,
                manifest.authority_id,
                manifest.store_id,
                manifest.rollback_domain_id,
                manifest.manifest_hash,
            )
            or (
                root_service.allowed_peer_uid,
                root_service.allowed_peer_gid,
            )
            != adapter.expected_server_identity
            or (root_daemon.high_water_authority_id != adapter.high_water_authority_id)
        ):
            raise ValueError("external root/resource authority service manifests conflict")
        authority_store_paths = {
            root_daemon.backend_path.resolve(strict=False),
            resource_service.resource_journal_path.resolve(strict=False),
            resource_service.high_water_cache_path.resolve(strict=False),
        }
        if len(authority_store_paths) != 3:
            raise ValueError("authority durable stores cannot self-attest in one file")
        isolation_details = verify_authority_os_isolation(root_daemon, resource_daemon)
        if type(isolation_details) is not tuple or isolation_details != (
            "external-root uid/gid and private state isolated",
            "resource-authority uid/gid and private state isolated",
            "lighthouse restricted to resource socket client group",
        ):
            raise ValueError("authority OS isolation evidence is invalid")
        verifier = ClosedExternalMonotonicRootVerifier(
            public_key_path=resource_daemon.root_public_key_path,
            issuer=root_binding.root_issuer,
            key_id=root_binding.root_key_id,
            key_purpose=root_binding.root_key_purpose,
        )
        root_probe = probe_external_monotonic_root_service(
            manifest,
            verifier=verifier,
            expected_transport_manifest_hash=root_binding.transport_manifest_hash,
        )
        resource_probe = probe_resource_authority_service(adapter)
        if root_probe.capabilities != ("current", "pin", "advance") or (
            resource_probe.capabilities != ("policy", "snapshot", "journal")
        ):
            raise ValueError("authority capability registry changed")
    except Exception as exc:
        return CheckResult(
            "resource_authority_services",
            "fail",
            f"closed authority probe failed: {type(exc).__name__}",
        )
    return CheckResult(
        "resource_authority_services",
        "ok",
        "external root/resource journal socket owner、peer、identity、capability 全部匹配",
        [
            f"  ✓ root={root_probe.authority_id}/{root_probe.store_id}",
            f"  ✓ resource={resource_probe.identity.authority_id}",
            *[f"  ✓ {detail}" for detail in deployment_details],
            *[f"  ✓ {detail}" for detail in isolation_details],
        ],
    )


# ---------- 聚合 + 输出 ----------


def run_all_checks(
    systemd_dir: Path | None = None,
    *,
    freshness_profile: FreshnessProfile = "production",
    recovery_config: RuntimeRecoveryPreflightConfig | None = None,
    runtime_root: Path | None = None,
    workload_runtime_config: WorkloadRuntimeProbeConfig | None = None,
) -> list[CheckResult]:
    """跑全部体检。systemd_dir 默认从项目根推断。"""
    project_root = Path(__file__).resolve().parents[2]
    systemd_dir = systemd_dir or (project_root / "deploy" / "systemd")
    resolved_runtime_root = runtime_root or Path(
        os.environ.get("RQUANT_RUNTIME_ROOT", str(settings.data_dir / "runtime"))
    )
    runtime_inspection, runtime_result = runtime_deployment_service_checks(resolved_runtime_root)
    workload_advisories = (
        ReceiptBoundWorkloadAdvisories(
            generation_hash=runtime_inspection.generation_hash,
            units=runtime_inspection.watchlist_quote_units,
            health_status=runtime_inspection.status,
            health_summary=runtime_inspection.summary,
        )
        if runtime_inspection.generation_hash is not None
        and runtime_inspection.watchlist_quote_units
        else None
    )
    runtime_probe_config = workload_runtime_config or WorkloadRuntimeProbeConfig()
    systemd_units = list(
        dict.fromkeys((*SERVICES_TO_CHECK, *runtime_inspection.strict_authority_units))
    )

    results: list[CheckResult] = []
    results.append(verify_runtime_dependencies())
    results.append(verify_daily_receipt_authority_runtime())
    results.append(verify_unit_files(systemd_dir))
    results.append(detail_systemd_state(systemd_units))
    results.append(runtime_result)
    quota_profile = "production" if freshness_profile == "production" else "candidate"
    for source, relative_path in (
        ("tushare.stk_auction", Path("live/auction-match/quota.sqlite3")),
        ("tushare.rt_min", Path("live/market-minute/quota.sqlite3")),
        ("tushare.reference_slow", Path("live/reference-slow/quota.sqlite3")),
        ("tushare.daily_close", Path("live/daily-close/quota.sqlite3")),
    ):
        results.append(
            verify_source_quota_ledger(
                resolved_runtime_root / relative_path,
                source=source,
                profile=quota_profile,
            )
        )
    results.append(detail_duckdb_lock(settings.duckdb_path))
    results.append(check_data_freshness(profile=freshness_profile))
    results.append(smoke_screen())
    results.append(
        verify_resource_authority_services(
            settings.rquant_external_monotonic_root_service_config_path,
            settings.rquant_resource_authority_service_config_path,
            required=(
                settings.app_env == "prod"
                and bool(settings.rquant_lab_resource_authority_config_json.strip())
            ),
        )
    )
    if recovery_config is not None:
        results.append(verify_runtime_recovery(recovery_config))
    results.append(_workload_check_result(verify_workload_unit_declarations(systemd_dir)))
    results.append(
        _workload_check_result(
            _safe_workload_probe(
                "workload_runtime",
                lambda: check_workload_runtime(
                    cgroup_root=runtime_probe_config.cgroup_root,
                    platform_name=runtime_probe_config.platform_name,
                    systemctl_path=runtime_probe_config.systemctl_path,
                    strict=runtime_probe_config.strict,
                    advisory_units=workload_advisories,
                    arbiter_path=runtime_probe_config.arbiter_path,
                    arbiter_hash_path=runtime_probe_config.arbiter_hash_path,
                    arbiter_expected_uid=runtime_probe_config.arbiter_expected_uid,
                ),
            )
        )
    )
    results.append(
        _workload_check_result(
            _safe_workload_probe("workload_capacity", check_workload_capacity_baseline)
        )
    )
    results.append(
        _workload_check_result(
            _safe_workload_probe("workload_high_water", check_workload_high_water_evidence)
        )
    )
    return results


_ICON = {"ok": "✓", "warn": "⚠️", "fail": "❌", "skip": "—"}


def format_report(results: list[CheckResult]) -> str:
    """Markdown 报告（多行，stdout 友好）。"""
    fails = sum(1 for r in results if r.status == "fail")
    warns = sum(1 for r in results if r.status == "warn")
    oks = sum(1 for r in results if r.status == "ok")
    skips = sum(1 for r in results if r.status == "skip")

    if fails == 0 and warns == 0 and skips == 0:
        header = "# [RQ] ✅ Preflight 全部通过"
    elif fails == 0 and warns == 0:
        header = f"# [RQ] ⚠️ Preflight: {skips} 项未验证"
    elif fails == 0:
        header = f"# [RQ] ⚠️ Preflight: {warns} 项预警 + {skips} 项未验证"
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
    skips = [r for r in results if r.status == "skip"]
    issues = len(fails) + len(warns)

    if issues == 0 and not skips:
        subject = "[RQ] ✅ Preflight 通过"
    elif issues == 0:
        subject = f"[RQ] ⚠️ Preflight: {len(skips)} 项未验证"
    elif fails:
        subject = f"[RQ] ❌ Preflight: {len(fails)} 失败 + {len(warns)} 预警 + {len(skips)} 未验证"
    else:
        subject = f"[RQ] ⚠️ Preflight: {len(warns)} 项预警 + {len(skips)} 未验证"

    lines = [f"{_ICON[r.status]} {r.name}: {r.summary}" for r in results]
    body = "\n".join(lines)
    return subject, body
