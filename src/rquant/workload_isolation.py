"""Read-only systemd workload-isolation contracts and cloud acceptance probes."""

from __future__ import annotations

import configparser
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Literal, Self

from pydantic import Field, model_validator

from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel

CheckStatus = Literal["ok", "warn", "fail", "skip"]
_RECEIPT_GENERATION_HASH = re.compile(r"^[0-9a-f]{64}$")
_WATCHLIST_ADVISORY_UNIT = re.compile(r"^rquant-runtime-watchlist-quote@svc-[0-9a-f]{64}\.service$")


@dataclass(frozen=True)
class WorkloadCheck:
    """One auditable workload isolation result."""

    name: str
    status: CheckStatus
    summary: str
    details: tuple[str, ...] = ()


class ReceiptBoundWorkloadAdvisories(RuntimeContractModel):
    """Receipt-derived candidate units that are observable but never authoritative."""

    generation_hash: str
    units: tuple[str, ...]
    health_status: CheckStatus
    health_summary: str

    @model_validator(mode="after")
    def validate_receipt_binding(self) -> Self:
        if _RECEIPT_GENERATION_HASH.fullmatch(self.generation_hash) is None:
            raise ValueError("advisory generation_hash must be a sha256 digest")
        if not self.units or len(set(self.units)) != len(self.units):
            raise ValueError("receipt-bound advisory units must be non-empty and unique")
        invalid = [unit for unit in self.units if _WATCHLIST_ADVISORY_UNIT.fullmatch(unit) is None]
        if invalid:
            raise ValueError(f"invalid watchlist advisory unit: {invalid[0]}")
        return self


PARENT_SLICE = "rquant.slice"
PARENT_SLICE_LIMITS: Mapping[str, str] = {
    "CPUWeight": "100",
    "IOWeight": "100",
    "MemoryLow": "3072M",
    "MemoryHigh": "6144M",
    "TasksMax": "1024",
}
WORKLOAD_SLICE_LIMITS: Mapping[str, Mapping[str, str]] = {
    "rquant-live.slice": {
        "CPUWeight": "1000",
        "IOWeight": "1000",
        "MemoryLow": "3072M",
        "MemoryHigh": "3840M",
        "TasksMax": "512",
    },
    "rquant-serving.slice": {
        "CPUWeight": "500",
        "IOWeight": "500",
        "MemoryHigh": "512M",
        "TasksMax": "256",
    },
    "rquant-research.slice": {
        "CPUWeight": "100",
        "CPUQuota": "100%",
        "IOWeight": "100",
        "MemoryHigh": "512M",
        "MemoryMax": "768M",
        "TasksMax": "128",
    },
    "rquant-maintenance.slice": {
        "CPUWeight": "50",
        "IOWeight": "50",
        "TasksMax": "128",
    },
}

_SLICE_ACCOUNTING = {
    "CPUAccounting": "true",
    "IOAccounting": "true",
    "MemoryAccounting": "true",
    "TasksAccounting": "true",
}
MAINTENANCE_UNITS = {
    "rquant-backup.service",
    "rquant-replica-sync.service",
}
ROOT_AUTHORITY_UNITS = {
    "rquant-daily-receipt-signer.service",
    "rquant-external-monotonic-root.service",
    "rquant-resource-authority.service",
}
_SYSTEM_SLICE = "system.slice"
_EXCEPTION_LIMITS: Mapping[str, Mapping[str, str]] = {
    "rquant-daily-receipt-signer.service": {
        "CPUWeight": "100",
        "IOWeight": "100",
        "MemoryHigh": "128M",
        "MemoryMax": "256M",
        "TasksMax": "32",
    },
}

WORKLOAD_ARBITER_PATH = "/usr/local/libexec/rquant-workload-arbiter"
WORKLOAD_ARBITER_HASH_PATH = f"{WORKLOAD_ARBITER_PATH}.sha256"

# Existing jobs retain their commands, timers, and data authorities. This map is
# exhaustive so a new production service cannot silently inherit the default slice.
WORKLOAD_UNIT_SLICES: Mapping[str, str] = {
    "rquant-alert@.service": "rquant-live.slice",
    "rquant-artifact-retention.service": "rquant-research.slice",
    "rquant-backup.service": "rquant-maintenance.slice",
    "rquant-canvas.service": "rquant-serving.slice",
    "rquant-daily-receipt-signer.service": _SYSTEM_SLICE,
    "rquant-daily-report.service": "rquant-live.slice",
    "rquant-daily.service": "rquant-live.slice",
    "rquant-dashboard.service": "rquant-serving.slice",
    "rquant-external-monotonic-root.service": _SYSTEM_SLICE,
    "rquant-kpl-snapshot.service": "rquant-live.slice",
    "rquant-lab-claim-finalizer.service": "rquant-research.slice",
    "rquant-midday-report.service": "rquant-live.slice",
    "rquant-monitor-watchdog.service": "rquant-live.slice",
    "rquant-monitor.service": "rquant-live.slice",
    "rquant-morning-pulse.service": "rquant-live.slice",
    "rquant-nl-screen.service": "rquant-serving.slice",
    "rquant-page-control.service": "rquant-serving.slice",
    "rquant-panorama-auth.service": "rquant-serving.slice",
    "rquant-panorama.service": "rquant-serving.slice",
    "rquant-pre-market-check.service": "rquant-live.slice",
    "rquant-replica-sync.service": "rquant-maintenance.slice",
    "rquant-research-ingest.service": "rquant-research.slice",
    "rquant-resource-authority.service": _SYSTEM_SLICE,
    "rquant-runtime-artifact-catalog@.service": "rquant-research.slice",
    "rquant-runtime-auction-match@.service": "rquant-live.slice",
    "rquant-runtime-auction-universe@.service": "rquant-live.slice",
    "rquant-runtime-candidate@.service": "rquant-live.slice",
    "rquant-runtime-daily-close@.service": "rquant-live.slice",
    "rquant-runtime-daily-orchestrator@.service": "rquant-research.slice",
    "rquant-runtime-feature@.service": "rquant-live.slice",
    "rquant-runtime-lab-jobs@.service": "rquant-research.slice",
    "rquant-runtime-market-minute@.service": "rquant-live.slice",
    "rquant-runtime-notifier@.service": "rquant-live.slice",
    "rquant-runtime-paper-broker@.service": "rquant-live.slice",
    "rquant-runtime-paper-constraint@.service": "rquant-live.slice",
    "rquant-runtime-promotions@.service": "rquant-research.slice",
    "rquant-runtime-recovery-rehearsal@.service": "rquant-research.slice",
    "rquant-runtime-recovery@.service": "rquant-research.slice",
    "rquant-runtime-reference-slow-publisher@.service": "rquant-live.slice",
    "rquant-runtime-reference-slow-source@.service": "rquant-live.slice",
    "rquant-runtime-runtime-health@.service": "rquant-serving.slice",
    "rquant-runtime-serving@.service": "rquant-serving.slice",
    "rquant-runtime-shadow@.service": "rquant-research.slice",
    "rquant-runtime-signal-router@.service": "rquant-live.slice",
    "rquant-runtime-strategy@.service": "rquant-live.slice",
    "rquant-runtime-watchlist-quote@.service": "rquant-live.slice",
    "rquant-surge-watch.service": "rquant-live.slice",
    "rquant-tushare-token-reminder.service": "rquant-live.slice",
    "rquant-workload-sample.service": "rquant-serving.slice",
}

LEGACY_RUNTIME_TEMPLATES = {
    "rquant-runtime-live@.service",
    "rquant-runtime-research@.service",
}
_REQUIRED_CGROUP_CONTROLLERS = {"cpu", "io", "memory", "pids"}
_RUNNING_STATES = {"active", "activating", "deactivating"}
_NON_SYSTEMD_ERRORS = (
    "system has not been booted with systemd",
    "failed to connect to bus",
    "no such file or directory",
)
_MIN_CPU_COUNT = 2
NOMINAL_HOST_MEMORY_BYTES = 8 * 1024**3
# Linux reports about 7.5 GiB usable RAM on the measured nominal 8 GiB host.
MIN_HOST_MEMORY_BYTES = 7680 * 1024**2
MIN_SYSTEM_RESERVE_BYTES = 1280 * 1024**2
RESEARCH_MEMORY_MAX_BYTES = 768 * 1024**2
_MIN_AVAILABLE_MEMORY_BYTES = MIN_SYSTEM_RESERVE_BYTES + RESEARCH_MEMORY_MAX_BYTES
_MIN_FREE_DISK_BYTES = 8 * 1024**3
_HIGH_WATER_MAX_AGE = timedelta(days=30)
DEFAULT_HIGH_WATER_EVIDENCE_PATH = Path("/var/lib/rquant/workload-isolation/high-water.json")
DEFAULT_HIGH_WATER_RAW_EVIDENCE_PATH = Path("/var/lib/rquant/workload-isolation/samples.jsonl")
WORKLOAD_MEMORY_BUDGET_MIB: Mapping[str, int] = {
    "nominal_host": 8192,
    "usable_host": 7680,
    "live": 3840,
    "serving": 512,
    "research": 768,
    "os": 1280,
    "monitor_observed_current": 2415,
    "monitor_observed_peak": 2814,
}
_SYSTEMCTL_PROPERTIES = (
    "LoadState,ActiveState,UnitFileState,Slice,ControlGroup,CPUWeight,IOWeight,"
    "MemoryLow,MemoryHigh,MemoryMax,TasksMax,CPUQuotaPerSecUSec,ExecStart"
)


class WorkloadHighWaterEvidence(RuntimeContractModel):
    """Audited cloud high-water observations used by the research gate."""

    schema_version: Literal[1]
    observed_at: AwareUtcDatetime
    observation_window_hours: int = Field(strict=True, ge=24, le=24 * 31)
    host_mem_total_mib: int = Field(strict=True, gt=0)
    monitor_current_mib: int = Field(strict=True, ge=0)
    monitor_peak_mib: int = Field(strict=True, gt=0)
    live_concurrent_peak_mib: int = Field(strict=True, gt=0)
    serving_concurrent_peak_mib: int = Field(strict=True, ge=0)
    backup_peak_mib: int = Field(strict=True, gt=0)
    replica_peak_mib: int = Field(strict=True, ge=0)
    maintenance_concurrent_peak_mib: int = Field(strict=True, gt=0)
    backup_successful_runs: int = Field(strict=True, gt=0)
    replica_successful_runs: int = Field(strict=True, gt=0)
    backup_sample_count: int = Field(strict=True, gt=0)
    replica_sample_count: int = Field(strict=True, gt=0)
    backup_successful_runtime_seconds: int = Field(strict=True, gt=0)
    replica_successful_runtime_seconds: int = Field(strict=True, gt=0)
    raw_evidence_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    os_system_slice_peak_mib: int = Field(strict=True, gt=0)
    min_mem_available_mib: int = Field(strict=True, gt=0)

    @model_validator(mode="after")
    def validate_peak_relationships(self) -> Self:
        if self.monitor_current_mib > self.monitor_peak_mib:
            raise ValueError("monitor current exceeds monitor peak")
        if self.monitor_peak_mib > self.live_concurrent_peak_mib:
            raise ValueError("monitor peak exceeds live concurrent peak")
        if self.maintenance_concurrent_peak_mib < max(
            self.backup_peak_mib,
            self.replica_peak_mib,
        ):
            raise ValueError("maintenance concurrent peak is below an individual peak")
        return self


def parse_systemd_bytes(value: str) -> int:
    """Parse the absolute memory syntax used by checked-in unit contracts."""

    match = re.fullmatch(r"([0-9]+)([KMGT]?)", value)
    if match is None:
        raise ValueError(f"memory value must be absolute, got {value!r}")
    amount = int(match.group(1))
    multiplier = {
        "": 1,
        "K": 1024,
        "M": 1024**2,
        "G": 1024**3,
        "T": 1024**4,
    }[match.group(2)]
    return amount * multiplier


def _parse_unit(path: Path) -> configparser.ConfigParser:
    parser = configparser.ConfigParser(interpolation=None, strict=False)
    parser.optionxform = str
    with path.open(encoding="utf-8") as stream:
        parser.read_file(stream)
    return parser


def _slice_limits_from_disk(systemd_dir: Path, slice_name: str) -> dict[str, str]:
    return dict(_parse_unit(systemd_dir / slice_name)["Slice"])


def verify_workload_memory_admission(systemd_dir: Path) -> WorkloadCheck:
    """Verify hard-cap safety while maintenance remains pending calibration."""

    errors: list[str] = []
    details: list[str] = []
    budget = WORKLOAD_MEMORY_BUDGET_MIB
    try:
        parent = _slice_limits_from_disk(systemd_dir, PARENT_SLICE)
        children = {
            name: _slice_limits_from_disk(systemd_dir, name) for name in WORKLOAD_SLICE_LIMITS
        }
        parent_low = parse_systemd_bytes(parent["MemoryLow"])
        live_low = parse_systemd_bytes(children["rquant-live.slice"]["MemoryLow"])
    except (configparser.Error, KeyError, OSError, ValueError) as exc:
        return WorkloadCheck(
            "workload_memory_admission",
            "fail",
            f"cannot parse workload memory envelope: {type(exc).__name__}",
            (f"  x {exc}",),
        )

    for slice_name, values in {PARENT_SLICE: parent, **children}.items():
        if slice_name != "rquant-research.slice" and "MemoryMax" in values:
            errors.append(f"{slice_name}: unsafe hard MemoryMax is not calibrated")
    if children["rquant-research.slice"].get("MemoryMax") != "768M":
        errors.append("rquant-research.slice: MemoryMax must be the strict 768M cap")
    if parent_low < live_low:
        errors.append("parent MemoryLow is below live MemoryLow")

    normal = budget["live"] + budget["serving"] + budget["research"] + budget["os"]
    if normal > budget["usable_host"]:
        errors.append("normal research regime exceeds usable host memory")
    if budget["live"] - budget["monitor_observed_peak"] < 1024:
        errors.append("live envelope leaves less than 1024 MiB above monitor peak")

    for unit in MAINTENANCE_UNITS:
        try:
            service = _parse_unit(systemd_dir / unit)["Service"]
        except (configparser.Error, KeyError, OSError) as exc:
            errors.append(f"{unit}: cannot parse ({type(exc).__name__})")
            continue
        if service.get("Slice") != "rquant-maintenance.slice":
            errors.append(f"{unit}: must use aggregate rquant-maintenance.slice")
        if "MemoryHigh" in service:
            errors.append(f"{unit}: uncalibrated service MemoryHigh must be absent")
        if "MemoryMax" in service:
            errors.append(f"{unit}: backup/replica file cache must not have service MemoryMax")

    maintenance_slice = children["rquant-maintenance.slice"]
    if "MemoryHigh" in maintenance_slice or "MemoryMax" in maintenance_slice:
        errors.append("rquant-maintenance.slice: memory threshold is pending calibration")

    for unit, slice_name in WORKLOAD_UNIT_SLICES.items():
        if slice_name != "rquant-research.slice":
            continue
        try:
            service = _parse_unit(systemd_dir / unit)["Service"]
            service_max_raw = service.get("MemoryMax")
            if service_max_raw is None:
                continue
            service_max = parse_systemd_bytes(service_max_raw)
        except (configparser.Error, KeyError, OSError, ValueError) as exc:
            errors.append(f"{unit}: cannot parse service MemoryMax ({type(exc).__name__})")
            continue
        if service_max > RESEARCH_MEMORY_MAX_BYTES:
            errors.append(f"{unit}: service MemoryMax exceeds research slice cap")

    details.extend(
        (
            f"  ok nominal host=8192 MiB; minimum reported usable={budget['usable_host']} MiB",
            f"  ok normal research regime={normal} MiB <= {budget['usable_host']} MiB",
            f"  ok live={budget['live']} MiB includes monitor peak="
            f"{budget['monitor_observed_peak']} MiB + "
            f"{budget['live'] - budget['monitor_observed_peak']} MiB concurrency margin",
            "  warn maintenance memory threshold pending calibration; backup and replica "
            "remain uncapped and their timers are unchanged",
            "  ok parent/live/serving use MemoryHigh reclaim; only research has MemoryMax",
        )
    )
    if errors:
        return WorkloadCheck(
            "workload_memory_admission",
            "fail",
            f"{len(errors)} workload memory envelope contracts failed",
            tuple(details + [f"  x {error}" for error in errors]),
        )
    return WorkloadCheck(
        "workload_memory_admission",
        "warn",
        "maintenance memory calibration is pending; research acceptance remains blocked",
        tuple(details),
    )


def verify_workload_unit_declarations(systemd_dir: Path) -> WorkloadCheck:
    """Check checked-in unit declarations without requiring Linux or systemd."""

    errors: list[str] = []
    details: list[str] = []
    expected_slices = {PARENT_SLICE: PARENT_SLICE_LIMITS, **WORKLOAD_SLICE_LIMITS}
    for slice_name, expected_limits in expected_slices.items():
        path = systemd_dir / slice_name
        if not path.is_file():
            errors.append(f"{slice_name}: missing")
            continue
        try:
            section = _parse_unit(path)["Slice"]
        except (configparser.Error, KeyError, OSError) as exc:
            errors.append(f"{slice_name}: cannot parse ({type(exc).__name__})")
            continue
        expected = {**_SLICE_ACCOUNTING, **expected_limits}
        for key, value in expected.items():
            if section.get(key) != value:
                errors.append(f"{slice_name}: {key}={section.get(key)!r}, expected {value!r}")
        unexpected_max = section.get("MemoryMax")
        if "MemoryMax" not in expected_limits and unexpected_max is not None:
            errors.append(f"{slice_name}: unexpected MemoryMax={unexpected_max!r}")
        details.append(f"  ok static slice {slice_name}")

    observed_services = {path.name for path in systemd_dir.glob("rquant-*.service")}
    expected_services = set(WORKLOAD_UNIT_SLICES)
    for unit in sorted(observed_services - expected_services):
        if unit in LEGACY_RUNTIME_TEMPLATES:
            errors.append(
                f"{unit}: legacy template is installed; run migration preview and "
                "accept only after concrete replacement verification"
            )
        else:
            errors.append(f"{unit}: unclassified production service")
    for unit in sorted(expected_services - observed_services):
        errors.append(f"{unit}: expected production service is missing")

    for unit, expected_slice in WORKLOAD_UNIT_SLICES.items():
        path = systemd_dir / unit
        if not path.is_file():
            continue
        try:
            service = _parse_unit(path)["Service"]
        except (configparser.Error, KeyError, OSError) as exc:
            errors.append(f"{unit}: cannot parse ({type(exc).__name__})")
            continue
        actual_slice = service.get("Slice")
        if actual_slice != expected_slice:
            errors.append(f"{unit}: Slice={actual_slice!r}, expected {expected_slice!r}")
        for key, value in _EXCEPTION_LIMITS.get(unit, {}).items():
            if service.get(key) != value:
                errors.append(f"{unit}: {key}={service.get(key)!r}, expected {value!r}")
        if expected_slice == "rquant-research.slice":
            if "ExecCondition" in service:
                errors.append(f"{unit}: racy research ExecCondition must be removed")
            if not service.get("ExecStart", "").startswith(f"{WORKLOAD_ARBITER_PATH} research -- "):
                errors.append(f"{unit}: research lifecycle arbiter is missing")
            success_statuses = set(service.get("SuccessExitStatus", "").split())
            if "75" not in success_statuses:
                errors.append(f"{unit}: research rejection status 75 is not accepted")
        if expected_slice == "rquant-maintenance.slice" and not service.get(
            "ExecStart", ""
        ).startswith(f"{WORKLOAD_ARBITER_PATH} maintenance "):
            errors.append(f"{unit}: maintenance lifecycle arbiter is missing")
        details.append(f"  ok {unit} -> {expected_slice}")

    admission = verify_workload_memory_admission(systemd_dir)
    details.extend(admission.details)
    if admission.status == "fail":
        errors.append(admission.summary)
    if errors:
        return WorkloadCheck(
            "workload_unit_declarations",
            "fail",
            f"{len(errors)} workload unit/slice static contracts failed",
            tuple(details + [f"  x {error}" for error in errors]),
        )
    if admission.status == "warn":
        return WorkloadCheck(
            "workload_unit_declarations",
            "warn",
            (
                f"{len(WORKLOAD_UNIT_SLICES)} production services classified; "
                "maintenance memory is pending calibration"
            ),
            tuple(details),
        )
    return WorkloadCheck(
        "workload_unit_declarations",
        "ok",
        (
            f"{len(WORKLOAD_UNIT_SLICES)} production services explicitly classified; "
            "maintenance is aggregate and root authority remains in system.slice"
        ),
        tuple(details),
    )


def _properties(stdout: str) -> dict[str, str]:
    return {
        key: value
        for line in stdout.splitlines()
        if "=" in line
        for key, value in (line.split("=", 1),)
    }


def _run_systemctl(
    systemctl_path: Path,
    arguments: Sequence[str],
) -> tuple[subprocess.CompletedProcess[str] | None, str | None, bool]:
    command = [str(systemctl_path), *arguments]
    try:
        completed = subprocess.run(
            command,
            check=False,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=8,
        )
    except subprocess.TimeoutExpired as exc:
        return None, f"TimeoutExpired after {exc.timeout}s", False
    except PermissionError as exc:
        return None, f"PermissionError: {exc}", False
    except OSError as exc:
        unavailable = isinstance(exc, FileNotFoundError)
        return None, f"{type(exc).__name__}: {exc}", unavailable
    if completed.returncode == 0:
        return completed, None, False
    diagnostic = (completed.stderr or completed.stdout).strip()[:240]
    unavailable = any(token in diagnostic.lower() for token in _NON_SYSTEMD_ERRORS)
    return (
        completed,
        f"systemctl exit={completed.returncode}: {diagnostic or 'no diagnostic'}",
        unavailable,
    )


def _systemctl_unavailable(strict: bool, summary: str) -> WorkloadCheck:
    status: CheckStatus = "fail" if strict else "skip"
    context = "cloud strict gate fails closed" if strict else "non-systemd Linux skipped"
    return WorkloadCheck("workload_runtime", status, f"{summary}; {context}")


def _list_loaded_rquant_services(
    systemctl_path: Path,
) -> tuple[tuple[str, ...], str | None, bool]:
    completed, failure, unavailable = _run_systemctl(
        systemctl_path,
        (
            "list-units",
            "--all",
            "--type=service",
            "--plain",
            "--no-legend",
            "--no-pager",
            "rquant-*.service",
        ),
    )
    if completed is None or failure:
        return (), failure, unavailable
    units = tuple(
        line.split(maxsplit=1)[0]
        for line in completed.stdout.splitlines()
        if line.strip() and line.split(maxsplit=1)[0].endswith(".service")
    )
    return units, None, False


def _show_systemd_unit(
    systemctl_path: Path,
    unit: str,
) -> tuple[dict[str, str], str | None, bool]:
    completed, failure, unavailable = _run_systemctl(
        systemctl_path,
        (
            "show",
            unit,
            "--no-pager",
            f"--property={_SYSTEMCTL_PROPERTIES}",
        ),
    )
    if completed is None or failure:
        return {}, failure, unavailable
    return _properties(completed.stdout), None, False


def _template_for_instance(unit: str) -> str:
    prefix, marker, suffix = unit.partition("@")
    if not marker or not suffix.endswith(".service"):
        return unit
    return f"{prefix}@.service"


def _expected_slice_for_loaded_unit(unit: str) -> str | None:
    return WORKLOAD_UNIT_SLICES.get(unit) or WORKLOAD_UNIT_SLICES.get(_template_for_instance(unit))


def _is_descendant(control_group: str, ancestor: str) -> bool:
    path = PurePosixPath(control_group)
    parent = PurePosixPath(ancestor)
    return path != parent and parent in path.parents


def _read_cgroup_value(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return None


def _io_weight(value: str | None) -> str | None:
    if value is None:
        return None
    parts = value.split()
    return parts[-1] if parts else None


def _runtime_memory_matches(observed: str | None, expected: str) -> bool:
    if observed is None:
        return False
    try:
        return int(observed) == parse_systemd_bytes(expected)
    except ValueError:
        return observed == expected


def _is_unbounded(value: str | None) -> bool:
    return value in {"max", "infinity", "18446744073709551615"}


def _cgroup_path(cgroup_root: Path, control_group: str) -> Path:
    return cgroup_root / control_group.removeprefix("/")


def _arbiter_provenance_failure(
    arbiter_path: Path,
    digest_path: Path,
    *,
    expected_uid: int,
) -> str | None:
    try:
        helper_metadata = arbiter_path.lstat()
        digest_metadata = digest_path.lstat()
    except OSError as exc:
        return f"arbiter provenance unavailable: {type(exc).__name__}: {exc}"
    expected = (
        (arbiter_path, helper_metadata, 0o755),
        (digest_path, digest_metadata, 0o444),
    )
    for path, metadata, mode in expected:
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_uid != expected_uid
            or stat.S_IMODE(metadata.st_mode) != mode
        ):
            return f"unsafe arbiter provenance metadata: {path}"
    try:
        declared = digest_path.read_text(encoding="ascii")
        observed = hashlib.sha256(arbiter_path.read_bytes()).hexdigest()
    except (OSError, UnicodeError) as exc:
        return f"cannot read arbiter provenance: {type(exc).__name__}: {exc}"
    if not re.fullmatch(r"[0-9a-f]{64}\n", declared):
        return "arbiter sha256 file is not canonical"
    if declared.rstrip("\n") != observed:
        return "arbiter sha256 does not match the installed fixed helper"
    return None


def check_workload_runtime(
    *,
    cgroup_root: Path = Path("/sys/fs/cgroup"),
    platform_name: str | None = None,
    systemctl_path: Path = Path("/usr/bin/systemctl"),
    strict: bool = False,
    advisory_units: ReceiptBoundWorkloadAdvisories | None = None,
    arbiter_path: Path = Path(WORKLOAD_ARBITER_PATH),
    arbiter_hash_path: Path = Path(WORKLOAD_ARBITER_HASH_PATH),
    arbiter_expected_uid: int = 0,
) -> WorkloadCheck:
    """Audit loaded services and resolved cgroup-v2 state without changing units."""

    resolved_platform = platform_name or ("Linux" if Path("/proc").is_dir() else "Darwin")
    if resolved_platform != "Linux":
        return WorkloadCheck(
            "workload_runtime",
            "skip",
            "non-Linux: static/fixture checks only; cloud must audit systemd/cgroup v2",
        )
    controllers = _read_cgroup_value(cgroup_root / "cgroup.controllers")
    if controllers is None:
        return _systemctl_unavailable(strict, "cgroup v2 controllers are unavailable")
    missing_controllers = _REQUIRED_CGROUP_CONTROLLERS - set(controllers.split())
    if missing_controllers:
        return WorkloadCheck(
            "workload_runtime",
            "fail",
            "cgroup v2 is missing required controllers",
            (f"  x {', '.join(sorted(missing_controllers))}",),
        )

    loaded_units, failure, unavailable = _list_loaded_rquant_services(systemctl_path)
    if failure:
        if unavailable:
            return _systemctl_unavailable(strict, failure)
        return WorkloadCheck("workload_runtime", "fail", failure)
    if strict and not loaded_units:
        return WorkloadCheck(
            "workload_runtime",
            "fail",
            "cloud strict gate found no loaded rquant service units",
        )

    legacy_units = tuple(
        unit for unit in loaded_units if _template_for_instance(unit) in LEGACY_RUNTIME_TEMPLATES
    )
    if legacy_units:
        return WorkloadCheck(
            "workload_runtime",
            "fail",
            "legacy runtime migration required before workload acceptance",
            tuple(f"  x loaded legacy instance: {unit}" for unit in legacy_units),
        )

    errors: list[str] = []
    details = [f"  ok cgroup v2 controllers: {controllers}"]
    if strict:
        arbiter_failure = _arbiter_provenance_failure(
            arbiter_path,
            arbiter_hash_path,
            expected_uid=arbiter_expected_uid,
        )
        if arbiter_failure:
            errors.append(arbiter_failure)
        else:
            details.append("  ok fixed root-owned arbiter sha256 provenance")
    advisory_names = set(advisory_units.units if advisory_units is not None else ())
    advisory_degraded = bool(advisory_units is not None and advisory_units.health_status != "ok")
    if advisory_units is not None:
        details.append(
            f"  advisory receipt generation={advisory_units.generation_hash}: "
            f"{advisory_units.health_summary}"
        )

    def record_unit_issue(unit: str, message: str) -> None:
        nonlocal advisory_degraded
        if unit in advisory_names:
            advisory_degraded = True
            details.append(f"  advisory {unit}: {message}")
            return
        errors.append(f"{unit}: {message}")

    loaded_contracts: dict[str, str] = {}
    service_properties: dict[str, dict[str, str]] = {}
    active_plane_slices: set[str] = set()
    audit_units = tuple(dict.fromkeys((*loaded_units, *sorted(advisory_names))))
    for unit in audit_units:
        expected_slice = _expected_slice_for_loaded_unit(unit)
        if expected_slice is None:
            record_unit_issue(unit, "loaded rquant service has no template classification")
            continue
        loaded_contracts[unit] = expected_slice
        properties, show_failure, _unavailable = _show_systemd_unit(systemctl_path, unit)
        if show_failure:
            record_unit_issue(unit, show_failure)
            continue
        service_properties[unit] = properties
        if properties.get("LoadState") != "loaded":
            record_unit_issue(unit, f"LoadState={properties.get('LoadState')!r}")
        if properties.get("Slice") != expected_slice:
            record_unit_issue(
                unit,
                f"Slice={properties.get('Slice')!r}, expected {expected_slice!r}",
            )
        resolved_exec = properties.get("ExecStart", "")
        if expected_slice in {"rquant-research.slice", "rquant-maintenance.slice"}:
            plane = "research" if expected_slice == "rquant-research.slice" else "maintenance"
            expected_argv = f"argv[]={WORKLOAD_ARBITER_PATH} {plane} "
            if (
                f"path={WORKLOAD_ARBITER_PATH}" not in resolved_exec
                or expected_argv not in resolved_exec
            ):
                record_unit_issue(
                    unit,
                    f"resolved ExecStart does not use fixed {plane} arbiter",
                )
        active_state = properties.get("ActiveState", "")
        if active_state == "failed":
            record_unit_issue(unit, "ActiveState=failed")
        if (
            unit not in advisory_names
            and active_state in _RUNNING_STATES
            and expected_slice in WORKLOAD_SLICE_LIMITS
        ):
            active_plane_slices.add(expected_slice)

    required_slices = {PARENT_SLICE, *WORKLOAD_SLICE_LIMITS}
    if _SYSTEM_SLICE in loaded_contracts.values():
        required_slices.add(_SYSTEM_SLICE)
    slice_control_groups: dict[str, str] = {}
    slice_properties: dict[str, dict[str, str]] = {}
    for slice_name in sorted(required_slices):
        properties, show_failure, _unavailable = _show_systemd_unit(systemctl_path, slice_name)
        if show_failure:
            errors.append(f"{slice_name}: {show_failure}")
            continue
        if properties.get("LoadState") != "loaded":
            errors.append(f"{slice_name}: LoadState={properties.get('LoadState')!r}")
        slice_properties[slice_name] = properties
        control_group = properties.get("ControlGroup", "")
        if control_group.startswith("/"):
            slice_control_groups[slice_name] = control_group

    parent_control_group = slice_control_groups.get(PARENT_SLICE)
    if active_plane_slices and parent_control_group is None:
        errors.append("rquant.slice: active plane exists but parent ControlGroup is unresolved")
    elif not active_plane_slices and parent_control_group is None:
        details.append("  ok all planes idle; parent cgroup not required")
    if parent_control_group:
        for slice_name in WORKLOAD_SLICE_LIMITS:
            child_control_group = slice_control_groups.get(slice_name)
            if child_control_group and not _is_descendant(
                child_control_group, parent_control_group
            ):
                errors.append(
                    f"{slice_name}: ControlGroup={child_control_group!r} is not below "
                    f"{parent_control_group!r}"
                )

    for unit, expected_slice in loaded_contracts.items():
        properties = service_properties.get(unit)
        if properties is None:
            continue
        active_state = properties.get("ActiveState", "")
        control_group = properties.get("ControlGroup", "")
        if active_state in _RUNNING_STATES:
            expected_control_group = slice_control_groups.get(expected_slice)
            if not expected_control_group or not _is_descendant(
                control_group, expected_control_group
            ):
                record_unit_issue(
                    unit,
                    f"ControlGroup={control_group!r}, expected descendant of "
                    f"resolved {expected_control_group!r}",
                )
        template = _template_for_instance(unit)
        for key, value in _EXCEPTION_LIMITS.get(template, {}).items():
            observed = properties.get(key)
            matches = (
                _runtime_memory_matches(observed, value)
                if key.startswith("Memory")
                else observed == value
            )
            if not matches:
                record_unit_issue(
                    unit,
                    f"runtime {key}={observed!r}, expected {value!r}",
                )
        detail_prefix = "advisory" if unit in advisory_names else "ok loaded"
        details.append(
            f"  {detail_prefix} {unit}: {active_state or '?'} "
            f"Slice={properties.get('Slice') or '?'} ControlGroup={control_group or '-'}"
        )

    expected_slices = {PARENT_SLICE: PARENT_SLICE_LIMITS, **WORKLOAD_SLICE_LIMITS}
    for slice_name, limits in expected_slices.items():
        properties = slice_properties.get(slice_name)
        control_group = slice_control_groups.get(slice_name)
        plane_active = slice_name in active_plane_slices or (
            slice_name == PARENT_SLICE and bool(active_plane_slices)
        )
        if properties is None:
            continue
        if control_group is None:
            if plane_active:
                errors.append(f"{slice_name}: active but ControlGroup is unresolved")
            else:
                details.append(f"  ok {slice_name} idle; cgroup not materialized")
            continue
        for key in ("CPUWeight", "IOWeight", "TasksMax"):
            if properties.get(key) != limits[key]:
                errors.append(
                    f"{slice_name}: {key}={properties.get(key)!r}, expected {limits[key]!r}"
                )
        for key in ("MemoryLow", "MemoryHigh"):
            expected = limits.get(key)
            matches = (
                _is_unbounded(properties.get(key))
                if key == "MemoryHigh" and expected is None
                else _runtime_memory_matches(properties.get(key), expected or "0")
            )
            if not matches:
                errors.append(
                    f"{slice_name}: {key}={properties.get(key)!r}, "
                    f"expected {expected or ('unbounded' if key == 'MemoryHigh' else '0')!r}"
                )
        expected_max = limits.get("MemoryMax")
        if expected_max is None:
            if not _is_unbounded(properties.get("MemoryMax")):
                errors.append(f"{slice_name}: MemoryMax must be unbounded")
        elif not _runtime_memory_matches(properties.get("MemoryMax"), expected_max):
            errors.append(
                f"{slice_name}: MemoryMax={properties.get('MemoryMax')!r}, "
                f"expected {expected_max!r}"
            )

        cgroup = _cgroup_path(cgroup_root, control_group)
        if not cgroup.is_dir():
            if plane_active:
                errors.append(f"{slice_name}: resolved cgroup path {cgroup} is missing")
            continue
        cgroup_values = {
            "CPUWeight": _read_cgroup_value(cgroup / "cpu.weight"),
            "IOWeight": _io_weight(_read_cgroup_value(cgroup / "io.weight")),
            "MemoryLow": _read_cgroup_value(cgroup / "memory.low"),
            "MemoryHigh": _read_cgroup_value(cgroup / "memory.high"),
            "MemoryMax": _read_cgroup_value(cgroup / "memory.max"),
            "TasksMax": _read_cgroup_value(cgroup / "pids.max"),
        }
        for key in ("CPUWeight", "IOWeight", "TasksMax"):
            if cgroup_values[key] != limits[key]:
                errors.append(
                    f"{slice_name}: cgroup {key}={cgroup_values[key]!r}, expected {limits[key]!r}"
                )
        for key in ("MemoryLow", "MemoryHigh"):
            expected_bytes = (
                "max"
                if key == "MemoryHigh" and key not in limits
                else str(parse_systemd_bytes(limits.get(key, "0")))
            )
            if cgroup_values[key] != expected_bytes:
                errors.append(
                    f"{slice_name}: cgroup {key}={cgroup_values[key]!r}, "
                    f"expected {expected_bytes!r}"
                )
        if expected_max is None:
            if cgroup_values["MemoryMax"] != "max":
                errors.append(f"{slice_name}: cgroup memory.max must be max")
        else:
            expected_bytes = str(parse_systemd_bytes(expected_max))
            if cgroup_values["MemoryMax"] != expected_bytes:
                errors.append(
                    f"{slice_name}: cgroup MemoryMax={cgroup_values['MemoryMax']!r}, "
                    f"expected {expected_bytes!r}"
                )
        if slice_name == "rquant-research.slice":
            cpu_max = _read_cgroup_value(cgroup / "cpu.max")
            try:
                quota_raw, period_raw = (cpu_max or "").split()
                quota_ok = int(quota_raw) > 0 and int(quota_raw) == int(period_raw)
            except (TypeError, ValueError):
                quota_ok = False
            if not quota_ok:
                errors.append(
                    f"{slice_name}: cgroup cpu.max={cpu_max!r} must equal exactly one CPU"
                )
            if properties.get("CPUQuotaPerSecUSec") not in {"1s", "100%"}:
                errors.append(f"{slice_name}: systemd CPUQuota is not exactly 100%")
            details.append(f"  ok research cpu.max={cpu_max}")
        details.append(f"  ok runtime slice {slice_name} ControlGroup={control_group}")

    if errors:
        return WorkloadCheck(
            "workload_runtime",
            "fail",
            f"{len(errors)} cloud workload runtime checks failed",
            tuple(details + [f"  x {error}" for error in errors]),
        )
    if advisory_degraded:
        return WorkloadCheck(
            "workload_runtime",
            "warn",
            "authoritative workload controls are valid; receipt-bound advisory is degraded",
            tuple(details),
        )
    return WorkloadCheck(
        "workload_runtime",
        "ok",
        "loaded service instances and resolved cgroup-v2 controls are valid",
        tuple(details),
    )


def _read_meminfo(path: Path) -> dict[str, int]:
    values: dict[str, int] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return values
    for line in lines:
        key, separator, raw_value = line.partition(":")
        if not separator:
            continue
        fields = raw_value.split()
        if len(fields) != 2 or fields[1] != "kB" or not fields[0].isdigit():
            continue
        values[key] = int(fields[0]) * 1024
    return values


def check_workload_capacity_baseline(
    *,
    data_path: Path = Path("/home/lighthouse/rquant/data"),
    meminfo_path: Path = Path("/proc/meminfo"),
    platform_name: str | None = None,
    cpu_count: int | None = None,
    strict: bool = False,
) -> WorkloadCheck:
    """Verify the measured nominal 8 GiB host capacity baseline."""

    resolved_platform = platform_name or ("Linux" if Path("/proc").is_dir() else "Darwin")
    if resolved_platform != "Linux":
        return WorkloadCheck(
            "workload_capacity",
            "skip",
            "non-Linux: cloud gate must read real /proc and data-volume capacity",
        )
    production_path_available = data_path.exists() or data_path.parent.exists()
    if not meminfo_path.is_file() or not production_path_available:
        status: CheckStatus = "fail" if strict else "skip"
        context = "cloud strict gate fails closed" if strict else "non-production Linux skipped"
        return WorkloadCheck(
            "workload_capacity",
            status,
            f"production meminfo/data path unavailable; {context}",
        )
    memory = _read_meminfo(meminfo_path)
    total_memory = memory.get("MemTotal")
    available_memory = memory.get("MemAvailable")
    observed_cpu_count = cpu_count if cpu_count is not None else os.cpu_count()
    target = data_path if data_path.exists() else data_path.parent
    try:
        free_disk = shutil.disk_usage(target).free
    except OSError:
        free_disk = None

    errors: list[str] = []
    details: list[str] = []
    if observed_cpu_count is None or observed_cpu_count < _MIN_CPU_COUNT:
        errors.append(f"CPU={observed_cpu_count!r}, baseline requires >= {_MIN_CPU_COUNT}")
    else:
        details.append(f"  ok CPU={observed_cpu_count}")
    if total_memory is None or total_memory < MIN_HOST_MEMORY_BYTES:
        errors.append("MemTotal below 7.5 GiB usable baseline for an 8 GiB-class host")
    else:
        details.append(f"  ok MemTotal={total_memory / 1024**3:.1f} GiB")
    if available_memory is None or available_memory < _MIN_AVAILABLE_MEMORY_BYTES:
        errors.append("MemAvailable below 2048 MiB research admission floor")
    else:
        details.append(f"  ok MemAvailable={available_memory / 1024**3:.1f} GiB")
    if free_disk is None or free_disk < _MIN_FREE_DISK_BYTES:
        errors.append(f"free disk below 8 GiB at {target}")
    else:
        details.append(f"  ok free_disk={free_disk / 1024**3:.1f} GiB at {target}")

    if errors:
        return WorkloadCheck(
            "workload_capacity",
            "fail",
            f"{len(errors)} host capacity baselines failed; research admission is blocked",
            tuple(details + [f"  x {error}" for error in errors]),
        )
    return WorkloadCheck(
        "workload_capacity",
        "ok",
        "host capacity satisfies the measured 2 CPU/8 GiB-class baseline",
        tuple(details),
    )


def check_workload_high_water_evidence(
    *,
    evidence_path: Path = DEFAULT_HIGH_WATER_EVIDENCE_PATH,
    raw_evidence_path: Path | None = None,
    strict: bool = False,
    as_of: datetime | None = None,
) -> WorkloadCheck:
    """Validate fresh concurrent high-water evidence without mutating the host."""

    status: CheckStatus = "fail" if strict else "warn"
    try:
        raw = json.loads(evidence_path.read_text(encoding="utf-8"))
        evidence = WorkloadHighWaterEvidence.model_validate(raw)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        return WorkloadCheck(
            "workload_high_water",
            status,
            f"high-water evidence unavailable or invalid: {type(exc).__name__}",
            (f"  x {evidence_path}",),
        )

    resolved_raw_path = raw_evidence_path or (
        DEFAULT_HIGH_WATER_RAW_EVIDENCE_PATH
        if evidence_path == DEFAULT_HIGH_WATER_EVIDENCE_PATH
        else evidence_path.with_name("samples.jsonl")
    )
    try:
        from rquant.workload_evidence import (
            WorkloadEvidenceError,
            summarize_workload_samples,
        )

        replayed = summarize_workload_samples(resolved_raw_path)
    except (OSError, WorkloadEvidenceError, ValueError) as exc:
        return WorkloadCheck(
            "workload_high_water",
            status,
            f"raw high-water evidence replay failed: {type(exc).__name__}",
            (f"  x {resolved_raw_path}: {exc}",),
        )
    declared_fields = evidence.model_dump(mode="json")
    replayed_fields = replayed.model_dump(mode="json")
    mismatched_fields = tuple(
        name for name in declared_fields if declared_fields[name] != replayed_fields[name]
    )
    if mismatched_fields:
        return WorkloadCheck(
            "workload_high_water",
            status,
            "declared high-water summary does not match replayed raw evidence",
            tuple(
                f"  x {name}: declared={declared_fields[name]!r} replayed={replayed_fields[name]!r}"
                for name in mismatched_fields
            ),
        )
    observed_raw_sha = replayed.raw_evidence_sha256

    observed_at = (as_of or datetime.now(UTC)).astimezone(UTC)
    age = observed_at - evidence.observed_at.astimezone(UTC)
    if age < timedelta(0) or age > _HIGH_WATER_MAX_AGE:
        return WorkloadCheck(
            "workload_high_water",
            status,
            f"high-water evidence is stale: age={age.total_seconds() / 86400:.1f}d",
        )

    maintenance_sum = evidence.backup_peak_mib + evidence.replica_peak_mib
    errors: list[str] = []
    if evidence.host_mem_total_mib < WORKLOAD_MEMORY_BUDGET_MIB["usable_host"]:
        errors.append("observed host memory is below the usable 8 GiB-class baseline")
    if evidence.min_mem_available_mib * 1024**2 < _MIN_AVAILABLE_MEMORY_BYTES:
        errors.append("24h minimum MemAvailable is below the research admission floor")
    observed_with_research = (
        evidence.live_concurrent_peak_mib
        + evidence.serving_concurrent_peak_mib
        + evidence.maintenance_concurrent_peak_mib
        + evidence.os_system_slice_peak_mib
        + RESEARCH_MEMORY_MAX_BYTES // 1024**2
    )
    if observed_with_research > evidence.host_mem_total_mib:
        errors.append("observed plane/OS peaks plus research hard cap exceed host memory")

    details = [
        f"  ok monitor current={evidence.monitor_current_mib} MiB; "
        f"monitor peak={evidence.monitor_peak_mib} MiB",
        f"  ok live concurrent peak={evidence.live_concurrent_peak_mib} MiB",
        f"  ok serving concurrent peak={evidence.serving_concurrent_peak_mib} MiB",
        f"  ok backup+replica observed peaks={maintenance_sum} MiB; "
        f"maintenance concurrent peak={evidence.maintenance_concurrent_peak_mib} MiB",
        f"  ok backup successful runs={evidence.backup_successful_runs} "
        f"samples={evidence.backup_sample_count} "
        f"runtime={evidence.backup_successful_runtime_seconds}s",
        f"  ok replica successful runs={evidence.replica_successful_runs} "
        f"samples={evidence.replica_sample_count} "
        f"runtime={evidence.replica_successful_runtime_seconds}s",
        f"  ok OS/system.slice peak={evidence.os_system_slice_peak_mib} MiB; "
        f"minimum MemAvailable={evidence.min_mem_available_mib} MiB",
        f"  ok raw evidence sha256={observed_raw_sha}",
        f"  ok observation window={evidence.observation_window_hours}h age="
        f"{age.total_seconds() / 3600:.1f}h",
    ]
    if errors:
        return WorkloadCheck(
            "workload_high_water",
            status,
            f"{len(errors)} high-water calibration checks failed",
            tuple(details + [f"  x {error}" for error in errors]),
        )
    return WorkloadCheck(
        "workload_high_water",
        status,
        ("fresh 24h raw evidence is complete; maintenance memory policy is pending calibration"),
        tuple(details),
    )


def check_research_workload_admission(
    *,
    evidence_path: Path = DEFAULT_HIGH_WATER_EVIDENCE_PATH,
    meminfo_path: Path = Path("/proc/meminfo"),
    platform_name: str | None = None,
    systemctl_path: Path = Path("/usr/bin/systemctl"),
    as_of: datetime | None = None,
) -> WorkloadCheck:
    """Fail closed before a research unit starts under unsafe host conditions."""

    resolved_platform = platform_name or ("Linux" if Path("/proc").is_dir() else "Darwin")
    if resolved_platform != "Linux":
        return WorkloadCheck(
            "research_workload_admission",
            "fail",
            "research workload admission requires the production Linux host",
        )
    high_water = check_workload_high_water_evidence(
        evidence_path=evidence_path,
        strict=True,
        as_of=as_of,
    )
    if high_water.status != "ok":
        return WorkloadCheck(
            "research_workload_admission",
            "fail",
            f"research blocked: {high_water.summary}",
            high_water.details,
        )

    memory = _read_meminfo(meminfo_path)
    total = memory.get("MemTotal")
    available = memory.get("MemAvailable")
    if total is None or total < MIN_HOST_MEMORY_BYTES:
        return WorkloadCheck(
            "research_workload_admission",
            "fail",
            "research blocked: host memory is below the usable baseline",
        )
    if available is None or available < _MIN_AVAILABLE_MEMORY_BYTES:
        return WorkloadCheck(
            "research_workload_admission",
            "fail",
            "research blocked: MemAvailable is below 2048 MiB",
        )

    active_maintenance: list[str] = []
    for unit in sorted(MAINTENANCE_UNITS):
        properties, failure, _unavailable = _show_systemd_unit(systemctl_path, unit)
        if failure:
            return WorkloadCheck(
                "research_workload_admission",
                "fail",
                f"research blocked: cannot resolve maintenance state: {failure}",
            )
        if properties.get("LoadState") != "loaded":
            return WorkloadCheck(
                "research_workload_admission",
                "fail",
                f"research blocked: maintenance unit {unit} is not loaded",
            )
        if properties.get("ActiveState") in _RUNNING_STATES:
            active_maintenance.append(unit)
    if active_maintenance:
        return WorkloadCheck(
            "research_workload_admission",
            "fail",
            "research blocked: maintenance active",
            tuple(f"  x {unit}" for unit in active_maintenance),
        )
    return WorkloadCheck(
        "research_workload_admission",
        "ok",
        "research admitted: high-water, memory reserve, and maintenance state are safe",
        (
            f"  ok MemAvailable={available / 1024**2:.0f} MiB",
            "  ok backup and replica-sync inactive",
        ),
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run the systemd ExecCondition admission probe."""

    arguments = tuple(argv if argv is not None else sys.argv[1:])
    if arguments != ("research-admission",):
        print("usage: python -m rquant.workload_isolation research-admission", file=sys.stderr)
        return 2
    result = check_research_workload_admission()
    print(f"{result.status.upper()} {result.summary}")
    if result.details:
        print("\n".join(result.details))
    return 0 if result.status == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
