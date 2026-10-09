"""Bounded, read-only systemd observations for the optional ops owner."""

from __future__ import annotations

import base64
import os
import re
import selectors
import socket
import stat
import subprocess
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from decimal import Decimal, localcontext
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal

from pydantic import (
    Field,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
    field_validator,
    model_validator,
)

from rquant.ed25519_verify import verify_ed25519_signature
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads

if TYPE_CHECKING:
    from rquant.task_center_projection import TaskOpsSample
    from rquant.task_center_runtime import TaskUnitRunSource
    from rquant.task_cpu import LinuxTaskCpuReader, TaskCpuObservation

STATIC_TIMER_STEMS = (
    "artifact-retention",
    "backup",
    "daily-report",
    "daily",
    "kpl-snapshot",
    "midday-report",
    "monitor-watchdog",
    "monitor",
    "morning-pulse",
    "pre-market-check",
    "replica-sync",
    "research-ingest",
    "surge-watch",
    "tushare-token-reminder",
)
_STATIC_TIMERS = frozenset(f"rquant-{stem}.timer" for stem in STATIC_TIMER_STEMS)
_TEMPLATE_STEMS = (
    "rquant-runtime-daily-orchestrator@",
    "rquant-runtime-recovery-rehearsal@",
)
_INSTANCE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
_SLICES = (
    "rquant.slice",
    "rquant-live.slice",
    "rquant-serving.slice",
    "rquant-research.slice",
    "rquant-maintenance.slice",
)
_PROPERTIES: Mapping[str, tuple[str, ...]] = MappingProxyType(
    {
        "timer": (
            "LoadState",
            "UnitFileState",
            "ActiveState",
            "SubState",
            "LastTriggerUSec",
            "NextElapseUSecRealtime",
        ),
        "service": (
            "LoadState",
            "ActiveState",
            "SubState",
            "Result",
            "InvocationID",
            "ExecMainStatus",
            "ExecMainStartTimestamp",
            "ExecMainExitTimestamp",
        ),
        "slice": ("LoadState", "ActiveState", "MemoryCurrent", "MemoryPeak"),
    }
)
_BOOT_PATH = "/proc/sys/kernel/random/boot_id"
_MEMINFO_PATH = "/proc/meminfo"
_STAT_PATH = "/proc/stat"
_MAX_CPU_STAT_BYTES = 32 * 1024
_MAX_CPU_PAIR_BYTES = 4 * 1024
_MAX_UNIT_COUNT = 32
_MAX_COMMAND_BYTES = 4_096
_MAX_MANIFEST_BYTES = 64 * 1024
_TOTAL_SECONDS = 20.0
_PER_COMMAND_SECONDS = 1.0
_TIMESTAMP = re.compile(
    r"^[A-Za-z]{3} (?P<date>\d{4}-\d{2}-\d{2}) "
    r"(?P<clock>\d{2}:\d{2}:\d{2}) UTC$"
)


def _allowed_unit(unit: str, kind: Literal["timer", "service", "slice"]) -> bool:
    if kind == "slice":
        return unit in _SLICES
    timer = unit if kind == "timer" else unit.removesuffix(".service") + ".timer"
    if timer in _STATIC_TIMERS:
        expected = timer if kind == "timer" else timer.removesuffix(".timer") + ".service"
        return unit == expected
    for stem in _TEMPLATE_STEMS:
        if timer.startswith(stem) and timer.endswith(".timer"):
            instance = timer[len(stem) : -len(".timer")]
            expected = timer if kind == "timer" else timer.removesuffix(".timer") + ".service"
            return bool(_INSTANCE.fullmatch(instance)) and unit == expected
    return False


class OpsUnitInstall(RuntimeContractModel):
    timer: StrictStr
    service: StrictStr
    label: StrictStr = Field(min_length=1, max_length=40)
    expected_enabled: StrictBool
    session: Literal["all", "trading_day", "market_hours"]
    resource_group: Literal["live", "serving", "research", "maintenance"]

    @model_validator(mode="after")
    def validate_pair(self) -> OpsUnitInstall:
        if not _allowed_unit(self.timer, "timer") or not _allowed_unit(self.service, "service"):
            raise ValueError("unit is outside the ops allowlist")
        if self.service != self.timer.removesuffix(".timer") + ".service":
            raise ValueError("timer and service must be an exact pair")
        return self


class OpsInstallManifest(RuntimeContractModel):
    version: Literal[1]
    host_name: StrictStr = Field(min_length=1, max_length=253)
    units: tuple[OpsUnitInstall, ...] = Field(max_length=_MAX_UNIT_COUNT)

    @model_validator(mode="after")
    def validate_set(self) -> OpsInstallManifest:
        timers = tuple(item.timer for item in self.units)
        if len(timers) != len(set(timers)):
            raise ValueError("ops install manifest contains duplicate timers")
        if not _STATIC_TIMERS.issubset(timers):
            raise ValueError("ops install manifest requires the exact static timer set")
        return self

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))

    def signing_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json"))


class SignedOpsInstallManifest(RuntimeContractModel):
    manifest: OpsInstallManifest
    signature: StrictStr = Field(min_length=88, max_length=88)

    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json"))


def verify_ops_manifest(
    signed: SignedOpsInstallManifest,
    *,
    public_key_pem: bytes,
    expected_host: str,
) -> tuple[OpsInstallManifest, str]:
    if signed.manifest.host_name != expected_host:
        raise ValueError("ops install manifest host does not match collector")
    try:
        signature = base64.b64decode(signed.signature, validate=True)
    except (ValueError, base64.binascii.Error) as exc:
        raise ValueError("ops install manifest signature is invalid") from exc
    if not verify_ed25519_signature(
        public_key_pem=public_key_pem,
        message=signed.manifest.signing_bytes(),
        signature=signature,
    ):
        raise ValueError("ops install manifest signature is invalid")
    return signed.manifest, signed.manifest.digest


def load_signed_ops_manifest(
    path: Path,
    *,
    public_key_pem: bytes,
    expected_host: str,
) -> tuple[OpsInstallManifest, str]:
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
    )
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_size > _MAX_MANIFEST_BYTES:
            raise ValueError("ops install manifest is not a bounded regular file")
        payload = os.read(descriptor, _MAX_MANIFEST_BYTES + 1)
        after = os.fstat(descriptor)
        if len(payload) > _MAX_MANIFEST_BYTES or (
            before.st_dev,
            before.st_ino,
            before.st_mtime_ns,
            before.st_size,
        ) != (after.st_dev, after.st_ino, after.st_mtime_ns, after.st_size):
            raise ValueError("ops install manifest changed during read or exceeds byte budget")
    finally:
        os.close(descriptor)
    document = strict_canonical_json_loads(payload)
    signed = SignedOpsInstallManifest.model_validate(document)
    if signed.canonical_bytes() != payload:
        raise ValueError("ops install manifest is not canonical")
    return verify_ops_manifest(signed, public_key_pem=public_key_pem, expected_host=expected_host)


def _run_bounded(argv: tuple[str, ...], timeout_seconds: float, max_bytes: int) -> bytes:
    process = subprocess.Popen(
        argv,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        env={"LC_ALL": "C", "TZ": "UTC", "SYSTEMD_PAGER": ""},
    )
    assert process.stdout is not None
    deadline = time.monotonic() + timeout_seconds
    output = bytearray()
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("systemctl show timed out")
                if not selector.select(remaining):
                    raise TimeoutError("systemctl show timed out")
                chunk = os.read(process.stdout.fileno(), min(4096, max_bytes + 1 - len(output)))
                if not chunk:
                    break
                output.extend(chunk)
                if len(output) > max_bytes:
                    raise ValueError("systemctl show exceeded byte budget")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("systemctl show timed out")
        if process.wait(timeout=remaining) != 0:
            raise ValueError("systemctl show failed")
        return bytes(output)
    finally:
        if process.poll() is None:
            process.kill()
        process.wait()
        process.stdout.close()


def _bounded_proc_read(path: str, max_bytes: int) -> bytes:
    if path not in (_BOOT_PATH, _MEMINFO_PATH, _STAT_PATH):
        raise ValueError("proc path is outside the ops allowlist")
    with open(path, "rb") as source:
        payload = source.read(max_bytes + 1)
    if len(payload) > max_bytes:
        raise ValueError("proc read exceeded byte budget")
    return payload


def _properties(payload: bytes, kind: str) -> Mapping[str, str]:
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("systemctl show output is not UTF-8") from exc
    result: dict[str, str] = {}
    allowed = set(_PROPERTIES[kind])
    for line in text.splitlines():
        key, separator, value = line.partition("=")
        if not separator or key not in allowed or key in result or len(value) > 512:
            raise ValueError("systemctl show output contains invalid properties")
        result[key] = value
    return MappingProxyType(result)


def _timestamp(value: str | None) -> datetime | None:
    if not value or value == "n/a":
        return None
    match = _TIMESTAMP.fullmatch(value)
    if match is None:
        return None
    try:
        return (
            datetime.fromisoformat(f"{match['date']}T{match['clock']}")
            .replace(tzinfo=UTC)
            .astimezone(UTC)
        )
    except ValueError:
        return None


def _memory_value(value: str | None) -> int | None:
    if value is None or not value.isdecimal():
        return None
    parsed = int(value)
    return parsed if 0 <= parsed <= 2**63 - 1 else None


def _host_memory(payload: bytes) -> tuple[int | None, int | None]:
    values: dict[str, int] = {}
    for line in payload.decode("ascii").splitlines():
        match = re.fullmatch(r"(MemTotal|MemAvailable):\s+(\d+) kB", line)
        if match is not None:
            if match[1] in values:
                raise ValueError("proc meminfo contains duplicate memory fields")
            values[match[1]] = int(match[2]) * 1024
    total = values.get("MemTotal")
    available = values.get("MemAvailable")
    if total is not None and available is not None and available > total:
        raise ValueError("proc meminfo available memory exceeds total")
    return total, available


class OpsUnitEvidence(RuntimeContractModel):
    timer: StrictStr
    service: StrictStr
    label: StrictStr
    expected_enabled: StrictBool
    session: Literal["all", "trading_day", "market_hours"]
    resource_group: Literal["live", "serving", "research", "maintenance"]
    timer_load_state: StrictStr | None = None
    timer_unit_file_state: StrictStr | None = None
    timer_active_state: StrictStr | None = None
    timer_sub_state: StrictStr | None = None
    last_trigger_at: AwareUtcDatetime | None = None
    next_at: AwareUtcDatetime | None = None
    service_load_state: StrictStr | None = None
    service_active_state: StrictStr | None = None
    service_sub_state: StrictStr | None = None
    service_result: StrictStr | None = None
    service_invocation_id: StrictStr | None = None
    service_exec_status: StrictStr | None = None
    service_start_at: AwareUtcDatetime | None = None
    service_exit_at: AwareUtcDatetime | None = None
    last_result: Literal["success", "failure"] | None = None

    @model_validator(mode="after")
    def refuse_unattributed_result(self) -> OpsUnitEvidence:
        if self.last_result is not None:
            raise ValueError("timer result requires a trusted trigger receipt")
        return self


class OpsResourceEvidence(RuntimeContractModel):
    slice_name: StrictStr
    load_state: StrictStr | None = None
    active_state: StrictStr | None = None
    memory_current_bytes: StrictInt | None = Field(default=None, ge=0)
    memory_peak_bytes: StrictInt | None = Field(default=None, ge=0)


class OpsHostCpuCounters(RuntimeContractModel):
    user: StrictInt = Field(ge=0, le=2**64 - 1)
    nice: StrictInt = Field(ge=0, le=2**64 - 1)
    system: StrictInt = Field(ge=0, le=2**64 - 1)
    idle: StrictInt = Field(ge=0, le=2**64 - 1)
    iowait: StrictInt = Field(ge=0, le=2**64 - 1)
    irq: StrictInt = Field(ge=0, le=2**64 - 1)
    softirq: StrictInt = Field(ge=0, le=2**64 - 1)
    steal: StrictInt = Field(ge=0, le=2**64 - 1)
    guest: StrictInt | None = Field(default=None, ge=0, le=2**64 - 1)
    guest_nice: StrictInt | None = Field(default=None, ge=0, le=2**64 - 1)

    @model_validator(mode="after")
    def validate_guest(self) -> OpsHostCpuCounters:
        if self.guest is not None and self.guest > self.user:
            raise ValueError("host CPU guest time exceeds its included user time")
        if self.guest_nice is not None and (self.guest is None or self.guest_nice > self.nice):
            raise ValueError("host CPU guest nice time exceeds its included nice time")
        return self


_CPU_COUNTER_FIELDS = ("user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal")


def _host_cpu_counters(payload: bytes) -> OpsHostCpuCounters:
    if len(payload) > _MAX_CPU_STAT_BYTES:
        raise ValueError("host CPU proc stat exceeds byte budget")
    rows = [
        line.split() for line in payload.decode("ascii").splitlines() if line.split()[:1] == ["cpu"]
    ]
    if len(rows) != 1 or not 9 <= len(rows[0]) <= 11:
        raise ValueError("host CPU requires one exact aggregate counter row")
    values = rows[0][1:]
    if any(not re.fullmatch(r"[0-9]{1,20}", value) for value in values):
        raise ValueError("host CPU counters must be bounded unsigned integers")
    names = (*_CPU_COUNTER_FIELDS, "guest", "guest_nice")
    return OpsHostCpuCounters.model_validate(
        dict(zip(names[: len(values)], map(int, values), strict=True))
    )


class OpsHostCpuObservation(RuntimeContractModel):
    host_name: StrictStr = Field(min_length=1, max_length=253)
    boot_id: StrictStr = Field(
        pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    )
    manifest_digest: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")
    parser_contract: Literal["linux-proc-stat-aggregate/v1"] = "linux-proc-stat-aggregate/v1"
    observed_at: AwareUtcDatetime
    monotonic_seconds: StrictFloat = Field(ge=0, allow_inf_nan=False)
    counters: OpsHostCpuCounters


def _host_cpu_pair_result(
    previous: OpsHostCpuObservation | None,
    current: OpsHostCpuObservation | None,
) -> tuple[str, int | None, int | None, Decimal | None]:
    if previous is None or current is None:
        return "incomplete_pair", None, None, None
    if (
        previous.host_name,
        previous.boot_id,
        previous.manifest_digest,
        previous.parser_contract,
    ) != (current.host_name, current.boot_id, current.manifest_digest, current.parser_contract):
        return "identity_mismatch", None, None, None
    wall = (current.observed_at - previous.observed_at).total_seconds()
    monotonic = current.monotonic_seconds - previous.monotonic_seconds
    if not (0 < wall <= _TOTAL_SECONDS and 0 < monotonic <= _TOTAL_SECONDS):
        return "invalid_window", None, None, None
    for field in ("guest", "guest_nice"):
        old, new = getattr(previous.counters, field), getattr(current.counters, field)
        if (old is None) != (new is None):
            return "counter_contract_changed", None, None, None
        if old is not None and new < old:
            return "counter_regression", None, None, None
    deltas = {
        field: getattr(current.counters, field) - getattr(previous.counters, field)
        for field in _CPU_COUNTER_FIELDS
    }
    if any(value < 0 for value in deltas.values()):
        # iowait may regress in the kernel; its lost window remains unknown.
        return "counter_regression", None, None, None
    total = sum(deltas.values())
    if total == 0:
        return "no_counter_delta", None, None, None
    busy = total - deltas["idle"] - deltas["iowait"]
    # user/nice already contain guest; steal is declared non-idle host time.
    with localcontext() as context:
        context.prec = 28
        fraction = Decimal(busy) / Decimal(total)
    return "available", total, busy, fraction


class OpsHostCpuEvidence(RuntimeContractModel):
    previous: OpsHostCpuObservation | None = None
    current: OpsHostCpuObservation | None = None
    availability: Literal["available", "unavailable"]
    reason_code: Literal[
        "available",
        "capture_unavailable",
        "incomplete_pair",
        "invalid_window",
        "identity_mismatch",
        "counter_regression",
        "counter_contract_changed",
        "no_counter_delta",
    ]
    total_delta: StrictInt | None = Field(default=None, gt=0)
    busy_delta: StrictInt | None = Field(default=None, ge=0)
    busy_fraction: Decimal | None = Field(default=None, ge=0, le=1, allow_inf_nan=False)

    @field_validator("busy_fraction", mode="before")
    @classmethod
    def reject_bool(cls, value: object) -> object:
        if isinstance(value, bool):
            raise ValueError("host CPU fraction cannot be bool")
        return value

    @model_validator(mode="after")
    def validate_pair(self) -> OpsHostCpuEvidence:
        raw_pair = {
            "previous": None if self.previous is None else self.previous.model_dump(mode="json"),
            "current": None if self.current is None else self.current.model_dump(mode="json"),
        }
        if len(canonical_json_bytes(raw_pair)) > _MAX_CPU_PAIR_BYTES:
            raise ValueError("host CPU raw pair exceeds original 4 KiB budget")
        reason, total, busy, fraction = _host_cpu_pair_result(self.previous, self.current)
        if reason == "available":
            if (
                self.availability,
                self.reason_code,
                self.total_delta,
                self.busy_delta,
                self.busy_fraction,
            ) != ("available", reason, total, busy, fraction):
                raise ValueError("host CPU result does not match its original raw pair")
        elif (
            self.availability != "unavailable"
            or any(
                value is not None
                for value in (self.total_delta, self.busy_delta, self.busy_fraction)
            )
            or (
                self.reason_code != reason
                and not (reason == "incomplete_pair" and self.reason_code == "capture_unavailable")
            )
        ):
            raise ValueError("unavailable host CPU pair cannot carry a value or a different reason")
        return self

    @classmethod
    def from_samples(
        cls,
        previous: OpsHostCpuObservation | None,
        current: OpsHostCpuObservation | None,
        *,
        capture_failed: bool = False,
    ) -> OpsHostCpuEvidence:
        reason, total, busy, fraction = _host_cpu_pair_result(previous, current)
        if capture_failed and reason == "incomplete_pair":
            reason = "capture_unavailable"
        return cls(
            previous=previous,
            current=current,
            availability="available" if reason == "available" else "unavailable",
            reason_code=reason,
            total_delta=total,
            busy_delta=busy,
            busy_fraction=fraction,
        )


class OpsSnapshot(RuntimeContractModel):
    sampled_at: AwareUtcDatetime
    host_name: StrictStr
    boot_id: StrictStr
    manifest_digest: StrictStr = Field(pattern=r"^[0-9a-f]{64}$")
    host_memory_total_bytes: StrictInt | None = Field(default=None, ge=0)
    host_memory_available_bytes: StrictInt | None = Field(default=None, ge=0)
    units: tuple[OpsUnitEvidence, ...] = Field(max_length=_MAX_UNIT_COUNT)
    resources: tuple[OpsResourceEvidence, ...] = Field(max_length=len(_SLICES))
    host_cpu: OpsHostCpuEvidence | None = Field(
        default=None, exclude_if=lambda value: value is None
    )

    @model_validator(mode="after")
    def validate_set(self) -> OpsSnapshot:
        timers = tuple(item.timer for item in self.units)
        if len(set(timers)) != len(timers):
            raise ValueError("ops snapshot has duplicate timers")
        if not _STATIC_TIMERS.issubset(timers):
            raise ValueError("ops snapshot requires the exact static timer set")
        for item in self.units:
            if (
                not _allowed_unit(item.timer, "timer")
                or item.service != item.timer.removesuffix(".timer") + ".service"
            ):
                raise ValueError("ops snapshot unit is outside the allowlist or mismatched")
        if tuple(item.slice_name for item in self.resources) != _SLICES:
            raise ValueError("ops snapshot requires fixed resource slices")
        if self.host_cpu is not None:
            for sample in (self.host_cpu.previous, self.host_cpu.current):
                if sample is not None and (
                    (sample.host_name, sample.boot_id, sample.manifest_digest)
                    != (self.host_name, self.boot_id, self.manifest_digest)
                    or sample.observed_at > self.sampled_at
                ):
                    raise ValueError("host CPU observation is detached from its Ops snapshot")
        return self


CommandRunner = Callable[[tuple[str, ...], float, int], bytes]
ProcReader = Callable[[str, int], bytes]


class OpsStatusCollector:
    def __init__(
        self,
        *,
        command_runner: CommandRunner = _run_bounded,
        proc_reader: ProcReader = _bounded_proc_read,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        monotonic: Callable[[], float] = time.monotonic,
        host_name: Callable[[], str] = socket.gethostname,
        observe_host_cpu: bool = False,
    ) -> None:
        if type(observe_host_cpu) is not bool:
            raise TypeError("host CPU opt-in must be an explicit boolean")
        self.command_runner = command_runner
        self.proc_reader = proc_reader
        self.clock = clock
        self.monotonic = monotonic
        self.host_name = host_name
        self.observe_host_cpu = observe_host_cpu

    def collect_tasks(
        self,
        manifest: OpsInstallManifest,
        *,
        previous: TaskCpuObservation | None,
        cpu_reader: LinuxTaskCpuReader,
        run_source: TaskUnitRunSource | None = None,
    ) -> TaskOpsSample:
        from rquant.task_center_projection import TaskOpsEvidence, TaskOpsSample
        from rquant.task_center_runtime import TaskUnitRunSource
        from rquant.task_cpu import LinuxTaskCpuReader, TaskCpuPair, compute_task_cpu

        if type(cpu_reader) is not LinuxTaskCpuReader:
            raise TypeError("task CPU collection requires the exact fixed kernel reader")
        if run_source is not None and type(run_source) is not TaskUnitRunSource:
            raise TypeError("task run collection requires the exact original journal reader")
        started = self.monotonic()
        snapshot = self.collect(manifest)
        runs = ()
        if run_source is not None:
            try:
                runs = run_source.read(
                    host_name=snapshot.host_name,
                    boot_id=snapshot.boot_id,
                    manifest_digest=snapshot.manifest_digest,
                    units=tuple(unit.service for unit in snapshot.units),
                    cutoff=self.clock(),
                )
            except (OSError, ValueError, TimeoutError):
                runs = ()
        try:
            properties = {name: self._show_task_cpu(name, start=started) for name in _SLICES}
            remaining = _TOTAL_SECONDS - (self.monotonic() - started)
            current = cpu_reader.capture(
                host_name=snapshot.host_name,
                boot_id=snapshot.boot_id,
                manifest_digest=snapshot.manifest_digest,
                properties=properties,
                max_seconds=remaining,
                clock=self.clock,
            )
            if self.monotonic() - started >= _TOTAL_SECONDS:
                raise TimeoutError("task CPU exceeded original collection deadline")
            if (
                self.host_name() != snapshot.host_name
                or self.proc_reader(_BOOT_PATH, 128).decode("ascii").strip() != snapshot.boot_id
            ):
                raise ValueError("ops host or boot changed during task collection")
            at = self.clock()
            snapshot = OpsSnapshot.model_validate(
                snapshot.model_dump(mode="python") | {"sampled_at": at}
            )
            pair = TaskCpuPair(previous=previous, current=current)
            cpu = compute_task_cpu(pair, cutoff=at)
            return TaskOpsSample(snapshot=snapshot, evidence=TaskOpsEvidence(cpu=cpu, runs=runs))
        except (OSError, ValueError, TimeoutError, AttributeError) as exc:
            reason = "budget_exceeded" if isinstance(exc, TimeoutError) else "capture_unavailable"
            snapshot = OpsSnapshot.model_validate(
                snapshot.model_dump() | {"sampled_at": self.clock()}
            )
            return TaskOpsSample(
                snapshot=snapshot,
                evidence=TaskOpsEvidence(cpu=None, cpu_unavailable_reason=reason, runs=runs),
            )

    def _show_task_cpu(self, unit: str, *, start: float) -> Mapping[str, str]:
        if unit not in _SLICES:
            raise ValueError("CPU unit is outside the exact fixed slices")
        remaining = _TOTAL_SECONDS - (self.monotonic() - start)
        if remaining <= 0:
            raise TimeoutError("ops collection exceeded total time budget")
        allowed = ("LoadState", "ControlGroup", "InvocationID")
        argv = ("/usr/bin/systemctl", "show", unit, "--no-pager", "--property=" + ",".join(allowed))
        payload = self.command_runner(
            argv, min(_PER_COMMAND_SECONDS, remaining), _MAX_COMMAND_BYTES
        )
        if len(payload) > _MAX_COMMAND_BYTES:
            raise ValueError("CPU systemctl show exceeded byte budget")
        result: dict[str, str] = {}
        for line in payload.decode("ascii").splitlines():
            key, separator, value = line.partition("=")
            if not separator or key not in allowed or key in result or len(value) > 512:
                raise ValueError("CPU systemctl show contains invalid fixed properties")
            result[key] = value
        if self.monotonic() - start >= _TOTAL_SECONDS:
            raise TimeoutError("ops collection exceeded total time budget")
        return MappingProxyType(result)

    def collect(self, manifest: OpsInstallManifest) -> OpsSnapshot:
        manifest = OpsInstallManifest.model_validate(manifest)
        if self.host_name() != manifest.host_name:
            raise ValueError("ops install manifest host does not match collector")
        start = self.monotonic()
        first_boot = self.proc_reader(_BOOT_PATH, 128).decode("ascii").strip()
        if not re.fullmatch(r"[0-9a-f-]{36}", first_boot):
            raise ValueError("ops boot identity is invalid")
        previous = current = None
        capture_failed = False
        if self.observe_host_cpu:
            try:
                previous = self._capture_host_cpu(manifest, first_boot, start=start)
            except (OSError, ValueError, TimeoutError):
                capture_failed = True
        meminfo = self.proc_reader(_MEMINFO_PATH, 128 * 1024)
        if len(meminfo) > 128 * 1024:
            raise ValueError("proc read exceeded byte budget")
        total, available = _host_memory(meminfo)
        units: list[OpsUnitEvidence] = []
        for installed in manifest.units:
            timer = self._show(installed.timer, "timer", start=start)
            service = self._show(installed.service, "service", start=start)
            units.append(
                OpsUnitEvidence(
                    **installed.model_dump(),
                    timer_load_state=timer.get("LoadState"),
                    timer_unit_file_state=timer.get("UnitFileState"),
                    timer_active_state=timer.get("ActiveState"),
                    timer_sub_state=timer.get("SubState"),
                    last_trigger_at=_timestamp(timer.get("LastTriggerUSec")),
                    next_at=_timestamp(timer.get("NextElapseUSecRealtime")),
                    service_load_state=service.get("LoadState"),
                    service_active_state=service.get("ActiveState"),
                    service_sub_state=service.get("SubState"),
                    service_result=service.get("Result"),
                    service_invocation_id=service.get("InvocationID"),
                    service_exec_status=service.get("ExecMainStatus"),
                    service_start_at=_timestamp(service.get("ExecMainStartTimestamp")),
                    service_exit_at=_timestamp(service.get("ExecMainExitTimestamp")),
                )
            )
        resources = tuple(
            OpsResourceEvidence(
                slice_name=slice_name,
                load_state=(properties := self._show(slice_name, "slice", start=start)).get(
                    "LoadState"
                ),
                active_state=properties.get("ActiveState"),
                memory_current_bytes=_memory_value(properties.get("MemoryCurrent")),
                memory_peak_bytes=_memory_value(properties.get("MemoryPeak")),
            )
            for slice_name in _SLICES
        )
        if self.observe_host_cpu:
            try:
                current = self._capture_host_cpu(manifest, first_boot, start=start)
            except (OSError, ValueError, TimeoutError):
                capture_failed = True
            if self.host_name() != manifest.host_name:
                raise ValueError("ops host identity changed during collection")
        last_boot = self.proc_reader(_BOOT_PATH, 128).decode("ascii").strip()
        if first_boot != last_boot:
            raise ValueError("ops boot identity changed during collection")
        if self.monotonic() - start > _TOTAL_SECONDS:
            raise TimeoutError("ops collection exceeded total time budget")
        return OpsSnapshot(
            sampled_at=self.clock(),
            host_name=manifest.host_name,
            boot_id=first_boot,
            manifest_digest=manifest.digest,
            host_memory_total_bytes=total,
            host_memory_available_bytes=available,
            units=tuple(units),
            resources=resources,
            host_cpu=OpsHostCpuEvidence.from_samples(
                previous, current, capture_failed=capture_failed
            )
            if self.observe_host_cpu
            else None,
        )

    def _capture_host_cpu(
        self,
        manifest: OpsInstallManifest,
        boot_id: str,
        *,
        start: float,
    ) -> OpsHostCpuObservation:
        if self.monotonic() - start >= _TOTAL_SECONDS:
            raise TimeoutError("host CPU exceeded original collection deadline")
        counters = _host_cpu_counters(self.proc_reader(_STAT_PATH, _MAX_CPU_STAT_BYTES))
        monotonic = self.monotonic()
        if monotonic - start > _TOTAL_SECONDS:
            raise TimeoutError("host CPU exceeded original collection deadline")
        return OpsHostCpuObservation(
            host_name=self.host_name(),
            boot_id=boot_id,
            manifest_digest=manifest.digest,
            observed_at=self.clock(),
            monotonic_seconds=monotonic,
            counters=counters,
        )

    def _show(
        self,
        unit: str,
        kind: Literal["timer", "service", "slice"],
        *,
        start: float,
    ) -> Mapping[str, str]:
        if not _allowed_unit(unit, kind):
            raise ValueError("unit is outside the ops allowlist")
        remaining = _TOTAL_SECONDS - (self.monotonic() - start)
        if remaining <= 0:
            raise TimeoutError("ops collection exceeded total time budget")
        timeout = min(_PER_COMMAND_SECONDS, remaining)
        argv = (
            "/usr/bin/systemctl",
            "show",
            unit,
            "--no-pager",
            "--property=" + ",".join(_PROPERTIES[kind]),
        )
        payload = self.command_runner(argv, timeout, _MAX_COMMAND_BYTES)
        if len(payload) > _MAX_COMMAND_BYTES:
            raise ValueError("systemctl show exceeded byte budget")
        if self.monotonic() - start > _TOTAL_SECONDS:
            raise TimeoutError("ops collection exceeded total time budget")
        return _properties(payload, kind)


__all__ = [
    "STATIC_TIMER_STEMS",
    "OpsInstallManifest",
    "OpsHostCpuCounters",
    "OpsHostCpuObservation",
    "OpsHostCpuEvidence",
    "OpsResourceEvidence",
    "OpsSnapshot",
    "OpsStatusCollector",
    "OpsUnitEvidence",
    "OpsUnitInstall",
    "SignedOpsInstallManifest",
    "load_signed_ops_manifest",
    "verify_ops_manifest",
]
