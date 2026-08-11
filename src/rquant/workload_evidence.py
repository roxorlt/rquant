"""Append-only cloud workload samples and deterministic high-water summaries."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import os
import stat
import subprocess
import time
from collections.abc import Callable, Sequence
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads
from rquant.workload_isolation import (
    DEFAULT_HIGH_WATER_EVIDENCE_PATH,
    DEFAULT_HIGH_WATER_RAW_EVIDENCE_PATH,
    WorkloadHighWaterEvidence,
)

_MIB = 1024**2
_SAMPLE_CADENCE_SECONDS = 5 * 60
_MAX_SAMPLE_GAP_SECONDS = 7 * 60 + 30
_MIN_WINDOW_SECONDS = 24 * 60 * 60
_MIN_WINDOW_SAMPLE_COUNT = _MIN_WINDOW_SECONDS // _SAMPLE_CADENCE_SECONDS + 1
_SYSTEMCTL = Path("/usr/bin/systemctl")
_MEMINFO = Path("/proc/meminfo")
_BOOT_ID = Path("/proc/sys/kernel/random/boot_id")
_BOOT_ID_PATTERN = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
_UNIT_PROPERTIES = (
    "LoadState,ActiveState,Result,ExecMainStatus,InvocationID,MemoryCurrent,MemoryPeak,"
    "ExecMainStartTimestampMonotonic,ExecMainExitTimestampMonotonic"
)
_SLICE_PROPERTIES = "MemoryCurrent,MemoryPeak"


class WorkloadEvidenceError(RuntimeError):
    """Evidence could not be safely appended or summarized."""


class UnitWorkloadSample(RuntimeContractModel):
    invocation_id: str
    load_state: str
    active_state: str
    result: str
    exec_main_status: int = Field(strict=True)
    memory_current_mib: int = Field(strict=True, ge=0)
    memory_peak_mib: int = Field(strict=True, ge=0)
    successful_runtime_seconds: int = Field(strict=True, ge=0)

    @property
    def is_successful_run(self) -> bool:
        return (
            bool(self.invocation_id)
            and self.result == "success"
            and self.exec_main_status == 0
            and self.successful_runtime_seconds > 0
        )


class WorkloadSample(RuntimeContractModel):
    schema_version: Literal[2]
    sampled_at: AwareUtcDatetime
    boot_id: str = Field(pattern=_BOOT_ID_PATTERN)
    clock_boottime_ns: int = Field(strict=True, gt=0)
    host_mem_total_mib: int = Field(strict=True, gt=0)
    mem_available_mib: int = Field(strict=True, ge=0)
    live_current_mib: int = Field(strict=True, ge=0)
    live_peak_mib: int = Field(strict=True, ge=0)
    serving_current_mib: int = Field(strict=True, ge=0)
    serving_peak_mib: int = Field(strict=True, ge=0)
    maintenance_current_mib: int = Field(strict=True, ge=0)
    maintenance_peak_mib: int = Field(strict=True, ge=0)
    os_system_slice_current_mib: int = Field(strict=True, ge=0)
    os_system_slice_peak_mib: int = Field(strict=True, gt=0)
    monitor: UnitWorkloadSample
    backup: UnitWorkloadSample
    replica: UnitWorkloadSample

    @model_validator(mode="after")
    def validate_peaks(self) -> WorkloadSample:
        pairs = (
            (self.live_current_mib, self.live_peak_mib, "live"),
            (self.serving_current_mib, self.serving_peak_mib, "serving"),
            (self.maintenance_current_mib, self.maintenance_peak_mib, "maintenance"),
            (
                self.os_system_slice_current_mib,
                self.os_system_slice_peak_mib,
                "os/system.slice",
            ),
        )
        for current, peak, name in pairs:
            if current > peak:
                raise ValueError(f"{name} current memory exceeds peak")
        return self


class ChainedWorkloadSample(RuntimeContractModel):
    schema_version: Literal[1]
    sequence: int = Field(strict=True, gt=0)
    previous_record_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    sample: WorkloadSample
    record_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


def _record_payload(record: ChainedWorkloadSample) -> dict[str, object]:
    return {
        "schema_version": record.schema_version,
        "sequence": record.sequence,
        "previous_record_sha256": record.previous_record_sha256,
        "sample": record.sample.model_dump(mode="json"),
    }


def _record_sha256(payload: dict[str, object]) -> str:
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _decode_records(raw: bytes) -> tuple[ChainedWorkloadSample, ...]:
    records: list[ChainedWorkloadSample] = []
    expected_previous = "0" * 64
    for expected_sequence, line in enumerate(raw.splitlines(keepends=True), start=1):
        if not line.endswith(b"\n"):
            raise WorkloadEvidenceError("workload evidence has a partial final record")
        try:
            decoded = strict_canonical_json_loads(line, trailing_newline=True)
            record = ChainedWorkloadSample.model_validate(decoded)
        except ValueError as exc:
            raise WorkloadEvidenceError(
                f"invalid canonical workload evidence record: {exc}"
            ) from exc
        if record.sequence != expected_sequence:
            raise WorkloadEvidenceError("workload evidence sequence is not contiguous")
        if record.previous_record_sha256 != expected_previous:
            raise WorkloadEvidenceError("workload evidence hash chain is broken")
        observed_sha = _record_sha256(_record_payload(record))
        if record.record_sha256 != observed_sha:
            raise WorkloadEvidenceError("workload evidence record sha256 is invalid")
        records.append(record)
        expected_previous = record.record_sha256
    return tuple(records)


def _ensure_safe_regular_file(path: Path, *, allow_missing: bool) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        if allow_missing:
            return
        raise WorkloadEvidenceError(f"evidence file is missing: {path}") from None
    except OSError as exc:
        raise WorkloadEvidenceError(f"cannot inspect evidence file: {exc}") from exc
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
        raise WorkloadEvidenceError(f"unsafe evidence target: {path}")


def append_workload_sample(path: Path, sample: WorkloadSample) -> None:
    """Append and fsync one canonical hash-chain record without following symlinks."""

    target = Path(path)
    _ensure_safe_regular_file(target, allow_missing=True)
    flags = os.O_RDWR | os.O_APPEND | os.O_CREAT
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(target, flags, 0o640)
    except OSError as exc:
        raise WorkloadEvidenceError(f"unsafe evidence append failed: {exc}") from exc
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            raise WorkloadEvidenceError(f"unsafe evidence target: {target}")
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        os.lseek(descriptor, 0, os.SEEK_SET)
        chunks: list[bytes] = []
        while chunk := os.read(descriptor, 1024 * 1024):
            chunks.append(chunk)
        existing = b"".join(chunks)
        records = _decode_records(existing)
        previous = records[-1].record_sha256 if records else "0" * 64
        payload_fields: dict[str, object] = {
            "schema_version": 1,
            "sequence": len(records) + 1,
            "previous_record_sha256": previous,
            "sample": sample.model_dump(mode="json"),
        }
        record = {
            **payload_fields,
            "record_sha256": _record_sha256(payload_fields),
        }
        payload = canonical_json_bytes(record, trailing_newline=True)
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _load_samples(path: Path) -> tuple[bytes, tuple[WorkloadSample, ...]]:
    target = Path(path)
    _ensure_safe_regular_file(target, allow_missing=False)
    try:
        raw = target.read_bytes()
    except OSError as exc:
        raise WorkloadEvidenceError(f"cannot read workload evidence: {exc}") from exc
    records = _decode_records(raw)
    if not records:
        raise WorkloadEvidenceError("workload evidence is empty")
    return raw, tuple(record.sample for record in records)


def _continuous_boot_segments(
    samples: Sequence[WorkloadSample],
) -> tuple[tuple[WorkloadSample, ...], ...]:
    segments: list[list[WorkloadSample]] = []
    seen_boot_ids: set[str] = set()
    previous: WorkloadSample | None = None
    for sample in samples:
        if previous is not None and sample.sampled_at <= previous.sampled_at:
            raise WorkloadEvidenceError(
                "workload evidence wall clock is not strictly increasing"
            )
        if previous is None or sample.boot_id != previous.boot_id:
            if sample.boot_id in seen_boot_ids:
                raise WorkloadEvidenceError(
                    "workload evidence boot id appears in non-contiguous segments"
                )
            seen_boot_ids.add(sample.boot_id)
            segments.append([sample])
        else:
            if sample.clock_boottime_ns <= previous.clock_boottime_ns:
                raise WorkloadEvidenceError(
                    "workload evidence monotonic clock is not strictly increasing"
                )
            wall_gap = (sample.sampled_at - previous.sampled_at).total_seconds()
            monotonic_gap = (
                sample.clock_boottime_ns - previous.clock_boottime_ns
            ) / 1_000_000_000
            if (
                wall_gap > _MAX_SAMPLE_GAP_SECONDS
                or monotonic_gap > _MAX_SAMPLE_GAP_SECONDS
            ):
                raise WorkloadEvidenceError(
                    "workload evidence sampling gap exceeds 450 seconds: "
                    f"wall={wall_gap:.3f}s monotonic={monotonic_gap:.3f}s"
                )
            segments[-1].append(sample)
        previous = sample
    return tuple(tuple(segment) for segment in segments)


def _latest_complete_segment(
    samples: Sequence[WorkloadSample],
) -> tuple[WorkloadSample, ...]:
    complete: list[tuple[WorkloadSample, ...]] = []
    for segment in _continuous_boot_segments(samples):
        first = segment[0]
        last = segment[-1]
        wall_seconds = (last.sampled_at - first.sampled_at).total_seconds()
        monotonic_seconds = (
            last.clock_boottime_ns - first.clock_boottime_ns
        ) / 1_000_000_000
        if (
            wall_seconds >= _MIN_WINDOW_SECONDS
            and monotonic_seconds >= _MIN_WINDOW_SECONDS
            and len(segment) >= _MIN_WINDOW_SAMPLE_COUNT
        ):
            complete.append(segment)
    if not complete:
        raise WorkloadEvidenceError(
            "workload evidence has no continuous same-boot window spanning 24 hours "
            "with "
            f"at least {_MIN_WINDOW_SAMPLE_COUNT} samples at "
            f"{_SAMPLE_CADENCE_SECONDS}-second cadence"
        )
    return complete[-1]


def _successful_runs(
    samples: Sequence[WorkloadSample],
    name: Literal["backup", "replica"],
) -> tuple[int, int]:
    durations: dict[str, int] = {}
    for sample in samples:
        unit = getattr(sample, name)
        if unit.is_successful_run:
            durations[unit.invocation_id] = max(
                durations.get(unit.invocation_id, 0),
                unit.successful_runtime_seconds,
            )
    return len(durations), sum(durations.values())


def summarize_workload_samples(path: Path) -> WorkloadHighWaterEvidence:
    """Build the strict admission summary from an immutable read of raw samples."""

    raw, raw_samples = _load_samples(path)
    samples = _latest_complete_segment(raw_samples)
    first = samples[0].sampled_at.astimezone(UTC)
    last = samples[-1].sampled_at.astimezone(UTC)
    window_seconds = int((last - first).total_seconds())
    backup_runs, backup_runtime = _successful_runs(samples, "backup")
    replica_runs, replica_runtime = _successful_runs(samples, "replica")
    if backup_runs == 0 or replica_runs == 0:
        raise WorkloadEvidenceError("backup and replica require non-zero successful runs")

    latest = samples[-1]
    return WorkloadHighWaterEvidence(
        schema_version=1,
        observed_at=last,
        observation_window_hours=window_seconds // 3600,
        host_mem_total_mib=min(sample.host_mem_total_mib for sample in samples),
        monitor_current_mib=latest.monitor.memory_current_mib,
        monitor_peak_mib=max(sample.monitor.memory_peak_mib for sample in samples),
        live_concurrent_peak_mib=max(sample.live_peak_mib for sample in samples),
        serving_concurrent_peak_mib=max(
            sample.serving_peak_mib for sample in samples
        ),
        backup_peak_mib=max(sample.backup.memory_peak_mib for sample in samples),
        replica_peak_mib=max(sample.replica.memory_peak_mib for sample in samples),
        maintenance_concurrent_peak_mib=max(
            sample.maintenance_peak_mib for sample in samples
        ),
        backup_successful_runs=backup_runs,
        replica_successful_runs=replica_runs,
        backup_sample_count=len(samples),
        replica_sample_count=len(samples),
        backup_successful_runtime_seconds=backup_runtime,
        replica_successful_runtime_seconds=replica_runtime,
        raw_evidence_sha256=hashlib.sha256(raw).hexdigest(),
        os_system_slice_peak_mib=max(
            sample.os_system_slice_peak_mib for sample in samples
        ),
        min_mem_available_mib=min(sample.mem_available_mib for sample in samples),
    )


def _properties(stdout: str) -> dict[str, str]:
    return {
        key: value
        for line in stdout.splitlines()
        if "=" in line
        for key, value in (line.split("=", 1),)
    }


def _show(
    unit: str,
    properties: str,
    *,
    runner: Callable[..., subprocess.CompletedProcess[str]],
) -> dict[str, str]:
    try:
        completed = runner(
            [
                str(_SYSTEMCTL),
                "show",
                unit,
                "--no-pager",
                f"--property={properties}",
            ],
            check=False,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=8,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise WorkloadEvidenceError(f"systemctl show failed for {unit}: {exc}") from exc
    if completed.returncode != 0:
        diagnostic = (completed.stderr or completed.stdout).strip()[:240]
        raise WorkloadEvidenceError(
            f"systemctl show failed for {unit}: {diagnostic or completed.returncode}"
        )
    return _properties(completed.stdout)


def _mib(value: str | None) -> int:
    if value in {None, "", "[not set]", "infinity", "max"}:
        return 0
    try:
        return max(0, int(value) // _MIB)
    except ValueError as exc:
        raise WorkloadEvidenceError(f"invalid systemd memory value: {value!r}") from exc


def _unit_sample(properties: dict[str, str]) -> UnitWorkloadSample:
    try:
        start = int(properties.get("ExecMainStartTimestampMonotonic") or "0")
        end = int(properties.get("ExecMainExitTimestampMonotonic") or "0")
        status = int(properties.get("ExecMainStatus") or "0")
    except ValueError as exc:
        raise WorkloadEvidenceError("invalid unit execution measurement") from exc
    runtime = max(0, (end - start) // 1_000_000) if end and start else 0
    return UnitWorkloadSample(
        invocation_id=properties.get("InvocationID", ""),
        load_state=properties.get("LoadState", "unknown"),
        active_state=properties.get("ActiveState", "unknown"),
        result=properties.get("Result", "unknown"),
        exec_main_status=status,
        memory_current_mib=_mib(properties.get("MemoryCurrent")),
        memory_peak_mib=_mib(properties.get("MemoryPeak")),
        successful_runtime_seconds=runtime,
    )


def _meminfo(path: Path) -> tuple[int, int]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise WorkloadEvidenceError(f"cannot read meminfo: {exc}") from exc
    values: dict[str, int] = {}
    for line in lines:
        key, separator, raw = line.partition(":")
        fields = raw.split()
        if separator and len(fields) == 2 and fields[0].isdigit() and fields[1] == "kB":
            values[key] = int(fields[0]) // 1024
    try:
        return values["MemTotal"], values["MemAvailable"]
    except KeyError as exc:
        raise WorkloadEvidenceError(f"meminfo is missing {exc.args[0]}") from exc


def _read_boot_id(path: Path) -> str:
    try:
        boot_id = path.read_text(encoding="ascii").strip()
    except OSError as exc:
        raise WorkloadEvidenceError(f"cannot read kernel boot id: {exc}") from exc
    if not boot_id:
        raise WorkloadEvidenceError("kernel boot id is empty")
    return boot_id


def _read_clock_boottime_ns() -> int:
    clock_id = getattr(time, "CLOCK_BOOTTIME", None)
    if clock_id is None:
        raise WorkloadEvidenceError("CLOCK_BOOTTIME is unavailable on this host")
    try:
        return time.clock_gettime_ns(clock_id)
    except OSError as exc:
        raise WorkloadEvidenceError(f"cannot read CLOCK_BOOTTIME: {exc}") from exc


def collect_workload_sample(
    *,
    sampled_at: datetime | None = None,
    meminfo_path: Path = _MEMINFO,
    boot_id_path: Path = _BOOT_ID,
    boottime_reader: Callable[[], int] = _read_clock_boottime_ns,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> WorkloadSample:
    """Read systemd/cgroup counters without starting, stopping, or resetting units."""

    captured_at = (sampled_at or datetime.now(UTC)).astimezone(UTC)
    boot_id = _read_boot_id(boot_id_path)
    clock_boottime_ns = boottime_reader()
    total, available = _meminfo(meminfo_path)
    slices = {
        name: _show(name, _SLICE_PROPERTIES, runner=runner)
        for name in (
            "rquant-live.slice",
            "rquant-serving.slice",
            "rquant-maintenance.slice",
            "system.slice",
        )
    }
    units = {
        name: _unit_sample(_show(name, _UNIT_PROPERTIES, runner=runner))
        for name in (
            "rquant-monitor.service",
            "rquant-backup.service",
            "rquant-replica-sync.service",
        )
    }
    return WorkloadSample(
        schema_version=2,
        sampled_at=captured_at,
        boot_id=boot_id,
        clock_boottime_ns=clock_boottime_ns,
        host_mem_total_mib=total,
        mem_available_mib=available,
        live_current_mib=_mib(slices["rquant-live.slice"].get("MemoryCurrent")),
        live_peak_mib=_mib(slices["rquant-live.slice"].get("MemoryPeak")),
        serving_current_mib=_mib(
            slices["rquant-serving.slice"].get("MemoryCurrent")
        ),
        serving_peak_mib=_mib(slices["rquant-serving.slice"].get("MemoryPeak")),
        maintenance_current_mib=_mib(
            slices["rquant-maintenance.slice"].get("MemoryCurrent")
        ),
        maintenance_peak_mib=_mib(
            slices["rquant-maintenance.slice"].get("MemoryPeak")
        ),
        os_system_slice_current_mib=_mib(
            slices["system.slice"].get("MemoryCurrent")
        ),
        os_system_slice_peak_mib=_mib(slices["system.slice"].get("MemoryPeak")),
        monitor=units["rquant-monitor.service"],
        backup=units["rquant-backup.service"],
        replica=units["rquant-replica-sync.service"],
    )


def _write_summary(path: Path, evidence: WorkloadHighWaterEvidence) -> None:
    target = Path(path)
    _ensure_safe_regular_file(target, allow_missing=True)
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(temporary, flags, 0o640)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(evidence.model_dump_json(indent=2))
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
    except OSError as exc:
        with suppress(OSError):
            temporary.unlink()
        raise WorkloadEvidenceError(f"cannot publish workload summary: {exc}") from exc


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    sample_parser = subparsers.add_parser("sample")
    sample_parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_HIGH_WATER_RAW_EVIDENCE_PATH,
    )
    summary_parser = subparsers.add_parser("summarize")
    summary_parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_HIGH_WATER_RAW_EVIDENCE_PATH,
    )
    summary_parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_HIGH_WATER_EVIDENCE_PATH,
    )
    args = parser.parse_args(argv)
    try:
        if args.command == "sample":
            append_workload_sample(args.output, collect_workload_sample())
        else:
            _write_summary(args.output, summarize_workload_samples(args.input))
    except WorkloadEvidenceError as exc:
        parser.error(str(exc))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "ChainedWorkloadSample",
    "UnitWorkloadSample",
    "WorkloadEvidenceError",
    "WorkloadSample",
    "append_workload_sample",
    "collect_workload_sample",
    "summarize_workload_samples",
]
