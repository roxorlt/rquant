"""Append-only workload evidence and strict high-water summarization contracts."""

from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

NOW = datetime(2026, 8, 5, 2, 0, tzinfo=UTC)
BOOT_ID = "11111111-2222-3333-4444-555555555555"
OTHER_BOOT_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
CADENCE = timedelta(minutes=5)
WINDOW_SAMPLE_COUNT = 24 * 60 // 5 + 1


def _unit(
    invocation_id: str,
    *,
    peak_mib: int,
    runtime_seconds: int,
) -> dict[str, object]:
    return {
        "invocation_id": invocation_id,
        "load_state": "loaded",
        "active_state": "inactive",
        "result": "success",
        "exec_main_status": 0,
        "memory_current_mib": 0,
        "memory_peak_mib": peak_mib,
        "successful_runtime_seconds": runtime_seconds,
    }


def _sample(
    at: datetime,
    *,
    suffix: str,
    boot_id: str = BOOT_ID,
    boottime_seconds: int | None = None,
) -> dict[str, object]:
    monotonic_seconds = (
        boottime_seconds
        if boottime_seconds is not None
        else 1_000 + int((at - NOW).total_seconds())
    )
    return {
        "schema_version": 2,
        "sampled_at": at.isoformat(),
        "boot_id": boot_id,
        "clock_boottime_ns": monotonic_seconds * 1_000_000_000,
        "host_mem_total_mib": 7690,
        "mem_available_mib": 2400 if suffix == "a" else 2100,
        "live_current_mib": 3200,
        "live_peak_mib": 3400,
        "serving_current_mib": 300,
        "serving_peak_mib": 430,
        "maintenance_current_mib": 0,
        "maintenance_peak_mib": 1500,
        "os_system_slice_current_mib": 700,
        "os_system_slice_peak_mib": 920,
        "monitor": _unit(f"monitor-{suffix}", peak_mib=2814, runtime_seconds=100),
        "backup": _unit(f"backup-{suffix}", peak_mib=1303, runtime_seconds=60),
        "replica": _unit(f"replica-{suffix}", peak_mib=310, runtime_seconds=40),
    }


def _continuous_samples(
    start: datetime = NOW,
    *,
    boot_id: str = BOOT_ID,
    count: int = WINDOW_SAMPLE_COUNT,
    first_boottime_seconds: int = 1_000,
) -> tuple[dict[str, object], ...]:
    return tuple(
        _sample(
            start + index * CADENCE,
            suffix="a" if index < count // 2 else "b",
            boot_id=boot_id,
            boottime_seconds=first_boottime_seconds + index * 300,
        )
        for index in range(count)
    )


def test_append_only_samples_summarize_strict_auditable_evidence(
    tmp_path: Path,
) -> None:
    from rquant.workload_evidence import (
        WorkloadSample,
        append_workload_sample,
        summarize_workload_samples,
    )

    raw = tmp_path / "samples.jsonl"
    for sample in _continuous_samples():
        append_workload_sample(raw, WorkloadSample.model_validate(sample))

    raw_bytes = raw.read_bytes()
    evidence = summarize_workload_samples(raw)

    assert raw_bytes.count(b"\n") == WINDOW_SAMPLE_COUNT
    records = [json.loads(line) for line in raw_bytes.splitlines()]
    assert [record["sequence"] for record in records] == list(range(1, WINDOW_SAMPLE_COUNT + 1))
    assert records[0]["previous_record_sha256"] == "0" * 64
    assert records[1]["previous_record_sha256"] == records[0]["record_sha256"]
    assert records[0]["sample"]["boot_id"] == BOOT_ID
    assert records[0]["sample"]["clock_boottime_ns"] == 1_000_000_000_000
    assert evidence.observation_window_hours == 24
    assert evidence.backup_successful_runs == 2
    assert evidence.replica_successful_runs == 2
    assert evidence.backup_sample_count == WINDOW_SAMPLE_COUNT
    assert evidence.replica_sample_count == WINDOW_SAMPLE_COUNT
    assert evidence.backup_successful_runtime_seconds == 120
    assert evidence.replica_successful_runtime_seconds == 80
    assert evidence.raw_evidence_sha256 == hashlib.sha256(raw_bytes).hexdigest()
    assert evidence.os_system_slice_peak_mib == 920
    assert evidence.min_mem_available_mib == 2100


def test_summary_rejects_a_window_shorter_than_24_hours(tmp_path: Path) -> None:
    from rquant.workload_evidence import (
        WorkloadEvidenceError,
        WorkloadSample,
        append_workload_sample,
        summarize_workload_samples,
    )

    raw = tmp_path / "samples.jsonl"
    for sample in _continuous_samples(count=24 * 60 // 5):
        append_workload_sample(raw, WorkloadSample.model_validate(sample))

    with pytest.raises(WorkloadEvidenceError, match="24 hours"):
        summarize_workload_samples(raw)


def test_summary_rejects_two_samples_spanning_24_hours(tmp_path: Path) -> None:
    from rquant.workload_evidence import (
        WorkloadEvidenceError,
        WorkloadSample,
        append_workload_sample,
        summarize_workload_samples,
    )

    raw = tmp_path / "samples.jsonl"
    for sample in (
        _sample(NOW, suffix="a", boottime_seconds=1_000),
        _sample(
            NOW + timedelta(hours=24),
            suffix="b",
            boottime_seconds=1_000 + 24 * 3600,
        ),
    ):
        append_workload_sample(raw, WorkloadSample.model_validate(sample))

    with pytest.raises(WorkloadEvidenceError, match="samples|gap"):
        summarize_workload_samples(raw)


def test_summary_rejects_a_sampling_gap_above_timer_jitter_limit(
    tmp_path: Path,
) -> None:
    from rquant.workload_evidence import (
        WorkloadEvidenceError,
        WorkloadSample,
        append_workload_sample,
        summarize_workload_samples,
    )

    raw = tmp_path / "samples.jsonl"
    at = NOW
    boottime_seconds = 1_000
    for index in range(WINDOW_SAMPLE_COUNT + 1):
        if index > 0:
            gap_seconds = 451 if index == 100 else 300
            at += timedelta(seconds=gap_seconds)
            boottime_seconds += gap_seconds
        append_workload_sample(
            raw,
            WorkloadSample.model_validate(
                _sample(
                    at,
                    suffix="a" if index < 150 else "b",
                    boottime_seconds=boottime_seconds,
                )
            ),
        )

    with pytest.raises(WorkloadEvidenceError, match="gap"):
        summarize_workload_samples(raw)


def test_summary_allows_runtime_drift_below_gap_limit_at_nominal_sample_count(
    tmp_path: Path,
) -> None:
    from rquant.workload_evidence import (
        WorkloadSample,
        append_workload_sample,
        summarize_workload_samples,
    )

    raw = tmp_path / "samples.jsonl"
    for index in range(WINDOW_SAMPLE_COUNT):
        append_workload_sample(
            raw,
            WorkloadSample.model_validate(
                _sample(
                    NOW + timedelta(seconds=305 * index),
                    suffix="a" if index < 150 else "b",
                    boottime_seconds=1_000 + 305 * index,
                )
            ),
        )

    evidence = summarize_workload_samples(raw)

    assert evidence.backup_sample_count == WINDOW_SAMPLE_COUNT


@pytest.mark.parametrize("clock", ("wall", "monotonic"))
def test_summary_requires_strictly_increasing_wall_and_monotonic_clocks(
    tmp_path: Path,
    clock: str,
) -> None:
    from rquant.workload_evidence import (
        WorkloadEvidenceError,
        WorkloadSample,
        append_workload_sample,
        summarize_workload_samples,
    )

    raw = tmp_path / "samples.jsonl"
    for index in range(WINDOW_SAMPLE_COUNT + 1):
        wall_index = index - 1 if clock == "wall" and index == 100 else index
        mono_index = index - 1 if clock == "monotonic" and index == 100 else index
        append_workload_sample(
            raw,
            WorkloadSample.model_validate(
                _sample(
                    NOW + wall_index * CADENCE,
                    suffix="a" if index < 150 else "b",
                    boottime_seconds=1_000 + mono_index * 300,
                )
            ),
        )

    with pytest.raises(WorkloadEvidenceError, match=clock):
        summarize_workload_samples(raw)


def test_summary_never_combines_short_segments_from_different_boots(
    tmp_path: Path,
) -> None:
    from rquant.workload_evidence import (
        WorkloadEvidenceError,
        WorkloadSample,
        append_workload_sample,
        summarize_workload_samples,
    )

    raw = tmp_path / "samples.jsonl"
    first_segment = _continuous_samples(count=WINDOW_SAMPLE_COUNT // 2 + 1)
    second_start = NOW + (len(first_segment) * CADENCE)
    second_segment = _continuous_samples(
        second_start,
        boot_id=OTHER_BOOT_ID,
        count=WINDOW_SAMPLE_COUNT // 2 + 1,
        first_boottime_seconds=200,
    )
    for sample in (*first_segment, *second_segment):
        append_workload_sample(raw, WorkloadSample.model_validate(sample))

    with pytest.raises(WorkloadEvidenceError, match="same-boot|continuous"):
        summarize_workload_samples(raw)


def test_summary_can_select_a_complete_segment_after_a_reboot(tmp_path: Path) -> None:
    from rquant.workload_evidence import (
        WorkloadSample,
        append_workload_sample,
        summarize_workload_samples,
    )

    raw = tmp_path / "samples.jsonl"
    old_segment = _continuous_samples(count=2)
    current_start = NOW + 2 * CADENCE
    current_segment = _continuous_samples(
        current_start,
        boot_id=OTHER_BOOT_ID,
        first_boottime_seconds=200,
    )
    for sample in (*old_segment, *current_segment):
        append_workload_sample(raw, WorkloadSample.model_validate(sample))

    evidence = summarize_workload_samples(raw)

    assert evidence.observed_at == current_start + timedelta(hours=24)
    assert evidence.backup_sample_count == WINDOW_SAMPLE_COUNT


def test_append_refuses_a_symlink_target(tmp_path: Path) -> None:
    from rquant.workload_evidence import (
        WorkloadEvidenceError,
        WorkloadSample,
        append_workload_sample,
    )

    target = tmp_path / "target.jsonl"
    target.write_text("", encoding="utf-8")
    link = tmp_path / "samples.jsonl"
    link.symlink_to(target)

    with pytest.raises(WorkloadEvidenceError, match="unsafe"):
        append_workload_sample(
            link,
            WorkloadSample.model_validate(_sample(NOW, suffix="a")),
        )


def test_collector_records_kernel_boot_id_and_clock_boottime(tmp_path: Path) -> None:
    from rquant.workload_evidence import collect_workload_sample

    meminfo = tmp_path / "meminfo"
    meminfo.write_text(
        "MemTotal:        7874560 kB\nMemAvailable:   2457600 kB\n",
        encoding="utf-8",
    )
    boot_id = tmp_path / "boot_id"
    boot_id.write_text(f"{BOOT_ID}\n", encoding="ascii")

    def runner(
        command: list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        unit = command[2]
        if unit.endswith(".slice"):
            stdout = "MemoryCurrent=1048576\nMemoryPeak=2097152\n"
        else:
            stdout = "\n".join(
                (
                    "LoadState=loaded",
                    "ActiveState=inactive",
                    "Result=success",
                    "ExecMainStatus=0",
                    "InvocationID=invocation",
                    "MemoryCurrent=0",
                    "MemoryPeak=1048576",
                    "ExecMainStartTimestampMonotonic=1000000",
                    "ExecMainExitTimestampMonotonic=2000000",
                    "",
                )
            )
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    sample = collect_workload_sample(
        sampled_at=NOW,
        meminfo_path=meminfo,
        boot_id_path=boot_id,
        boottime_reader=lambda: 987_654_321,
        runner=runner,
    )

    assert sample.schema_version == 2
    assert sample.boot_id == BOOT_ID
    assert sample.clock_boottime_ns == 987_654_321


def test_summary_rejects_a_rewritten_record_even_with_a_self_declared_hash(
    tmp_path: Path,
) -> None:
    from rquant.strict_json import canonical_json_bytes
    from rquant.workload_evidence import (
        WorkloadEvidenceError,
        WorkloadSample,
        append_workload_sample,
        summarize_workload_samples,
    )

    raw = tmp_path / "samples.jsonl"
    for sample in (
        _sample(NOW, suffix="a"),
        _sample(NOW + timedelta(hours=24), suffix="b"),
    ):
        append_workload_sample(raw, WorkloadSample.model_validate(sample))
    records = [json.loads(line) for line in raw.read_bytes().splitlines()]
    records[0]["sample"]["mem_available_mib"] = 1
    records[0]["record_sha256"] = hashlib.sha256(
        canonical_json_bytes(
            {key: value for key, value in records[0].items() if key != "record_sha256"}
        )
    ).hexdigest()
    raw.write_bytes(
        b"".join(canonical_json_bytes(record, trailing_newline=True) for record in records)
    )

    with pytest.raises(WorkloadEvidenceError, match="hash chain"):
        summarize_workload_samples(raw)


def test_cloud_summary_step_is_explicit_fixed_path_and_never_controls_units() -> None:
    root = Path(__file__).resolve().parents[2]
    script = (root / "scripts/summarize-workload-high-water.sh").read_text(encoding="utf-8")

    assert "PATH=/usr/sbin:/usr/bin:/sbin:/bin" in script
    assert "/home/lighthouse/rquant/.venv/bin/python" in script
    assert "samples.jsonl" in script
    assert "high-water.json" in script
    assert "systemctl" not in script
