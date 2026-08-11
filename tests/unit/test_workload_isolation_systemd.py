"""Auditable workload-slice contracts for production systemd units."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
SYSTEMD = ROOT / "deploy" / "systemd"
GIB = 1024**3
MIB = 1024**2
NOW = datetime(2026, 8, 5, 2, 0, tzinfo=UTC)


def _write_arbiter_provenance(tmp_path: Path) -> tuple[Path, Path]:
    helper = tmp_path / "rquant-workload-arbiter"
    helper.write_bytes((ROOT / "deploy/libexec/rquant-workload-arbiter").read_bytes())
    helper.chmod(0o755)
    digest = tmp_path / "rquant-workload-arbiter.sha256"
    digest.write_text(
        f"{hashlib.sha256(helper.read_bytes()).hexdigest()}\n",
        encoding="ascii",
    )
    digest.chmod(0o444)
    return helper, digest


def test_static_contract_classifies_services_and_keeps_root_authority_separate() -> None:
    from rquant.workload_isolation import (
        MAINTENANCE_UNITS,
        ROOT_AUTHORITY_UNITS,
        WORKLOAD_UNIT_SLICES,
        verify_workload_unit_declarations,
    )

    result = verify_workload_unit_declarations(SYSTEMD)

    assert result.status == "warn", result.details
    assert "pending calibration" in result.summary
    assert WORKLOAD_UNIT_SLICES["rquant-monitor.service"] == "rquant-live.slice"
    assert WORKLOAD_UNIT_SLICES["rquant-daily.service"] == "rquant-live.slice"
    assert WORKLOAD_UNIT_SLICES["rquant-morning-pulse.service"] == "rquant-live.slice"
    assert WORKLOAD_UNIT_SLICES["rquant-midday-report.service"] == "rquant-live.slice"
    assert WORKLOAD_UNIT_SLICES["rquant-dashboard.service"] == "rquant-serving.slice"
    assert WORKLOAD_UNIT_SLICES["rquant-research-ingest.service"] == "rquant-research.slice"
    assert WORKLOAD_UNIT_SLICES["rquant-backup.service"] == "rquant-maintenance.slice"
    assert WORKLOAD_UNIT_SLICES["rquant-replica-sync.service"] == ("rquant-maintenance.slice")
    assert WORKLOAD_UNIT_SLICES["rquant-daily-receipt-signer.service"] == "system.slice"
    assert WORKLOAD_UNIT_SLICES["rquant-lab-claim-finalizer.service"] == ("rquant-research.slice")
    assert WORKLOAD_UNIT_SLICES["rquant-external-monotonic-root.service"] == "system.slice"
    assert WORKLOAD_UNIT_SLICES["rquant-resource-authority.service"] == "system.slice"
    assert {
        "rquant-backup.service",
        "rquant-replica-sync.service",
    } == MAINTENANCE_UNITS
    assert {
        "rquant-daily-receipt-signer.service",
        "rquant-external-monotonic-root.service",
        "rquant-resource-authority.service",
    } == ROOT_AUTHORITY_UNITS


def test_lab_claim_finalizer_uses_research_arbiter_and_exit_75_contract() -> None:
    from rquant.workload_isolation import WORKLOAD_ARBITER_PATH

    content = (SYSTEMD / "rquant-lab-claim-finalizer.service").read_text(encoding="utf-8")

    assert "Slice=rquant-research.slice" in content
    assert "EnvironmentFile=/etc/rquant/lab-claim-finalizer.env" in content
    assert (
        "ExecStartPre=/home/lighthouse/rquant/.venv/bin/rquant "
        "lab-claim-finalizer-preflight --format json"
    ) in content
    assert (
        f"ExecStart={WORKLOAD_ARBITER_PATH} research -- "
        "/home/lighthouse/rquant/.venv/bin/python -I -S "
        "/home/lighthouse/rquant/scripts/run-lab-daemon.py "
    ) in content
    assert "SuccessExitStatus=0 75" in content


def test_usable_8_gib_class_budget_marks_maintenance_pending_calibration() -> None:
    from rquant.workload_isolation import (
        MIN_HOST_MEMORY_BYTES,
        WORKLOAD_MEMORY_BUDGET_MIB,
        verify_workload_memory_admission,
    )

    result = verify_workload_memory_admission(SYSTEMD)
    budget = WORKLOAD_MEMORY_BUDGET_MIB
    normal = budget["live"] + budget["serving"] + budget["research"] + budget["os"]
    assert result.status == "warn", result.details
    assert MIN_HOST_MEMORY_BYTES == 7680 * MIB
    assert normal <= budget["usable_host"]
    assert "maintenance" not in budget
    assert "backup_observed_peak" not in budget
    assert "replica_provisional_allowance" not in budget
    assert any("pending calibration" in detail for detail in result.details)


def test_only_research_has_a_memory_hard_cap() -> None:
    from rquant.workload_isolation import (
        PARENT_SLICE_LIMITS,
        WORKLOAD_SLICE_LIMITS,
    )

    assert "MemoryMax" not in PARENT_SLICE_LIMITS
    for slice_name in (
        "rquant-live.slice",
        "rquant-serving.slice",
        "rquant-maintenance.slice",
    ):
        assert "MemoryMax" not in WORKLOAD_SLICE_LIMITS[slice_name]
    assert WORKLOAD_SLICE_LIMITS["rquant-research.slice"]["MemoryMax"] == "768M"
    assert WORKLOAD_SLICE_LIMITS["rquant-research.slice"]["CPUQuota"] == "100%"
    assert "MemoryHigh" not in WORKLOAD_SLICE_LIMITS["rquant-maintenance.slice"]


def test_static_contract_rejects_a_live_hard_cap(tmp_path: Path) -> None:
    from rquant.workload_isolation import verify_workload_memory_admission

    fixture = tmp_path / "systemd"
    shutil.copytree(SYSTEMD, fixture)
    live = fixture / "rquant-live.slice"
    live.write_text(
        live.read_text(encoding="utf-8") + "MemoryMax=4096M\n",
        encoding="utf-8",
    )

    result = verify_workload_memory_admission(fixture)

    assert result.status == "fail"
    assert any("unsafe hard MemoryMax" in detail for detail in result.details)


def test_static_contract_explicitly_blocks_an_installed_legacy_runtime_template(
    tmp_path: Path,
) -> None:
    from rquant.workload_isolation import verify_workload_unit_declarations

    fixture = tmp_path / "systemd"
    shutil.copytree(SYSTEMD, fixture)
    (fixture / "rquant-runtime-live@.service").write_text(
        "[Service]\nSlice=rquant-live.slice\n",
        encoding="utf-8",
    )

    result = verify_workload_unit_declarations(fixture)

    assert result.status == "fail"
    assert any(
        "legacy template is installed" in detail and "preview" in detail
        for detail in result.details
    )


def test_static_contract_gives_research_ingest_no_arbiter_exemption(
    tmp_path: Path,
) -> None:
    from rquant.workload_isolation import verify_workload_unit_declarations

    fixture = tmp_path / "systemd"
    shutil.copytree(SYSTEMD, fixture)
    ingest = fixture / "rquant-research-ingest.service"
    ingest.write_text(
        ingest.read_text(encoding="utf-8").replace(
            "/usr/local/libexec/rquant-workload-arbiter research -- ",
            "",
        ),
        encoding="utf-8",
    )

    result = verify_workload_unit_declarations(fixture)

    assert result.status == "fail"
    assert any("research lifecycle arbiter is missing" in item for item in result.details)


def _evidence_unit(
    invocation_id: str,
    *,
    current_mib: int,
    peak_mib: int,
    runtime_seconds: int,
) -> dict[str, object]:
    return {
        "invocation_id": invocation_id,
        "load_state": "loaded",
        "active_state": "inactive",
        "result": "success",
        "exec_main_status": 0,
        "memory_current_mib": current_mib,
        "memory_peak_mib": peak_mib,
        "successful_runtime_seconds": runtime_seconds,
    }


def _write_high_water(
    path: Path,
    *,
    raw_observed_at: datetime = NOW,
    raw_replica_peak_mib: int = 300,
    **overrides: object,
) -> None:
    from rquant.workload_evidence import (
        WorkloadSample,
        append_workload_sample,
        summarize_workload_samples,
    )

    raw_path = path.with_name("samples.jsonl")
    raw_path.unlink(missing_ok=True)
    sample_count = 24 * 60 // 5 + 1
    first_observed_at = raw_observed_at - timedelta(hours=24)
    for index in range(sample_count):
        invocation_suffix = "a" if index < sample_count // 2 else "b"
        sample = WorkloadSample.model_validate(
            {
                "schema_version": 2,
                "sampled_at": (first_observed_at + timedelta(minutes=5 * index)).isoformat(),
                "boot_id": "11111111-2222-3333-4444-555555555555",
                "clock_boottime_ns": (1_000 + index * 300) * 1_000_000_000,
                "host_mem_total_mib": 7690,
                "mem_available_mib": 2200 + min(index, 3) * 10,
                "live_current_mib": 3200,
                "live_peak_mib": 3300,
                "serving_current_mib": 300,
                "serving_peak_mib": 420,
                "maintenance_current_mib": 0,
                "maintenance_peak_mib": 1500,
                "os_system_slice_current_mib": 700,
                "os_system_slice_peak_mib": 910,
                "monitor": _evidence_unit(
                    f"monitor-{invocation_suffix}",
                    current_mib=2415,
                    peak_mib=2814,
                    runtime_seconds=100,
                ),
                "backup": _evidence_unit(
                    f"backup-{invocation_suffix}",
                    current_mib=0,
                    peak_mib=1303,
                    runtime_seconds=60,
                ),
                "replica": _evidence_unit(
                    f"replica-{invocation_suffix}",
                    current_mib=0,
                    peak_mib=raw_replica_peak_mib,
                    runtime_seconds=40,
                ),
            }
        )
        append_workload_sample(raw_path, sample)
    payload = summarize_workload_samples(raw_path).model_dump(mode="json")
    payload.update(overrides)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_high_water_evidence_is_complete_but_calibration_remains_pending(
    tmp_path: Path,
) -> None:
    from rquant.workload_isolation import check_workload_high_water_evidence

    evidence = tmp_path / "high-water.json"
    _write_high_water(evidence)

    result = check_workload_high_water_evidence(
        evidence_path=evidence,
        strict=False,
        as_of=NOW + timedelta(hours=1),
    )

    assert result.status == "warn", result.details
    assert "pending calibration" in result.summary
    assert any("backup+replica observed peaks=1603 MiB" in detail for detail in result.details)
    assert any("monitor peak=2814 MiB" in detail for detail in result.details)


def test_high_water_evidence_does_not_apply_an_uncalibrated_maintenance_threshold(
    tmp_path: Path,
) -> None:
    from rquant.workload_isolation import check_workload_high_water_evidence

    evidence = tmp_path / "high-water.json"
    _write_high_water(
        evidence,
        raw_replica_peak_mib=600,
    )

    result = check_workload_high_water_evidence(
        evidence_path=evidence,
        strict=False,
        as_of=NOW + timedelta(hours=1),
    )

    assert result.status == "warn", result.details
    assert any("backup+replica observed peaks=1903 MiB" in detail for detail in result.details)


def test_high_water_gate_requires_complete_run_and_raw_evidence_contract(
    tmp_path: Path,
) -> None:
    from rquant.workload_isolation import check_workload_high_water_evidence

    invalid_values = {
        "observation_window_hours": 23,
        "backup_successful_runs": 0,
        "replica_successful_runs": 0,
        "backup_sample_count": 0,
        "replica_sample_count": 0,
        "backup_successful_runtime_seconds": 0,
        "replica_successful_runtime_seconds": 0,
        "raw_evidence_sha256": "not-a-sha",
        "os_system_slice_peak_mib": 0,
        "min_mem_available_mib": 0,
    }
    for field, invalid in invalid_values.items():
        evidence = tmp_path / f"{field}.json"
        _write_high_water(evidence, **{field: invalid})

        result = check_workload_high_water_evidence(
            evidence_path=evidence,
            strict=True,
            as_of=NOW + timedelta(hours=1),
        )

        assert result.status == "fail", field


def test_high_water_gate_rejects_fake_raw_with_matching_self_declared_sha(
    tmp_path: Path,
) -> None:
    from rquant.workload_isolation import check_workload_high_water_evidence

    evidence = tmp_path / "high-water.json"
    _write_high_water(evidence)
    fake_raw = b'{"sample":"self-declared"}\n'
    evidence.with_name("samples.jsonl").write_bytes(fake_raw)
    payload = json.loads(evidence.read_text(encoding="utf-8"))
    payload["raw_evidence_sha256"] = hashlib.sha256(fake_raw).hexdigest()
    evidence.write_text(json.dumps(payload), encoding="utf-8")

    result = check_workload_high_water_evidence(
        evidence_path=evidence,
        strict=True,
        as_of=NOW + timedelta(hours=1),
    )

    assert result.status == "fail"
    assert "replay failed" in result.summary


def test_missing_or_stale_high_water_is_warn_relaxed_and_fail_strict(tmp_path: Path) -> None:
    from rquant.workload_isolation import check_workload_high_water_evidence

    missing = tmp_path / "missing.json"
    relaxed = check_workload_high_water_evidence(
        evidence_path=missing,
        strict=False,
        as_of=NOW,
    )
    strict = check_workload_high_water_evidence(
        evidence_path=missing,
        strict=True,
        as_of=NOW,
    )

    assert relaxed.status == "warn"
    assert strict.status == "fail"

    stale = tmp_path / "stale.json"
    _write_high_water(stale, raw_observed_at=NOW - timedelta(days=31))
    stale_result = check_workload_high_water_evidence(
        evidence_path=stale,
        strict=True,
        as_of=NOW,
    )

    assert stale_result.status == "fail"
    assert "stale" in stale_result.summary


def _write_slice_cgroup(
    cgroup_root: Path,
    control_group: str,
    limits: dict[str, str],
) -> None:
    from rquant.workload_isolation import parse_systemd_bytes

    path = cgroup_root / control_group.removeprefix("/")
    path.mkdir(parents=True)
    (path / "cpu.weight").write_text(f"{limits['CPUWeight']}\n", encoding="utf-8")
    (path / "io.weight").write_text(f"default {limits['IOWeight']}\n", encoding="utf-8")
    (path / "memory.low").write_text(
        f"{parse_systemd_bytes(limits.get('MemoryLow', '0'))}\n",
        encoding="utf-8",
    )
    memory_high = (
        str(parse_systemd_bytes(limits["MemoryHigh"])) if "MemoryHigh" in limits else "max"
    )
    (path / "memory.high").write_text(f"{memory_high}\n", encoding="utf-8")
    memory_max = str(parse_systemd_bytes(limits["MemoryMax"])) if "MemoryMax" in limits else "max"
    (path / "memory.max").write_text(f"{memory_max}\n", encoding="utf-8")
    (path / "pids.max").write_text(f"{limits['TasksMax']}\n", encoding="utf-8")
    if "CPUQuota" in limits:
        (path / "cpu.max").write_text("100000 100000\n", encoding="utf-8")


def test_runtime_enumerates_instances_and_uses_resolved_control_groups(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from rquant.workload_isolation import (
        PARENT_SLICE_LIMITS,
        WORKLOAD_SLICE_LIMITS,
        check_workload_runtime,
        parse_systemd_bytes,
    )

    cgroup_root = tmp_path / "cgroup"
    cgroup_root.mkdir()
    (cgroup_root / "cgroup.controllers").write_text("cpu io memory pids\n", encoding="utf-8")
    control_groups = {
        "rquant.slice": "/rquant.slice",
        **{name: f"/rquant.slice/{name}" for name in WORKLOAD_SLICE_LIMITS},
    }
    _write_slice_cgroup(cgroup_root, control_groups["rquant.slice"], dict(PARENT_SLICE_LIMITS))
    for slice_name, limits in WORKLOAD_SLICE_LIMITS.items():
        _write_slice_cgroup(cgroup_root, control_groups[slice_name], dict(limits))

    loaded = {
        "rquant-monitor.service": (
            "rquant-live.slice",
            "/rquant.slice/rquant-live.slice/rquant-monitor.service",
        ),
        "rquant-runtime-feature@svc-live.service": (
            "rquant-live.slice",
            "/rquant.slice/rquant-live.slice/rquant-runtime-feature@svc-live.service",
        ),
        "rquant-runtime-lab-jobs@svc-lab.service": (
            "rquant-research.slice",
            "/rquant.slice/rquant-research.slice/rquant-runtime-lab-jobs@svc-lab.service",
        ),
    }
    calls: list[list[str]] = []

    def fake_run(command: list[str], **_kwargs: object) -> SimpleNamespace:
        calls.append(command)
        if "list-units" in command:
            stdout = "\n".join(f"{unit} loaded active running fixture" for unit in loaded)
            return SimpleNamespace(returncode=0, stdout=f"{stdout}\n", stderr="")
        unit = command[2]
        if unit == "rquant.slice":
            limits = PARENT_SLICE_LIMITS
        elif unit in WORKLOAD_SLICE_LIMITS:
            limits = WORKLOAD_SLICE_LIMITS[unit]
        else:
            expected_slice, control_group = loaded[unit]
            return SimpleNamespace(
                returncode=0,
                stdout=(
                    "LoadState=loaded\nActiveState=active\n"
                    f"Slice={expected_slice}\nControlGroup={control_group}\n"
                    + (
                        "ExecStart={ path=/usr/local/libexec/rquant-workload-arbiter ; "
                        "argv[]=/usr/local/libexec/rquant-workload-arbiter research -- "
                        "/fixture ; }\n"
                        if expected_slice == "rquant-research.slice"
                        else ""
                    )
                ),
                stderr="",
            )
        memory_max = (
            str(parse_systemd_bytes(limits["MemoryMax"])) if "MemoryMax" in limits else "infinity"
        )
        memory_high = (
            parse_systemd_bytes(limits["MemoryHigh"]) if "MemoryHigh" in limits else "infinity"
        )
        stdout = (
            "LoadState=loaded\nActiveState=active\n"
            f"ControlGroup={control_groups[unit]}\n"
            f"CPUWeight={limits['CPUWeight']}\nIOWeight={limits['IOWeight']}\n"
            f"MemoryLow={parse_systemd_bytes(limits.get('MemoryLow', '0'))}\n"
            f"MemoryHigh={memory_high}\n"
            f"MemoryMax={memory_max}\nTasksMax={limits['TasksMax']}\n"
            f"CPUQuotaPerSecUSec={'1s' if unit == 'rquant-research.slice' else 'infinity'}\n"
        )
        return SimpleNamespace(returncode=0, stdout=stdout, stderr="")

    monkeypatch.setattr("rquant.workload_isolation.subprocess.run", fake_run)
    arbiter, arbiter_hash = _write_arbiter_provenance(tmp_path)

    result = check_workload_runtime(
        cgroup_root=cgroup_root,
        platform_name="Linux",
        systemctl_path=Path("/usr/bin/systemctl"),
        strict=True,
        arbiter_path=arbiter,
        arbiter_hash_path=arbiter_hash,
        arbiter_expected_uid=os.getuid(),
    )

    assert result.status == "ok", result.details
    assert any("list-units" in call for call in calls)
    queried = {call[2] for call in calls if len(call) > 2 and call[1] == "show"}
    assert set(loaded) <= queried
    assert "rquant-runtime-feature@.service" not in queried
    assert any("research cpu.max=100000 100000" in detail for detail in result.details)


def test_runtime_requires_exact_research_cpu_quota(tmp_path: Path, monkeypatch) -> None:
    from rquant.workload_isolation import check_workload_runtime

    cgroup_root = tmp_path / "cgroup"
    research = cgroup_root / "rquant.slice/rquant-research.slice"
    research.mkdir(parents=True)
    (cgroup_root / "cgroup.controllers").write_text("cpu io memory pids\n", encoding="utf-8")
    (research / "cpu.max").write_text("99999 100000\n", encoding="utf-8")
    for name, value in {
        "cpu.weight": "100",
        "io.weight": "default 100",
        "memory.low": "0",
        "memory.high": str(512 * MIB),
        "memory.max": str(768 * MIB),
        "pids.max": "128",
    }.items():
        (research / name).write_text(f"{value}\n", encoding="utf-8")

    def fake_run(command: list[str], **_kwargs: object) -> SimpleNamespace:
        if "list-units" in command:
            return SimpleNamespace(
                returncode=0,
                stdout=("rquant-runtime-lab-jobs@svc-lab.service loaded active running fixture\n"),
                stderr="",
            )
        unit = command[2]
        if unit == "rquant-runtime-lab-jobs@svc-lab.service":
            return SimpleNamespace(
                returncode=0,
                stdout=(
                    "LoadState=loaded\nActiveState=active\nSlice=rquant-research.slice\n"
                    "ControlGroup=/rquant.slice/rquant-research.slice/lab.service\n"
                    "ExecStart={ path=/usr/local/libexec/rquant-workload-arbiter ; "
                    "argv[]=/usr/local/libexec/rquant-workload-arbiter research -- "
                    "/fixture ; }\n"
                ),
                stderr="",
            )
        if unit == "rquant-research.slice":
            return SimpleNamespace(
                returncode=0,
                stdout=(
                    "LoadState=loaded\nActiveState=active\n"
                    "ControlGroup=/rquant.slice/rquant-research.slice\n"
                    "CPUWeight=100\nIOWeight=100\nMemoryLow=0\n"
                    f"MemoryHigh={512 * MIB}\nMemoryMax={768 * MIB}\n"
                    "TasksMax=128\nCPUQuotaPerSecUSec=1s\n"
                ),
                stderr="",
            )
        return SimpleNamespace(
            returncode=0,
            stdout="LoadState=loaded\nActiveState=inactive\nControlGroup=\n",
            stderr="",
        )

    monkeypatch.setattr("rquant.workload_isolation.subprocess.run", fake_run)
    arbiter, arbiter_hash = _write_arbiter_provenance(tmp_path)

    result = check_workload_runtime(
        cgroup_root=cgroup_root,
        platform_name="Linux",
        strict=True,
        arbiter_path=arbiter,
        arbiter_hash_path=arbiter_hash,
        arbiter_expected_uid=os.getuid(),
    )

    assert result.status == "fail"
    assert any("must equal exactly one CPU" in detail for detail in result.details)


def test_arbiter_provenance_rejects_tampered_helper_or_self_declared_hash(
    tmp_path: Path,
) -> None:
    from rquant.workload_isolation import _arbiter_provenance_failure

    helper, digest = _write_arbiter_provenance(tmp_path)
    helper.write_bytes(helper.read_bytes() + b"\n# tampered\n")
    digest.chmod(0o644)
    digest.write_text(
        f"{hashlib.sha256(b'self-declared').hexdigest()}\n",
        encoding="ascii",
    )
    digest.chmod(0o444)

    failure = _arbiter_provenance_failure(
        helper,
        digest,
        expected_uid=os.getuid(),
    )

    assert failure is not None
    assert "sha256" in failure


def test_idle_planes_do_not_require_a_materialized_parent_cgroup(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from rquant.workload_isolation import check_workload_runtime

    cgroup_root = tmp_path / "cgroup"
    cgroup_root.mkdir()
    (cgroup_root / "cgroup.controllers").write_text("cpu io memory pids\n", encoding="utf-8")

    def fake_run(command: list[str], **_kwargs: object) -> SimpleNamespace:
        if "list-units" in command:
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        return SimpleNamespace(
            returncode=0,
            stdout="LoadState=loaded\nActiveState=inactive\nControlGroup=\n",
            stderr="",
        )

    monkeypatch.setattr("rquant.workload_isolation.subprocess.run", fake_run)

    result = check_workload_runtime(
        cgroup_root=cgroup_root,
        platform_name="Linux",
        strict=False,
    )

    assert result.status == "ok", result.details
    assert any("parent cgroup not required" in detail for detail in result.details)


def test_runtime_blocks_loaded_legacy_instances(tmp_path: Path, monkeypatch) -> None:
    from rquant.workload_isolation import check_workload_runtime

    cgroup_root = tmp_path / "cgroup"
    cgroup_root.mkdir()
    (cgroup_root / "cgroup.controllers").write_text("cpu io memory pids\n", encoding="utf-8")

    def fake_run(command: list[str], **_kwargs: object) -> SimpleNamespace:
        assert "list-units" in command
        return SimpleNamespace(
            returncode=0,
            stdout="rquant-runtime-live@svc-old.service loaded active running legacy\n",
            stderr="",
        )

    monkeypatch.setattr("rquant.workload_isolation.subprocess.run", fake_run)

    result = check_workload_runtime(
        cgroup_root=cgroup_root,
        platform_name="Linux",
        strict=True,
    )

    assert result.status == "fail"
    assert "legacy runtime migration required" in result.summary


def test_runtime_process_failures_are_structured(tmp_path: Path, monkeypatch) -> None:
    from rquant.workload_isolation import check_workload_runtime

    cgroup_root = tmp_path / "cgroup"
    cgroup_root.mkdir()
    (cgroup_root / "cgroup.controllers").write_text("cpu io memory pids\n", encoding="utf-8")

    def timeout(*_args: object, **_kwargs: object) -> None:
        raise subprocess.TimeoutExpired("/usr/bin/systemctl", 8)

    monkeypatch.setattr("rquant.workload_isolation.subprocess.run", timeout)
    result = check_workload_runtime(
        cgroup_root=cgroup_root,
        platform_name="Linux",
        strict=True,
    )

    assert result.status == "fail"
    assert "TimeoutExpired" in result.summary


def test_non_systemd_linux_skips_relaxed_but_fails_strict(tmp_path: Path) -> None:
    from rquant.workload_isolation import (
        check_workload_capacity_baseline,
        check_workload_runtime,
    )

    relaxed = check_workload_runtime(
        cgroup_root=tmp_path / "not-cgroup-v2",
        platform_name="Linux",
        systemctl_path=Path("/missing/systemctl"),
        strict=False,
    )
    strict = check_workload_runtime(
        cgroup_root=tmp_path / "not-cgroup-v2",
        platform_name="Linux",
        systemctl_path=Path("/missing/systemctl"),
        strict=True,
    )
    relaxed_capacity = check_workload_capacity_baseline(
        data_path=tmp_path / "missing/data",
        meminfo_path=tmp_path / "missing/meminfo",
        platform_name="Linux",
        strict=False,
    )

    assert relaxed.status == "skip"
    assert strict.status == "fail"
    assert relaxed_capacity.status == "skip"


def test_capacity_accepts_reported_memory_of_an_8_gib_class_host(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from rquant.workload_isolation import check_workload_capacity_baseline

    data_path = tmp_path / "data"
    data_path.mkdir()
    meminfo_path = tmp_path / "meminfo"
    meminfo_path.write_text(
        "MemTotal:       7874560 kB\nMemAvailable:    4194304 kB\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "rquant.workload_isolation.shutil.disk_usage",
        lambda _path: SimpleNamespace(free=16 * GIB),
    )

    result = check_workload_capacity_baseline(
        data_path=data_path,
        meminfo_path=meminfo_path,
        platform_name="Linux",
        cpu_count=2,
    )

    assert result.status == "ok", result.details
    assert any("MemTotal=7.5 GiB" in detail for detail in result.details)


def test_research_admission_blocks_maintenance_and_missing_high_water(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from rquant.workload_isolation import check_research_workload_admission

    evidence = tmp_path / "high-water.json"
    _write_high_water(evidence)
    meminfo = tmp_path / "meminfo"
    meminfo.write_text(
        "MemTotal:       7874560 kB\nMemAvailable:    3145728 kB\n",
        encoding="utf-8",
    )

    def inactive(_path: Path, unit: str):
        return {"LoadState": "loaded", "ActiveState": "inactive"}, None, False

    monkeypatch.setattr("rquant.workload_isolation._show_systemd_unit", inactive)
    pending = check_research_workload_admission(
        evidence_path=evidence,
        meminfo_path=meminfo,
        platform_name="Linux",
        as_of=NOW + timedelta(hours=1),
    )

    assert pending.status == "fail", pending.details
    assert "pending calibration" in pending.summary

    def active(_path: Path, unit: str):
        state = "active" if unit == "rquant-backup.service" else "inactive"
        return {"LoadState": "loaded", "ActiveState": state}, None, False

    monkeypatch.setattr("rquant.workload_isolation._show_systemd_unit", active)
    blocked = check_research_workload_admission(
        evidence_path=evidence,
        meminfo_path=meminfo,
        platform_name="Linux",
        as_of=NOW + timedelta(hours=1),
    )
    missing = check_research_workload_admission(
        evidence_path=tmp_path / "missing.json",
        meminfo_path=meminfo,
        platform_name="Linux",
        as_of=NOW + timedelta(hours=1),
    )

    assert blocked.status == "fail"
    assert missing.status == "fail"


def test_health_skip_is_visible_and_never_aggregates_to_ok(monkeypatch) -> None:
    from rquant import health
    from rquant.workload_isolation import WorkloadCheck

    monkeypatch.setattr(
        health,
        "check_workload_runtime",
        lambda: WorkloadCheck("runtime", "skip", "no systemd"),
    )
    monkeypatch.setattr(
        health,
        "check_workload_capacity_baseline",
        lambda: WorkloadCheck("capacity", "ok", "capacity ok"),
    )
    monkeypatch.setattr(
        health,
        "check_workload_high_water_evidence",
        lambda: WorkloadCheck("high_water", "ok", "evidence ok"),
    )

    snapshot = health.get_workload_isolation_snapshot()

    assert snapshot.status == "warn"
    assert "no systemd" in snapshot.detail


def test_preflight_skip_is_nonblocking_but_not_reported_as_all_green() -> None:
    from rquant import preflight

    results = [preflight.CheckResult("workload_runtime", "skip", "no systemd")]

    report = preflight.format_report(results)
    subject, _body = preflight.format_pushdeer_summary(results)

    assert "全部通过" not in report
    assert "未验证" in report
    assert "通过" not in subject
    assert "未验证" in subject


def test_cloud_acceptance_script_is_fixed_path_strict_and_read_only() -> None:
    content = (ROOT / "scripts" / "verify-workload-isolation.sh").read_text(encoding="utf-8")

    assert "PATH=/usr/sbin:/usr/bin:/sbin:/bin" in content
    assert "/usr/bin/systemctl" in content
    assert "/usr/bin/systemd-analyze verify" in content
    assert "/usr/bin/systemd-analyze calendar" in content
    assert "/usr/bin/sha256sum" in content
    assert "/usr/local/libexec/rquant-workload-arbiter.sha256" in content
    assert "/var/lib/rquant/workload-isolation/migration" in content
    assert 'assert_root_mode "${MIGRATION_ROOT}" 700' in content
    assert 'assert_root_mode "${MIGRATION_LOCK}" 600' in content
    assert "migration lock must be a single-link regular file" in content
    assert "check_workload_high_water_evidence" in content
    assert "strict=True" in content
    assert "RQUANT_ROOT" not in content
    assert "systemctl start" not in content
    assert "systemctl restart" not in content
    assert "systemctl stop" not in content
    assert "daemon-reload" not in content


def test_pressure_slo_commands_resolve_cgroups_and_never_create_load() -> None:
    content = (SYSTEMD / "README.md").read_text(encoding="utf-8")

    assert "--property=ControlGroup" in content
    assert '"/sys/fs/cgroup${live_cgroup}/memory.events"' in content
    assert "/sys/fs/cgroup/rquant-live.slice" not in content
    assert "不得**自动生成 CPU、内存或 I/O 负载" in content


def test_research_and_maintenance_units_use_the_lifecycle_arbiter() -> None:
    from rquant.workload_isolation import WORKLOAD_ARBITER_PATH

    for path in SYSTEMD.glob("rquant-*.service"):
        content = path.read_text(encoding="utf-8")
        if "Slice=rquant-research.slice" in content:
            assert f"ExecStart={WORKLOAD_ARBITER_PATH} research -- " in content, path.name
        if "Slice=rquant-maintenance.slice" in content:
            assert f"ExecStart={WORKLOAD_ARBITER_PATH} maintenance " in content, path.name

    for script_name in ("backup-snapshot.sh", "sync-readonly-replica.sh"):
        maintenance_script = (ROOT / "scripts" / script_name).read_text(encoding="utf-8")
        assert "RQUANT_WORKLOAD_ARBITER_HELD" in maintenance_script
        assert "/usr/local/libexec/rquant-workload-arbiter" in maintenance_script
        assert "RQUANT_WORKLOAD_ARBITER:-" not in maintenance_script


def test_sampling_unit_is_installed_but_not_automatically_enabled() -> None:
    service = (SYSTEMD / "rquant-workload-sample.service").read_text(encoding="utf-8")
    timer = (SYSTEMD / "rquant-workload-sample.timer").read_text(encoding="utf-8")

    assert "Slice=rquant-serving.slice" in service
    assert "collect-workload-sample.py" in service
    assert "ReadWritePaths=/var/lib/rquant/workload-isolation" in service
    assert "[Install]" not in service
    assert "[Install]" not in timer
