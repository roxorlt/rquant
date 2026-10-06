"""Trusted CPU pairs: independent arithmetic and incomplete-kernel failures."""

from __future__ import annotations

import importlib
import importlib.util
from datetime import UTC, datetime, timedelta
from types import ModuleType
from typing import TYPE_CHECKING
from pathlib import Path
import os

import pytest

if TYPE_CHECKING:
    from rquant.task_cpu import TaskCpuEvidence, TaskCpuResult

NOW = datetime(2026, 10, 6, 0, 0, tzinfo=UTC)
SLICES = (
    "rquant.slice", "rquant-live.slice", "rquant-serving.slice",
    "rquant-research.slice", "rquant-maintenance.slice",
)


def _api() -> ModuleType:
    assert importlib.util.find_spec("rquant.task_cpu") is not None, "trusted CPU pair missing"
    return importlib.import_module("rquant.task_cpu")


def _observation(*, later: bool = False, delta: int = 400_000) -> dict[str, object]:
    at = NOW + timedelta(seconds=2 if later else 0)
    paths = ["/", "/rquant.slice"] + [f"/rquant.slice/{s}" for s in SLICES[1:]]
    nodes = [
        {
            "path": path, "device": 7, "inode": i + 10,
            "controllers": ("cpu", "cpuset", "memory") if i < 2 else ("cpu", "memory"),
            "subtree_control": ("cpu", "cpuset", "memory") if i == 0 else ("cpu", "memory"),
            "cpu_max": None if i == 0 else ("50000 100000" if i == 3 else "max 100000"),
            "cpu_max_error": "absent" if i == 0 else None,
            "cpuset_effective": "0-3" if i < 2 else None,
            "cpuset_error": None if i < 2 else "absent",
        }
        for i, path in enumerate(paths)
    ]
    identities = ((100, 100, 123, 5, 50, 2), (200, 200, 456, 5, 60, 3))
    groups = [
        {
            "slice_name": s, "node_index": 1 + i,
            "invocation_id": str(i + 1) * 32,
            "usage_usec": 1_000_000 + (delta if later else 0),
            "observed_at": at, "monotonic_ns": 1_000_000_000 + (2_000_000_000 if later else 0),
            "boottime_ns": 1_000_000_000 + (2_000_000_000 if later else 0),
            "members_before": (0, 1) if i == 0 else ((0,) if i == 1 else ((1,) if i == 2 else ())),
            "members_after": (0, 1) if i == 0 else ((0,) if i == 1 else ((1,) if i == 2 else ())),
            "error": None,
        }
        for i, s in enumerate(SLICES)
    ]
    return {
        "contract": "task-cpu/v1", "collector_digest": "c" * 64,
        "host_name": "synthetic-test", "boot_id": "12345678-1234-1234-1234-123456789abc",
        "manifest_digest": "a" * 64, "observed_at": at,
        "monotonic_ns": 1_000_000_000 + (2_000_000_000 if later else 0),
        "boottime_ns": 1_000_000_000 + (2_000_000_000 if later else 0),
        "mount_id": 25, "mount_device": "0:27", "mount_root": "/",
        "mount_path": "/sys/fs/cgroup", "filesystem": "cgroup2",
        "online_cpus": "0-3", "nodes": nodes,
        "identity_table": identities, "affinity_table": ("0-3",),
        "threads": ((0, 0, 0, 0), (1, 1, 0, 0)), "groups": groups,
    }


def _compute(first: dict[str, object] | None = None, second: dict[str, object] | None = None) -> TaskCpuEvidence:
    api = _api()
    pair = api.TaskCpuPair(previous=first or _observation(), current=second or _observation(later=True))
    return api.compute_task_cpu(pair, cutoff=NOW + timedelta(seconds=2))


def _serving(result: TaskCpuEvidence) -> TaskCpuResult:
    return next(row for row in result.groups if row.slice_name == "rquant-serving.slice")


@pytest.mark.parametrize("quota,delta,expected", [("50000 100000", 400_000, "40.0"), ("30000 100000", 570_000, "95.0")])
def test_tsc_04_manual_reference(quota: str, delta: int, expected: str) -> None:
    first, second = _observation(), _observation(later=True, delta=delta)
    first["nodes"][3]["cpu_max"] = second["nodes"][3]["cpu_max"] = quota
    actual = _serving(_compute(first, second))
    # Independent hand formula: .4 CPU seconds/(2 seconds*.5) or .57/(2*.3).
    assert actual.percent == expected
    assert actual.reason is None
    assert actual.capacity == ("1/2" if quota.startswith("50000") else "3/10")


def test_tsc_04_root_and_inherited_cpuset() -> None:
    actual = _serving(_compute())
    assert actual.percent == "40.0"
    assert actual.effective_cpus == (0, 1, 2, 3)


@pytest.mark.parametrize("index,field,value", [(3, "cpu_max", None), (1, "cpuset_effective", None), (3, "controllers", ("cpu", "cpuset")), (0, "cpu_max_error", "permission")])
def test_tsc_04_missing_not_unlimited(index: int, field: str, value: object) -> None:
    first, second = _observation(), _observation(later=True)
    for sample in (first, second):
        sample["nodes"][index][field] = value
        if field == "cpu_max": sample["nodes"][index]["cpu_max_error"] = "absent"
        if field == "cpuset_effective": sample["nodes"][index]["cpuset_error"] = "absent"
    actual = _serving(_compute(first, second))
    assert actual.percent is None
    assert actual.reason == "capacity_incomplete"


def test_tsc_04_affinity_and_empty() -> None:
    first, second = _observation(), _observation(later=True)
    for sample in (first, second):
        sample["nodes"][3]["cpu_max"] = "max 100000"
        sample["affinity_table"] = ("0",)
    actual = _serving(_compute(first, second))
    assert actual.percent == "20.0" and actual.capacity == "1"
    assert all(row.reason == "empty_members" for row in _compute().groups[-2:])


@pytest.mark.parametrize("delta,expected,reason", [(0, "0.0", None), (1_000_000, "100.0", None), (1_001_000, None, "capacity_mismatch")])
def test_tsc_04_zero_and_over_100(delta: int, expected: str | None, reason: str | None) -> None:
    actual = _serving(_compute(second=_observation(later=True, delta=delta)))
    assert (actual.percent, actual.reason) == (expected, reason)


@pytest.mark.parametrize("field,value", [("boot_id", "87654321-1234-1234-1234-123456789abc"), ("host_name", "other"), ("collector_digest", "d" * 64), ("manifest_digest", "b" * 64)])
def test_tsc_03_counter_reset_identity(field: str, value: object) -> None:
    second = _observation(later=True); second[field] = value
    assert _serving(_compute(second=second)).reason == "identity_changed"
    second = _observation(later=True); second["groups"][2]["usage_usec"] = 999_999
    assert _serving(_compute(second=second)).reason == "counter_reset"


@pytest.mark.parametrize("delta_ns", [0, 499_999_999, 120_000_000_001])
def test_tsc_03_clock_discontinuity(delta_ns: int) -> None:
    second = _observation(later=True)
    for group in second["groups"]:
        group["monotonic_ns"] = group["boottime_ns"] = 1_000_000_000 + delta_ns
    second["monotonic_ns"] = second["boottime_ns"] = 1_000_000_000 + max(delta_ns, 0)
    assert _serving(_compute(second=second)).reason == "clock_discontinuity"


def test_tsc_03_thread_reuse_capacity() -> None:
    second = _observation(later=True)
    second["identity_table"] = ((100, 100, 123, 5, 50, 2), (200, 200, 457, 5, 60, 3))
    assert _serving(_compute(second=second)).reason == "identity_changed"
    second = _observation(later=True); second["nodes"][3]["inode"] += 1
    assert _serving(_compute(second=second)).reason == "identity_changed"
    second = _observation(later=True); second["affinity_table"] = ("0",)
    assert _serving(_compute(second=second)).reason == "capacity_changed"


def test_m13_final_02_swapped_physical_thread_affinities_stay_unknown_and_next_pair_recovers() -> None:
    first, second = _observation(), _observation(later=True)
    for sample in (first, second):
        sample["affinity_table"] = ("0", "1")
    first["threads"] = ((0, 0, 0, 0), (1, 1, 1, 1))
    second["threads"] = ((0, 0, 1, 1), (1, 1, 0, 0))
    changed = _compute(first, second)
    assert changed.groups[0].percent is None
    assert changed.groups[0].reason == "capacity_changed"
    assert _serving(changed).reason == "capacity_changed"
    third = _observation(later=True, delta=800_000)
    third["affinity_table"] = second["affinity_table"]
    third["threads"] = second["threads"]
    for row in (third, *third["groups"]):
        row["observed_at"] = NOW + timedelta(seconds=4)
        row["monotonic_ns"] = row["boottime_ns"] = 5_000_000_000
    api = _api()
    stable = api.compute_task_cpu(api.TaskCpuPair(previous=second, current=third), cutoff=NOW + timedelta(seconds=4))
    assert (stable.groups[0].percent, stable.groups[0].capacity, stable.groups[0].effective_cpus) == ("10.0", "2", (0, 1))
    assert _serving(stable).percent == "40.0" and _serving(stable).reason is None


def test_m13_final_02_normalized_thread_masks_do_not_create_false_reset() -> None:
    first, second = _observation(), _observation(later=True)
    first["affinity_table"] = ("0-1", "2-3")
    first["threads"] = ((0, 0, 0, 0), (1, 1, 1, 1))
    second["affinity_table"] = ("3,2", "1,0")
    second["threads"] = ((0, 0, 1, 1), (1, 1, 0, 0))
    stable = _compute(first, second)
    assert (stable.groups[0].percent, stable.groups[0].capacity) == ("5.0", "4")
    assert stable.groups[0].reason is None and _serving(stable).percent == "40.0"


@pytest.mark.parametrize("field,value", [("usage_usec", "9" * 20), ("usage_usec", True), ("boottime_ns", 2**63)])
def test_tsc_02_numeric_budget(field: str, value: object) -> None:
    api = _api(); sample = _observation(); sample["groups"][2][field] = value
    with pytest.raises(ValueError): api.TaskCpuObservation.model_validate(sample)


def test_tsc_02_numeric_budget_cpu_range_and_threads() -> None:
    api = _api(); sample = _observation(); sample["online_cpus"] = "0-1000000000"
    with pytest.raises(ValueError): api.TaskCpuObservation.model_validate(sample)
    sample = _observation(); sample["threads"] = ((0, 0, 0, 0),) * 1025
    with pytest.raises(ValueError): api.TaskCpuObservation.model_validate(sample)


def test_first_sample_stays_unknown_and_future_material_is_rejected() -> None:
    api = _api(); current = _observation(later=True)
    result = api.compute_task_cpu(api.TaskCpuPair(previous=None, current=current), cutoff=NOW + timedelta(seconds=2))
    assert all(row.reason == "first_sample" for row in result.groups)
    assert all(row.percent is None for row in result.groups)
    with pytest.raises(ValueError, match="cutoff"):
        api.compute_task_cpu(api.TaskCpuPair(previous=_observation(), current=current), cutoff=NOW)


def test_raw_fact_corruption_closes_pair() -> None:
    api = _api(); sample = _observation()
    sample["groups"][2]["members_after"] = ()
    with pytest.raises(ValueError, match="member"):
        api.TaskCpuObservation.model_validate(sample)
    sample = _observation(); sample["threads"] = ((0, 0, 0, 0), (1, 0, 0, 0))
    with pytest.raises(ValueError, match="identity"):
        api.TaskCpuObservation.model_validate(sample)


def _kernel_tree(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, dict[str, str]]:
    api = _api()
    cgroup, proc = tmp_path / "cgroup", tmp_path / "proc"
    cgroup.mkdir(); proc.mkdir()
    paths = [cgroup, cgroup / "rquant.slice"] + [cgroup / "rquant.slice" / s for s in SLICES[1:]]
    for i, path in enumerate(paths):
        path.mkdir(parents=True, exist_ok=True)
        (path / "cpu.stat").write_text("usage_usec 1000000\nuser_usec 0\nsystem_usec 0\n")
        (path / "cgroup.procs").write_text("200\n" if i == 3 else "")
        (path / "cgroup.controllers").write_text("cpu cpuset memory" if i < 2 else "cpu memory")
        (path / "cgroup.subtree_control").write_text("cpu cpuset memory" if i == 0 else "cpu memory")
        if i: (path / "cpu.max").write_text("30000 100000" if i == 3 else "max 100000")
        if i < 2: (path / "cpuset.cpus.effective").write_text("0-3")
    for tid in (200, 201):
        task = proc / "200" / "task" / str(tid)
        task.mkdir(parents=True)
        (task / "stat").write_text(f"{tid} (opaque command with space) " + " ".join(["0"] * 19 + ["123"]))
        (task / "cgroup").write_text("0::/rquant.slice/rquant-serving.slice\n")
    boot = proc / "sys" / "kernel" / "random" / "boot_id"
    boot.parent.mkdir(parents=True); boot.write_text("12345678-1234-1234-1234-123456789abc\n")
    info = proc / str(os.getpid()) / "mountinfo"
    info.parent.mkdir(parents=True)
    dev = cgroup.stat().st_dev
    info.write_text(f"25 20 {os.major(dev)}:{os.minor(dev)} / /sys/fs/cgroup rw - cgroup2 cgroup rw\n")
    online = tmp_path / "online"; online.write_text("0-3")
    monkeypatch.setattr(api, "_CGROUP_ROOT", cgroup)
    monkeypatch.setattr(api, "_PROC_ROOT", proc)
    monkeypatch.setattr(api, "_ONLINE_PATH", online)
    monkeypatch.setattr(api.os, "sched_getaffinity", lambda _tid: {0, 1, 2, 3}, raising=False)
    monkeypatch.setattr(api, "_boottime_ns", lambda: 1_000_000_000)
    properties = {s: {"ControlGroup": "/rquant.slice" if i == 0 else f"/rquant.slice/{s}", "InvocationID": str(i + 1) * 32, "LoadState": "loaded"} for i, s in enumerate(SLICES)}
    return properties


def test_kernel_reader_enumerates_each_actual_thread_and_compacts_all_facts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    api = _api()
    assert hasattr(api, "LinuxTaskCpuReader"), "fixed kernel CPU reader missing"
    properties = _kernel_tree(tmp_path, monkeypatch)
    sample = api.LinuxTaskCpuReader().capture(host_name="synthetic-test", boot_id="12345678-1234-1234-1234-123456789abc", manifest_digest="a" * 64, properties=properties, max_seconds=1, clock=lambda: NOW)
    assert len(sample.threads) == 2
    assert {identity[1] for identity in sample.identity_table} == {200, 201}
    assert sample.groups[0].members_before == sample.groups[2].members_before
    assert sample.nodes[0].cpu_max_error == "absent"
    assert sample.nodes[sample.groups[2].node_index].cpuset_error == "absent"
    assert api.TaskCpuPair(previous=sample, current=sample).current == sample


def test_kernel_reader_refuses_incomplete_mount_and_budget_before_any_publication(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    api = _api()
    assert hasattr(api, "LinuxTaskCpuReader"), "fixed kernel CPU reader missing"
    properties = _kernel_tree(tmp_path, monkeypatch)
    kwargs = dict(host_name="synthetic-test", boot_id="12345678-1234-1234-1234-123456789abc", manifest_digest="a" * 64, properties=properties, clock=lambda: NOW)
    with pytest.raises(TimeoutError): api.LinuxTaskCpuReader().capture(**kwargs, max_seconds=0)
    (api._PROC_ROOT / str(os.getpid()) / "mountinfo").write_text("25 20 0:27 /wrong /sys/fs/cgroup rw - cgroup2 cgroup rw\n")
    with pytest.raises(ValueError, match="mount"): api.LinuxTaskCpuReader().capture(**kwargs, max_seconds=1)


def test_kernel_reader_refuses_wrong_cgroup_and_symlink(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    api = _api()
    assert hasattr(api, "LinuxTaskCpuReader"), "fixed kernel CPU reader missing"
    properties = _kernel_tree(tmp_path, monkeypatch)
    kwargs = dict(host_name="synthetic-test", boot_id="12345678-1234-1234-1234-123456789abc", manifest_digest="a" * 64, properties=properties, max_seconds=1, clock=lambda: NOW)
    properties[SLICES[2]]["ControlGroup"] = "/somewhere-else"
    with pytest.raises(ValueError, match="cgroup"): api.LinuxTaskCpuReader().capture(**kwargs)
    properties[SLICES[2]]["ControlGroup"] = "/rquant.slice/rquant-serving.slice"
    target = api._CGROUP_ROOT / "rquant.slice" / SLICES[2] / "cpu.stat"
    target.unlink(); target.symlink_to(tmp_path / "online")
    with pytest.raises((ValueError, OSError)): api.LinuxTaskCpuReader().capture(**kwargs)
