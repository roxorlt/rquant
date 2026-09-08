"""Static contracts for the parent and four systemd workload planes."""

from __future__ import annotations

import configparser
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SYSTEMD = ROOT / "deploy" / "systemd"
SLICE_NAMES = ("live", "serving", "research", "maintenance")


def _load_slice(name: str) -> configparser.ConfigParser:
    parser = configparser.ConfigParser(interpolation=None)
    parser.optionxform = str
    path = SYSTEMD / f"rquant-{name}.slice"
    with path.open(encoding="utf-8") as stream:
        parser.read_file(stream)
    return parser


def _memory_bytes(value: str) -> int:
    from rquant.workload_isolation import parse_systemd_bytes

    return parse_systemd_bytes(value)


def _percent(value: str) -> float:
    assert value.endswith("%")
    return float(value.removesuffix("%"))


def test_slices_are_resource_only_and_have_no_runtime_capabilities() -> None:
    resource_directives = {
        "CPUAccounting",
        "CPUQuota",
        "CPUWeight",
        "IOAccounting",
        "IOWeight",
        "MemoryAccounting",
        "MemoryHigh",
        "MemoryLow",
        "MemoryMax",
        "TasksAccounting",
        "TasksMax",
    }
    forbidden_directives = {
        "After",
        "Before",
        "Environment",
        "EnvironmentFile",
        "ExecStart",
        "ExecStop",
        "Group",
        "NetworkNamespacePath",
        "Requires",
        "User",
        "Wants",
    }

    for name in SLICE_NAMES:
        parser = _load_slice(name)
        assert set(parser.sections()) == {"Unit", "Slice"}
        assert set(parser["Unit"]) == {"Description"}
        assert set(parser["Slice"]) <= resource_directives
        assert forbidden_directives.isdisjoint(parser["Slice"])
        assert "Service" not in parser
        assert "Install" not in parser


def test_plane_priority_descends_from_live_to_serving_to_background() -> None:
    live = _load_slice("live")["Slice"]
    serving = _load_slice("serving")["Slice"]
    research = _load_slice("research")["Slice"]
    maintenance = _load_slice("maintenance")["Slice"]

    # CPU weights: maintenance sits above research since #243. A 10 GB snapshot
    # took 8m16s-8m50s and twice missed its 10min timeout at CPUWeight=50, which
    # is 3.0% of the parent's runnable share; it is 15.8% at 300, still behind
    # the two planes that serve the market.
    assert (
        int(live["CPUWeight"])
        > int(serving["CPUWeight"])
        > int(maintenance["CPUWeight"])
        > int(research["CPUWeight"])
    )
    assert (
        int(live["IOWeight"])
        > int(serving["IOWeight"])
        > int(research["IOWeight"])
        > int(maintenance["IOWeight"])
    )
    # The two planes that can run at the same time as a maintenance job together
    # hold at most one of the host's two cores. Research is quota'd separately at
    # exactly one core and is mutually exclusive with maintenance through the
    # arbiter, so it never overlaps a backup. Maintenance itself stays unquoted:
    # a bounded batch job should be able to use whatever the caps leave behind.
    assert _percent(live["CPUQuota"]) + _percent(serving["CPUQuota"]) <= 100
    assert "CPUQuota" not in maintenance
    assert "MemoryLow" in live
    assert _memory_bytes(live["MemoryLow"]) > 0
    assert "MemoryLow" not in serving
    assert "MemoryLow" not in research


def test_serving_uses_reclaim_without_an_unsafe_hard_cap() -> None:
    serving = _load_slice("serving")["Slice"]

    assert _memory_bytes(serving["MemoryHigh"]) > 0
    assert "MemoryMax" not in serving
    assert int(serving["TasksMax"]) > 0


def test_research_is_preemptible_and_hard_limited_with_portable_values() -> None:
    research = _load_slice("research")["Slice"]

    assert _percent(research["CPUQuota"]) == 100
    assert 0 < _memory_bytes(research["MemoryHigh"]) < _memory_bytes(research["MemoryMax"])
    assert _memory_bytes(research["MemoryMax"]) < 4 * 1024**3
    assert 1 <= int(research["IOWeight"]) < 500
    assert 1 <= int(research["TasksMax"]) <= 512


def test_parent_protects_live_and_maintenance_remains_pending_calibration() -> None:
    from rquant.workload_isolation import (
        WORKLOAD_MEMORY_BUDGET_MIB,
        parse_systemd_bytes,
        verify_workload_memory_admission,
    )

    parser = configparser.ConfigParser(interpolation=None)
    parser.optionxform = str
    parser.read(SYSTEMD / "rquant.slice")
    parent = parser["Slice"]
    budget = WORKLOAD_MEMORY_BUDGET_MIB

    assert parse_systemd_bytes(parent["MemoryLow"]) >= parse_systemd_bytes(
        _load_slice("live")["Slice"]["MemoryLow"]
    )
    assert "MemoryMax" not in parent
    assert "MemoryMax" not in _load_slice("live")["Slice"]
    assert "MemoryMax" not in _load_slice("serving")["Slice"]
    assert "MemoryMax" not in _load_slice("maintenance")["Slice"]
    assert (
        budget["live"] + budget["serving"] + budget["research"] + budget["os"]
        <= budget["usable_host"]
    )
    assert "maintenance" not in budget
    result = verify_workload_memory_admission(SYSTEMD)
    assert result.status == "warn", result.details
    assert "pending" in result.summary
