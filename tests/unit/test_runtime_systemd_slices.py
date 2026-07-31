"""Static contracts for the three systemd workload-isolation planes."""

from __future__ import annotations

import configparser
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SYSTEMD = ROOT / "deploy" / "systemd"
SLICE_NAMES = ("live", "serving", "research")


def _load_slice(name: str) -> configparser.ConfigParser:
    parser = configparser.ConfigParser(interpolation=None)
    parser.optionxform = str
    path = SYSTEMD / f"rquant-{name}.slice"
    with path.open(encoding="utf-8") as stream:
        parser.read_file(stream)
    return parser


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


def test_plane_priority_descends_from_live_to_serving_to_research() -> None:
    live = _load_slice("live")["Slice"]
    serving = _load_slice("serving")["Slice"]
    research = _load_slice("research")["Slice"]

    assert int(live["CPUWeight"]) > int(serving["CPUWeight"]) > int(
        research["CPUWeight"]
    )
    assert int(live["IOWeight"]) > int(serving["IOWeight"]) > int(
        research["IOWeight"]
    )
    assert "CPUQuota" not in live
    assert "CPUQuota" not in serving
    assert "MemoryLow" in live
    assert _percent(live["MemoryLow"]) > 0
    assert "MemoryLow" not in serving
    assert "MemoryLow" not in research


def test_serving_is_bounded_without_competing_with_live_protection() -> None:
    serving = _load_slice("serving")["Slice"]

    assert _percent(serving["MemoryHigh"]) > 0
    assert _percent(serving["MemoryHigh"]) < _percent(serving["MemoryMax"])
    assert int(serving["TasksMax"]) > 0


def test_research_is_preemptible_and_hard_limited_with_portable_values() -> None:
    research = _load_slice("research")["Slice"]

    assert 0 < _percent(research["CPUQuota"]) <= 100
    assert 0 < _percent(research["MemoryHigh"]) < _percent(
        research["MemoryMax"]
    )
    assert _percent(research["MemoryMax"]) < 100
    assert 1 <= int(research["IOWeight"]) < 500
    assert 1 <= int(research["TasksMax"]) <= 512
