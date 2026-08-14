"""Contracts for the checked-in full-suite CI shard manifest and runner."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import full_suite_shards as shards


def _write_bundle(root: Path, groups: tuple[tuple[str, ...], ...]) -> dict[str, object]:
    return shards.write_manifest_bundle(
        root,
        selector=(),
        shard_nodeids=groups,
        expected_skips=0,
    )


def test_manifest_rejects_duplicate_missing_and_extra_nodeids(tmp_path: Path) -> None:
    manifest_root = tmp_path / "manifest"
    nodeids = ("tests/a.py::test_a", "tests/b.py::test_b", "tests/c.py::test_c")
    _write_bundle(manifest_root, ((nodeids[0],), (nodeids[1],), (nodeids[2],), ()))

    shards.validate_manifest(manifest_root, nodeids)

    shard = manifest_root / "shard-1.jsonl"
    shard.write_text(json.dumps({"nodeid": nodeids[0]}) + "\n", encoding="utf-8")
    index = json.loads((manifest_root / "index.json").read_text(encoding="utf-8"))
    index["shards"][1]["sha256"] = shards.nodeid_digest((nodeids[0],))
    (manifest_root / "index.json").write_text(json.dumps(index), encoding="utf-8")
    with pytest.raises(shards.ContractError, match="duplicate"):
        shards.validate_manifest(manifest_root, nodeids)

    extra = "tests/extra.py::test_extra"
    shard.write_text(json.dumps({"nodeid": extra}) + "\n", encoding="utf-8")
    index["shards"][1]["sha256"] = shards.nodeid_digest((extra,))
    index["full_suite"]["sha256"] = shards.nodeid_digest((nodeids[0], extra, nodeids[2]))
    (manifest_root / "index.json").write_text(json.dumps(index), encoding="utf-8")
    with pytest.raises(shards.ContractError, match="extra|missing"):
        shards.validate_manifest(manifest_root, nodeids)


def test_argsfile_keeps_an_oversized_nodeid_as_one_argument(tmp_path: Path) -> None:
    nodeid = "tests/test_long.py::test_case[" + ("x" * (1024 * 1024)) + "]"

    with shards.argsfile_for_nodeids((nodeid,), directory=tmp_path) as argsfile:
        lines = argsfile.read_text(encoding="utf-8").splitlines()

    assert len(lines) == 1
    assert shards.parse_argsfile_line(lines[0]) == nodeid


def test_runner_detects_default_collect_drift_before_execution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    test_file = tmp_path / "test_small.py"
    test_file.write_text("def test_one():\n    assert True\n", encoding="utf-8")
    selector = (str(test_file),)
    nodeids = shards.collect_nodeids(selector)
    manifest_root = tmp_path / "manifest"
    shards.write_manifest_bundle(
        manifest_root,
        selector=selector,
        shard_nodeids=(nodeids, (), (), ()),
        expected_skips=0,
    )

    test_file.write_text(
        "def test_one():\n    assert True\n\ndef test_two():\n    assert True\n",
        encoding="utf-8",
    )

    with pytest.raises(shards.ContractError, match="full-suite collection differs"):
        shards.main(
            [
                "check",
                "--manifest-dir",
                str(manifest_root),
                "--shard",
                "0",
            ]
        )


def test_runner_uses_argsfile_and_writes_selection_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    test_file = tmp_path / "test_small.py"
    test_file.write_text("def test_one():\n    assert True\n", encoding="utf-8")
    selector = (str(test_file),)
    nodeids = shards.collect_nodeids(selector)
    manifest_root = tmp_path / "manifest"
    index = shards.write_manifest_bundle(
        manifest_root,
        selector=selector,
        shard_nodeids=(nodeids, (), (), ()),
        expected_skips=0,
    )
    junit = tmp_path / "result.xml"
    evidence = tmp_path / "selection.json"

    assert (
        shards.main(
            [
                "run",
                "--manifest-dir",
                str(manifest_root),
                "--shard",
                "0",
                "--junitxml",
                str(junit),
                "--selection-evidence",
                str(evidence),
                "--basetemp",
                str(tmp_path / "pytest-temp"),
            ]
        )
        == 0
    )
    payload = json.loads(evidence.read_text(encoding="utf-8"))
    assert payload["full_digest"] == index["full_suite"]["sha256"]
    assert payload["shard_digest"] == index["shards"][0]["sha256"]
    assert junit.is_file()


def test_lpt_file_weighting_separates_profile_and_recovery_coordinator() -> None:
    nodeids = (
        "tests/unit/test_runtime_production_profile.py::test_profile",
        "tests/unit/test_runtime_recovery_coordinator.py::test_recovery",
        "tests/unit/test_runtime_recovery_artifacts.py::test_artifacts",
        "tests/unit/test_runtime_recovery_backup.py::test_backup",
        "tests/unit/test_runtime_recovery_service.py::test_service",
    )

    groups = shards.plan_shards(nodeids, shard_count=4)
    locations = {nodeid: shard_id for shard_id, group in enumerate(groups) for nodeid in group}

    assert locations[nodeids[0]] != locations[nodeids[1]]
    assert len({locations[nodeid] for nodeid in nodeids[1:]}) >= 3
