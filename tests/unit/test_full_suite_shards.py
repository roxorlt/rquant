"""Contracts for the checked-in full-suite CI shard manifest and runner."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import full_suite_shards as shards


def _repository_for_manifest(root: Path) -> Path:
    return root.parent / "repository"


def _write_bundle(root: Path, groups: tuple[tuple[str, ...], ...]) -> dict[str, object]:
    repository_root = _repository_for_manifest(root)
    for nodeid in (nodeid for group in groups for nodeid in group):
        relative = nodeid.partition("::")[0]
        path = repository_root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.write_text("def test_case():\n    assert True\n", encoding="utf-8")
    return shards.write_manifest_bundle(
        root,
        selector=(),
        shard_nodeids=groups,
        expected_skips=0,
        repository_root=repository_root,
    )


def test_manifest_rejects_duplicate_missing_and_extra_nodeids(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest_root = tmp_path / "manifest"
    nodeids = ("tests/a.py::test_a", "tests/b.py::test_b", "tests/c.py::test_c")
    _write_bundle(manifest_root, ((nodeids[0],), (nodeids[1],), (nodeids[2],), ()))
    repository_root = _repository_for_manifest(manifest_root)

    def collect(
        _selector: tuple[str, ...],
        *,
        repository_root: Path,
    ) -> tuple[str, ...]:
        assert repository_root == _repository_for_manifest(manifest_root)
        return nodeids

    monkeypatch.setattr(shards, "collect_nodeids", collect)

    shards.validate_manifest(
        manifest_root,
        repository_root=repository_root,
    )

    shard = manifest_root / "shard-1.jsonl"
    shard.write_bytes(_canonical_bytes({"nodeid": nodeids[0]}))
    index = json.loads((manifest_root / "index.json").read_text(encoding="utf-8"))
    index["shards"][1]["sha256"] = shards.nodeid_digest((nodeids[0],))
    (manifest_root / "index.json").write_bytes(_canonical_bytes(index))
    with pytest.raises(shards.ContractError, match="duplicate"):
        shards.validate_manifest(
            manifest_root,
            repository_root=repository_root,
        )

    extra = "tests/extra.py::test_extra"
    extra_path = repository_root / "tests/extra.py"
    extra_path.write_text("def test_extra():\n    assert True\n", encoding="utf-8")
    shard.write_bytes(_canonical_bytes({"nodeid": extra}))
    index["shards"][1]["sha256"] = shards.nodeid_digest((extra,))
    index["full_suite"]["sha256"] = shards.nodeid_digest((nodeids[0], extra, nodeids[2]))
    (manifest_root / "index.json").write_bytes(_canonical_bytes(index))
    with pytest.raises(shards.ContractError, match="extra|missing"):
        shards.validate_manifest(
            manifest_root,
            repository_root=repository_root,
        )


def test_argsfile_keeps_an_oversized_nodeid_as_one_argument(tmp_path: Path) -> None:
    repository_root = _test_repository(tmp_path, "tests/test_long.py")
    nodeid = "tests/test_long.py::test_case[" + ("x" * (1024 * 1024)) + "]"

    with shards.argsfile_for_nodeids(
        (nodeid,),
        directory=tmp_path,
        repository_root=repository_root,
    ) as argsfile:
        lines = argsfile.read_text(encoding="utf-8").splitlines()

    assert len(lines) == 1
    assert shards.parse_argsfile_line(lines[0]) == nodeid


def test_runner_detects_default_collect_drift_before_execution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository_root = _test_repository(tmp_path, "tests/test_small.py")
    nodeids = ("tests/test_small.py::test_one",)
    manifest_root = tmp_path / "manifest"
    shards.write_manifest_bundle(
        manifest_root,
        selector=(),
        shard_nodeids=(nodeids, (), (), ()),
        expected_skips=0,
        repository_root=repository_root,
    )

    monkeypatch.setattr(
        shards,
        "collect_nodeids",
        lambda _selector, *, repository_root: (
            *nodeids,
            "tests/test_small.py::test_two",
        ),
    )

    with pytest.raises(shards.ContractError, match="full-suite collection differs"):
        shards.validate_manifest(manifest_root, repository_root=repository_root)


def test_runner_uses_argsfile_and_writes_selection_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    nodeids = (
        "tests/unit/test_strict_json.py::"
        "test_canonical_json_uses_one_utf8_non_ascii_representation",
    )
    manifest_root = tmp_path / "manifest"
    index = shards.write_manifest_bundle(
        manifest_root,
        selector=(),
        shard_nodeids=(nodeids, (), (), ()),
        expected_skips=0,
    )
    monkeypatch.setattr(shards, "validate_manifest", lambda _root: (index, (nodeids, (), (), ())))
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


def _canonical_bytes(value: object) -> bytes:
    return (shards._canonical_json(value) + "\n").encode("utf-8")


def _test_repository(tmp_path: Path, *relative_paths: str) -> Path:
    root = tmp_path / "repository"
    for relative in relative_paths:
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("def test_case():\n    assert True\n", encoding="utf-8")
    return root


def test_v1_manifest_rejects_selector_injection_and_unknown_index_field(tmp_path: Path) -> None:
    manifest_root = tmp_path / "manifest"
    _write_bundle(manifest_root, (("tests/a.py::test_a",), (), (), ()))
    repository_root = _repository_for_manifest(manifest_root)
    index_path = manifest_root / "index.json"
    index = json.loads(index_path.read_text(encoding="utf-8"))

    index["selector"] = ["--ignore=tests/unit/test_runtime_recovery_coordinator.py"]
    index_path.write_bytes(_canonical_bytes(index))
    with pytest.raises(shards.ContractError, match="selector"):
        shards.load_manifest(manifest_root, repository_root=repository_root)

    index["selector"] = {}
    index_path.write_bytes(_canonical_bytes(index))
    with pytest.raises(shards.ContractError, match="selector"):
        shards.load_manifest(manifest_root, repository_root=repository_root)

    index["selector"] = []
    index["untrusted"] = True
    index_path.write_bytes(_canonical_bytes(index))
    with pytest.raises(shards.ContractError, match="field|index"):
        shards.load_manifest(manifest_root, repository_root=repository_root)


def test_manifest_rejects_duplicate_json_keys_and_unknown_jsonl_field(tmp_path: Path) -> None:
    manifest_root = tmp_path / "manifest"
    _write_bundle(manifest_root, (("tests/a.py::test_a",), (), (), ()))
    repository_root = _repository_for_manifest(manifest_root)
    index_path = manifest_root / "index.json"
    raw_index = index_path.read_bytes()
    index_path.write_bytes(raw_index.replace(b'"selector":[]', b'"selector":[],"selector":[]'))

    with pytest.raises(shards.ContractError, match="duplicate"):
        shards.load_manifest(manifest_root, repository_root=repository_root)

    _write_bundle(manifest_root, (("tests/a.py::test_a",), (), (), ()))
    (manifest_root / "shard-0.jsonl").write_bytes(
        _canonical_bytes({"nodeid": "tests/a.py::test_a", "unknown": 1})
    )
    with pytest.raises(shards.ContractError, match="field|record"):
        shards.load_manifest(manifest_root, repository_root=repository_root)


def test_manifest_rejects_noncanonical_json_and_line_endings(tmp_path: Path) -> None:
    manifest_root = tmp_path / "manifest"
    _write_bundle(manifest_root, (("tests/a.py::test_a",), (), (), ()))
    repository_root = _repository_for_manifest(manifest_root)
    index_path = manifest_root / "index.json"
    index_path.write_bytes(index_path.read_bytes().replace(b'"selector":[]', b'"selector": []'))

    with pytest.raises(shards.ContractError, match="canonical"):
        shards.load_manifest(manifest_root, repository_root=repository_root)

    index_path.write_bytes(b"\xff\n")
    with pytest.raises(shards.ContractError, match="UTF-8"):
        shards.load_manifest(manifest_root, repository_root=repository_root)

    _write_bundle(manifest_root, (("tests/a.py::test_a",), (), (), ()))
    shard_path = manifest_root / "shard-0.jsonl"
    shard_path.write_bytes(shard_path.read_bytes().replace(b"\n", b"\r\n"))
    with pytest.raises(shards.ContractError, match="canonical|line ending"):
        shards.load_manifest(manifest_root, repository_root=repository_root)


@pytest.mark.parametrize(
    "nodeid",
    (
        "/tmp/outside.py::test_case",
        "../outside.py::test_case",
        "tests/../outside.py::test_case",
        "--junitxml=other.py::test_case",
        "tests/a.py::test_case\x00",
        "tests/a.py::test_case\x1f",
        "tests/a.py::test_case\x7f",
    ),
)
def test_nodeid_rejects_path_option_and_control_injection(
    tmp_path: Path,
    nodeid: str,
) -> None:
    repository_root = _test_repository(tmp_path, "tests/a.py")

    with pytest.raises(shards.ContractError, match="nodeid|path|control|option"):
        shards._file_for_nodeid(nodeid, repository_root=repository_root)


def test_nodeid_rejects_symlink_escape_but_accepts_parameter_punctuation(tmp_path: Path) -> None:
    repository_root = _test_repository(tmp_path, "tests/test_params.py")
    outside = tmp_path / "outside.py"
    outside.write_text("def test_case():\n    assert True\n", encoding="utf-8")
    (repository_root / "tests/escape.py").symlink_to(outside)

    with pytest.raises(shards.ContractError, match="symlink|outside"):
        shards._file_for_nodeid(
            "tests/escape.py::test_case",
            repository_root=repository_root,
        )
    with pytest.raises(shards.ContractError, match="control|line"):
        shards._file_for_nodeid(
            "tests/test_params.py::test_case[line\u2028break]",
            repository_root=repository_root,
        )

    valid = "tests/test_params.py::test_case[--ignore=elsewhere !@#$%^&*()[]{}:,.?]"
    assert shards._file_for_nodeid(valid, repository_root=repository_root) == (
        "tests/test_params.py"
    )


def test_nodeid_size_limit_accepts_boundary_and_rejects_one_byte_over(tmp_path: Path) -> None:
    repository_root = _test_repository(tmp_path, "tests/test_size.py")
    prefix = "tests/test_size.py::test_case["
    suffix = "]"
    boundary = prefix + ("x" * (shards.MAX_NODEID_BYTES - len(prefix) - len(suffix))) + suffix

    assert len(boundary.encode("utf-8")) == shards.MAX_NODEID_BYTES
    assert shards._file_for_nodeid(boundary, repository_root=repository_root) == (
        "tests/test_size.py"
    )
    with pytest.raises(shards.ContractError, match="size"):
        shards._file_for_nodeid(boundary + "x", repository_root=repository_root)


def test_manifest_line_and_total_size_limits_fail_closed(tmp_path: Path) -> None:
    manifest_root = tmp_path / "manifest"
    _write_bundle(manifest_root, (("tests/a.py::test_a",), (), (), ()))
    repository_root = _repository_for_manifest(manifest_root)
    size_test_path = repository_root / "tests/test_size.py"
    size_test_path.write_text("def test_case():\n    assert True\n", encoding="utf-8")
    (manifest_root / "shard-0.jsonl").write_bytes(b"x" * (shards.MAX_JSONL_LINE_BYTES + 1))
    with pytest.raises(shards.ContractError, match="line|size"):
        shards.load_manifest(manifest_root, repository_root=repository_root)

    prefix = "tests/test_size.py::test_case["
    suffix = "]"
    payload_size = shards.MAX_NODEID_BYTES - len(prefix) - len(suffix)
    nodeids = tuple(prefix + (str(index) + "x" * (payload_size - 1)) + suffix for index in range(4))
    with pytest.raises(shards.ContractError, match="total size"):
        shards.write_manifest_bundle(
            tmp_path / "oversized-manifest",
            selector=(),
            shard_nodeids=tuple((nodeid,) for nodeid in nodeids),
            expected_skips=0,
            repository_root=repository_root,
        )


def test_validate_manifest_collects_only_the_default_universe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository_root = _test_repository(tmp_path, "tests/test_default.py")
    manifest_root = tmp_path / "manifest"
    nodeids = ("tests/test_default.py::test_case",)
    shards.write_manifest_bundle(
        manifest_root,
        selector=(),
        shard_nodeids=(nodeids, (), (), ()),
        expected_skips=0,
        repository_root=repository_root,
    )
    calls: list[tuple[str, ...]] = []
    expected_root = repository_root

    def collect(
        selector: tuple[str, ...],
        *,
        repository_root: Path,
    ) -> tuple[str, ...]:
        assert repository_root == expected_root
        calls.append(selector)
        return nodeids

    monkeypatch.setattr(shards, "collect_nodeids", collect)

    shards.validate_manifest(
        manifest_root,
        repository_root=repository_root,
    )

    assert calls == [()]
