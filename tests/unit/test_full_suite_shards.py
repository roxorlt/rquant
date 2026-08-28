"""Contracts for the checked-in full-suite CI shard manifest and runner."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import xml.etree.ElementTree as ElementTree
from pathlib import Path

import pytest

from scripts import full_suite_shards as shards

SENTINEL_ENVIRONMENT = {
    "TUSHARE_TOKEN_BACKUP": "tushare-backup-sentinel",
    "PUSHDEER_KEYS": "pushdeer-sentinel",
    "AWS_ACCESS_KEY_ID": "aws-access-key-sentinel",
    "RQUANT_PANORAMA_GATE_TOKEN": "panorama-gate-sentinel",
    "GITHUB_ACTIONS": "github-actions-sentinel",
    "RUNNER_TEMP": "runner-temp-sentinel",
}


def _write_environment_isolation_repository(root: Path) -> Path:
    tests_root = root / "tests"
    tests_root.mkdir(parents=True)
    forbidden = repr(tuple(SENTINEL_ENVIRONMENT))
    (tests_root / "conftest.py").write_text(
        "\n".join(
            (
                "import os",
                f"_FORBIDDEN = {forbidden}",
                "_present = [name for name in _FORBIDDEN if name in os.environ]",
                "assert not _present, (",
                "    'forbidden environment variable names: ' + ', '.join(_present)",
                ")",
                "assert os.environ['PYTHONNOUSERSITE'] == '1'",
                "assert os.environ['PYTEST_DISABLE_PLUGIN_AUTOLOAD'] == '1'",
                "assert os.environ['PYTEST_ADDOPTS'] == ''",
                "",
            )
        ),
        encoding="utf-8",
    )
    test_path = tests_root / "test_execution_environment.py"
    test_path.write_text(
        "\n".join(
            (
                "from pathlib import Path",
                "import os",
                f"_FORBIDDEN = {forbidden}",
                "",
                "def test_execution_environment():",
                "    present = [name for name in _FORBIDDEN if name in os.environ]",
                "    assert not present, (",
                "        'forbidden environment variable names: ' + ', '.join(present)",
                "    )",
                "    assert os.environ['PYTEST_ADDOPTS'] == ''",
                "    Path(__file__).with_name('execution-proof.txt').write_text(",
                "        'execution environment isolated\\n', encoding='utf-8'",
                "    )",
                "",
            )
        ),
        encoding="utf-8",
    )
    return test_path


def _repository_for_manifest(root: Path) -> Path:
    return root.parent / "repository"


def _write_approved_skips(root: Path) -> None:
    """Every manifest carries an approved-skip map; these fixtures approve none."""
    root.mkdir(parents=True, exist_ok=True)
    shards.approved_skips_path(root).write_text(
        json.dumps(
            {"platforms": {"darwin": {}, "linux": {}}, "schema_version": 1},
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )


def _write_bundle(root: Path, groups: tuple[tuple[str, ...], ...]) -> dict[str, object]:
    repository_root = _repository_for_manifest(root)
    for nodeid in (nodeid for group in groups for nodeid in group):
        relative = nodeid.partition("::")[0]
        path = repository_root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.write_text("def test_case():\n    assert True\n", encoding="utf-8")
    _write_approved_skips(root)
    return shards.write_manifest_bundle(
        root,
        selector=(),
        shard_nodeids=groups,
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
    shard.write_bytes(
        _canonical_bytes({"nodeid": nodeids[0], "junit": shards.junit_identity(nodeids[0])})
    )
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
    _write_approved_skips(manifest_root)
    shards.write_manifest_bundle(
        manifest_root,
        selector=(),
        shard_nodeids=(nodeids, (), (), ()),
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
    repository_root = (tmp_path / "repository").resolve()
    test_path = _write_environment_isolation_repository(repository_root)
    nodeids = ("tests/test_execution_environment.py::test_execution_environment",)
    manifest_root = tmp_path / "manifest"
    _write_approved_skips(manifest_root)
    index = shards.write_manifest_bundle(
        manifest_root,
        selector=(),
        shard_nodeids=(nodeids, (), (), ()),
        repository_root=repository_root,
    )
    for name, value in SENTINEL_ENVIRONMENT.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("PYTEST_ADDOPTS", "--tb=short")
    junit = tmp_path / "result.xml"
    evidence = tmp_path / "selection.json"

    assert shards.collect_nodeids((), repository_root=repository_root) == nodeids

    assert (
        shards.run_shard(
            manifest_root=manifest_root,
            shard_id=0,
            mode="run",
            junitxml=junit,
            selection_evidence=evidence,
            basetemp=tmp_path / "pytest-temp",
            repository_root=repository_root,
        )
        == 0
    )
    payload = json.loads(evidence.read_text(encoding="utf-8"))
    assert payload["full_digest"] == index["full_suite"]["sha256"]
    assert payload["shard_digest"] == index["shards"][0]["sha256"]
    assert junit.is_file()
    assert test_path.with_name("execution-proof.txt").read_text(encoding="utf-8") == (
        "execution environment isolated\n"
    )


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


# A parametrized id is opaque: pytest keeps whatever the ids= tuple says, so it can
# carry the node separator, brackets, and option-looking text. Reporting the wrong
# JUnit identity for those cases makes the contract fail closed on a healthy shard.
_JUNIT_PROBE_MODULE = """import pytest


@pytest.mark.parametrize(
    "value",
    (1, 2, 3, 4, 5, 6),
    ids=(
        "--junitxml=other.py::test_case",
        "a::b::c",
        "x[y]::z",
        "plain",
        "[bracket::inside]",
        "::leading",
    ),
)
def test_top(value):
    assert value


class TestKlass:
    @pytest.mark.parametrize(
        "value",
        (1, 2),
        ids=("--opt=a.py::b", "nested::id[with]brackets"),
    )
    def test_inner(self, value):
        assert value

    def test_plain_inner(self):
        assert True


def test_no_param():
    assert True
"""

_JUNIT_PROBE_CASES = 10


def _real_pytest_junit_identities(tmp_path: Path) -> tuple[tuple[str, str, str], ...]:
    """Return the (nodeid, classname, name) triples a real pytest run reports."""
    root = tmp_path / "junit-probe"
    (root / "tests").mkdir(parents=True)
    (root / "tests" / "test_probe.py").write_text(_JUNIT_PROBE_MODULE, encoding="utf-8")
    environment = dict(os.environ)
    environment["PYTEST_ADDOPTS"] = ""
    command = [
        sys.executable,
        "-m",
        "pytest",
        "tests/test_probe.py",
        "-q",
        "-p",
        "no:cacheprovider",
        "-o",
        "addopts=",
        f"--rootdir={root}",
    ]
    collected = subprocess.run(
        [*command, "--collect-only"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
        env=environment,
    )
    nodeids = tuple(
        line.strip()
        for line in collected.stdout.splitlines()
        if line.startswith("tests/test_probe.py::")
    )
    subprocess.run(
        [*command, f"--junitxml={root / 'junit.xml'}", f"--basetemp={root / 'pt'}"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
        env=environment,
    )
    reported = tuple(
        (case.attrib["classname"], case.attrib["name"])
        for case in ElementTree.parse(root / "junit.xml").getroot().iter("testcase")
    )
    assert len(nodeids) == _JUNIT_PROBE_CASES
    assert len(reported) == _JUNIT_PROBE_CASES
    return tuple(
        (nodeid, classname, name)
        for nodeid, (classname, name) in zip(nodeids, reported, strict=True)
    )


def test_junit_identity_matches_what_pytest_actually_reports(tmp_path: Path) -> None:
    triples = _real_pytest_junit_identities(tmp_path)

    separators = [nodeid for nodeid, _, _ in triples if "::" in nodeid.partition("[")[2]]
    assert len(separators) == 7
    assert any("::" not in nodeid.partition("[")[2] for nodeid, _, _ in triples)
    for nodeid, classname, name in triples:
        assert shards.junit_identity(nodeid) == {
            "classname": classname,
            "name": name,
        }, nodeid


@pytest.mark.parametrize(
    ("nodeid", "classname", "name"),
    (
        (
            "tests/x/test_y.py::TestK::test_z[--opt=a.py::b]",
            "tests.x.test_y.TestK",
            "test_z[--opt=a.py::b]",
        ),
        (
            "tests/x/test_y.py::test_z[a::b::c]",
            "tests.x.test_y",
            "test_z[a::b::c]",
        ),
        (
            "tests/x/test_y.py::test_z[[bracket::inside]]",
            "tests.x.test_y",
            "test_z[[bracket::inside]]",
        ),
        (
            "tests/x/test_y.py::TestK::test_z[nested::id[with]brackets]",
            "tests.x.test_y.TestK",
            "test_z[nested::id[with]brackets]",
        ),
        (
            "tests/x/test_y.py::test_z[::leading]",
            "tests.x.test_y",
            "test_z[::leading]",
        ),
        (
            "tests/x/test_y.py::TestK::TestInner::test_z",
            "tests.x.test_y.TestK.TestInner",
            "test_z",
        ),
        ("tests/x/test_y.py::test_z", "tests.x.test_y", "test_z"),
        ("tests/x/test_y.py::test_z[plain]", "tests.x.test_y", "test_z[plain]"),
    ),
    ids=(
        "option-and-separator-in-class-parameter",
        "repeated-separator-in-parameter",
        "leading-bracket-parameter",
        "mixed-brackets-and-separator",
        "parameter-starting-with-separator",
        "nested-classes-without-parameters",
        "bare-function",
        "ordinary-parameter",
    ),
)
def test_junit_identity_keeps_parametrized_ids_intact(
    nodeid: str,
    classname: str,
    name: str,
) -> None:
    assert shards.junit_identity(nodeid) == {"classname": classname, "name": name}


@pytest.mark.parametrize(
    "nodeid",
    (
        "tests/a.py",
        "tests/a.py::",
        "tests/a.py::TestK::",
        "tests/a.py::::test_case",
        "tests/a.txt::test_case",
    ),
    ids=(
        "no-separator",
        "empty-selection",
        "empty-name-after-class",
        "empty-class",
        "not-a-python-module",
    ),
)
def test_junit_identity_rejects_nodeids_it_cannot_map(nodeid: str) -> None:
    with pytest.raises(shards.ContractError, match="JUnit testcase"):
        shards.junit_identity(nodeid)


def test_nodeid_file_resolution_ignores_separators_inside_parameters(
    tmp_path: Path,
) -> None:
    """The path stops at the first separator, so parameter text cannot move it."""
    repository_root = _test_repository(tmp_path, "tests/test_params.py")

    for nodeid in (
        "tests/test_params.py::test_case[a::b::c]",
        "tests/test_params.py::TestK::test_case[--opt=elsewhere.py::other]",
        "tests/test_params.py::test_case[[bracket::inside]]",
    ):
        assert shards._file_for_nodeid(nodeid, repository_root=repository_root) == (
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
    # Each record repeats the nodeid in its JUnit identity, so size the nodeids
    # to sit just under the per-shard cap: four of them then exceed the total.
    nodeid_size = shards.MAX_SHARD_MANIFEST_BYTES // 2 - 128
    payload_size = nodeid_size - len(prefix) - len(suffix)
    nodeids = tuple(prefix + (str(index) + "x" * (payload_size - 1)) + suffix for index in range(4))
    _write_approved_skips(tmp_path / "oversized-manifest")
    with pytest.raises(shards.ContractError, match="total size"):
        shards.write_manifest_bundle(
            tmp_path / "oversized-manifest",
            selector=(),
            shard_nodeids=tuple((nodeid,) for nodeid in nodeids),
            repository_root=repository_root,
        )


def test_validate_manifest_collects_only_the_default_universe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository_root = _test_repository(tmp_path, "tests/test_default.py")
    manifest_root = tmp_path / "manifest"
    nodeids = ("tests/test_default.py::test_case",)
    _write_approved_skips(manifest_root)
    shards.write_manifest_bundle(
        manifest_root,
        selector=(),
        shard_nodeids=(nodeids, (), (), ()),
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
