"""Fail-closed aggregation contracts for full-suite shard CI evidence."""

from __future__ import annotations

import json
import os
import re
import stat
import subprocess
import sys
from pathlib import Path

import pytest

from scripts import full_suite_shards as shards
from tests.support import assert_full_suite_shards as validator

NODEIDS = (
    "tests/a.py::test_a",
    "tests/b.py::test_b",
    "tests/c.py::test_c",
    "tests/d.py::test_d",
)

APPROVED_SKIP_NODEID = NODEIDS[3]
APPROVED_SKIP_REASON = "Darwin-only capability gate"

CLEAN_ENV_NODEIDS = (
    "tests/test_a.py::test_a",
    "tests/test_b.py::test_b",
    "tests/test_c.py::test_c",
    "tests/test_d.py::test_d",
)

SENTINEL_ENVIRONMENT = {
    "TUSHARE_TOKEN_BACKUP": "tushare-backup-sentinel",
    "PUSHDEER_KEYS": "pushdeer-sentinel",
    "AWS_ACCESS_KEY_ID": "aws-access-key-sentinel",
    "RQUANT_PANORAMA_GATE_TOKEN": "panorama-gate-sentinel",
    "GITHUB_ACTIONS": "github-actions-sentinel",
    "RUNNER_TEMP": "runner-temp-sentinel",
}

# Darwin tests that drive their subject through injected fakes and therefore
# behave identically whatever sys.platform claims. The kqueue registration
# cases are deliberately absent: they need the real select.kqueue, so they
# carry a Darwin skipif and run in the macOS lane instead.
LINUX_DARWIN_CONTRACT_NODEIDS = (
    "tests/unit/test_contained_subprocess.py::test_darwin_registration_rejects_hooks_before_initialized_queue_side_effects",
    "tests/unit/test_contained_subprocess.py::test_darwin_register_root_rechecks_hooks_after_registration_handoffs",
    "tests/unit/test_contained_subprocess.py::test_darwin_register_root_rejects_non_pristine_tracker_without_side_effects",
    "tests/unit/test_contained_subprocess.py::test_darwin_close_reports_join_error_after_safe_queue_cleanup",
    "tests/unit/test_contained_subprocess.py::test_darwin_close_retains_owner_until_thread_stop_is_verified",
    "tests/unit/test_contained_subprocess.py::test_darwin_poll_rejects_hooks_at_every_state_handoff",
    "tests/unit/test_contained_subprocess.py::test_darwin_track_direct_call_rejects_hooks_before_state_changes",
    "tests/unit/test_contained_subprocess.py::test_darwin_register_root_reports_startup_thread_hooks",
    "tests/unit/test_contained_subprocess.py::test_darwin_track_stops_before_next_control_when_hook_activates",
    "tests/unit/test_contained_subprocess.py::test_darwin_track_preserves_first_error_across_later_control_failure",
    "tests/unit/test_contained_subprocess.py::test_darwin_track_rechecks_hooks_after_kernel_handoffs",
    "tests/unit/test_lab_launchd.py::test_lab_launchd_plists_pass_plutil_lint",
)


def _repository_for_manifest(root: Path) -> Path:
    return root.parent / "repository"


def _write_approved_skips(
    root: Path,
    *,
    linux: dict[str, str] | None = None,
    darwin: dict[str, str] | None = None,
) -> None:
    root.mkdir(parents=True, exist_ok=True)
    document = {
        "platforms": {
            "darwin": dict(darwin or {}),
            "linux": {APPROVED_SKIP_NODEID: APPROVED_SKIP_REASON} if linux is None else linux,
        },
        "schema_version": 1,
    }
    shards.approved_skips_path(root).write_text(
        json.dumps(document, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )


def _write_bundle(root: Path, *, nodeids: tuple[str, ...] = NODEIDS) -> dict[str, object]:
    repository_root = _repository_for_manifest(root)
    for nodeid in nodeids:
        path = repository_root / nodeid.partition("::")[0]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("def test_case():\n    assert True\n", encoding="utf-8")
    _write_approved_skips(root)
    return shards.write_manifest_bundle(
        root,
        selector=(),
        shard_nodeids=tuple((nodeid,) for nodeid in nodeids),
        repository_root=repository_root,
    )


def _validate(
    manifest_root: Path,
    artifacts: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    platform: str = "linux",
    nodeids: tuple[str, ...] = NODEIDS,
) -> dict[str, object]:
    repository_root = _repository_for_manifest(manifest_root)

    def collect(_selector: tuple[str, ...], *, repository_root: Path) -> tuple[str, ...]:
        assert repository_root == _repository_for_manifest(manifest_root)
        return nodeids

    monkeypatch.setattr(shards, "collect_nodeids", collect)
    return validator.validate_artifacts(
        manifest_root,
        artifacts,
        expected_python="3.12",
        platform=platform,
        repository_root=repository_root,
    )


def _write_artifacts(
    root: Path,
    index: dict[str, object],
    *,
    python_version: str = "3.12",
    shard_with_skip: int | None = 3,
    outcome: str = "pass",
    skip_reason: str = APPROVED_SKIP_REASON,
    nodeids: tuple[str, ...] = NODEIDS,
    rename_case: tuple[int, str] | None = None,
) -> None:
    """Write one JUnit report per shard, keyed by the manifest's own identities."""
    full = index["full_suite"]
    for shard in index["shards"]:
        shard_id = shard["id"]
        artifact = root / f"full-suite-evidence-py{python_version}-shard{shard_id}"
        artifact.mkdir(parents=True, exist_ok=True)
        evidence = {
            "schema_version": 1,
            "python_version": python_version,
            "shard": shard_id,
            "full_count": full["cases"],
            "full_digest": full["sha256"],
            "shard_count": shard["count"],
            "shard_digest": shard["sha256"],
        }
        (artifact / "selection.json").write_text(json.dumps(evidence), encoding="utf-8")
        child = ""
        skipped = 0
        if shard_id == shard_with_skip:
            child = f'<skipped type="pytest.skip" message="{skip_reason}"/>'
            skipped = 1
        if shard_id == 0 and outcome in {"failure", "error"}:
            child = f"<{outcome}/>"
        failures = int(outcome == "failure" and shard_id == 0)
        errors = int(outcome == "error" and shard_id == 0)
        identity = shards.junit_identity(nodeids[shard_id])
        name = identity["name"]
        if rename_case is not None and rename_case[0] == shard_id:
            name = rename_case[1]
        cases = f'<testcase classname="{identity["classname"]}" name="{name}">{child}</testcase>'
        xml = (
            f'<testsuite tests="{shard["count"]}" failures="{failures}" '
            f'errors="{errors}" skipped="{skipped}">' + cases + "</testsuite>"
        )
        (artifact / "junit.xml").write_text(xml, encoding="utf-8")


def _write_clean_environment_repository(root: Path) -> None:
    tests_root = root / "tests"
    tests_root.mkdir(parents=True)
    for nodeid in CLEAN_ENV_NODEIDS:
        relative, _, test_name = nodeid.partition("::")
        (root / relative).write_text(
            f"def {test_name}():\n    assert True\n",
            encoding="utf-8",
        )
    (tests_root / "conftest.py").write_text(
        """from pathlib import Path
import os

_FORBIDDEN = (
    "TUSHARE_TOKEN_BACKUP",
    "PUSHDEER_KEYS",
    "AWS_ACCESS_KEY_ID",
    "RQUANT_PANORAMA_GATE_TOKEN",
    "GITHUB_ACTIONS",
    "RUNNER_TEMP",
)
_present = [name for name in _FORBIDDEN if name in os.environ]
assert not _present, "forbidden environment variable names: " + ", ".join(_present)
assert os.environ["PYTHONNOUSERSITE"] == "1"
assert os.environ["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] == "1"
assert os.environ["PYTEST_ADDOPTS"] == ""

root = Path(os.environ["RQUANT_CI_ROOT"])
assert root.is_absolute() and root.resolve(strict=True) == root
assert os.environ["RQUANT_DISABLE_DOTENV"] == "1"
assert os.environ["TUSHARE_TOKEN_MAIN"] == "0" * 32
assert os.environ["NOTIFY_ENABLED"] == "false"
for name, relative in {
    "TMPDIR": "tmp",
    "TMP": "tmp",
    "TEMP": "tmp",
    "HOME": "home",
    "DATA_DIR": "data",
    "DUCKDB_PATH": "data/test.duckdb",
    "DUCKDB_READONLY_PATH": "data/test_ro.duckdb",
    "PARQUET_DIR": "parquet",
    "LOG_DIR": "logs",
}.items():
    assert Path(os.environ[name]) == root / relative

from rquant.config import settings

assert settings.data_dir == root / "data"
assert settings.duckdb_path == root / "data/test.duckdb"
assert settings.parquet_dir == root / "parquet"
assert settings.log_dir == root / "logs"
""",
        encoding="utf-8",
    )


def _assert_linux_darwin_contract(repository_root: Path) -> None:
    command = (
        "import sys; "
        "sys.platform = 'linux'; "
        "import pytest; "
        "raise SystemExit(pytest.main(sys.argv[1:]))"
    )
    with shards._isolated_pytest_environment() as environment:
        completed = subprocess.run(
            [
                sys.executable,
                "-c",
                command,
                "--noconftest",
                "-q",
                *LINUX_DARWIN_CONTRACT_NODEIDS,
            ],
            cwd=repository_root,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
    output = completed.stdout + completed.stderr
    assert completed.returncode == 0, output
    assert "43 passed" in output
    assert "skipped" not in output


def test_validator_aggregates_real_testcases_and_skips(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest_root = tmp_path / "manifest"
    index = _write_bundle(manifest_root)
    artifacts = tmp_path / "artifacts"
    _write_artifacts(artifacts, index)

    summary = _validate(manifest_root, artifacts, monkeypatch)

    assert summary == {
        "cases": 4,
        "passed": 3,
        "approved_skips": 1,
        "platform": "linux",
        "python_version": "3.12",
    }


def test_clean_environment_aggregate_uses_shared_private_collect_setup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository_root = (tmp_path / "clean-repository").resolve()
    _write_clean_environment_repository(repository_root)
    manifest_root = tmp_path / "manifest"
    _write_approved_skips(
        manifest_root,
        linux={CLEAN_ENV_NODEIDS[3]: APPROVED_SKIP_REASON},
    )
    index = shards.write_manifest_bundle(
        manifest_root,
        selector=(),
        shard_nodeids=tuple((nodeid,) for nodeid in CLEAN_ENV_NODEIDS),
        repository_root=repository_root,
    )
    artifacts = tmp_path / "artifacts"
    _write_artifacts(artifacts, index, nodeids=CLEAN_ENV_NODEIDS)
    project_environment = {
        "DATA_DIR",
        "DEEPSEEK_API_KEY",
        "DUCKDB_PATH",
        "DUCKDB_READONLY_PATH",
        "LOG_DIR",
        "NOTIFY_ENABLED",
        "PANORAMA_COOKIE_SECRET",
        "PANORAMA_GATE_TOKEN",
        "PARQUET_DIR",
        "PUSHDEER_KEYS",
        "PUSHPLUS_TOKENS",
        "TEMP",
        "TMP",
        "TMPDIR",
        "TUSHARE_TOKEN_BACKUP",
        "TUSHARE_TOKEN_MAIN",
    }
    project_environment.update(name for name in os.environ if name.startswith("RQUANT_"))
    for name in project_environment:
        monkeypatch.delenv(name, raising=False)
    for name, value in SENTINEL_ENVIRONMENT.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("PYTEST_ADDOPTS", "--tb=short")

    summary = validator.validate_artifacts(
        manifest_root,
        artifacts,
        expected_python="3.12",
        platform="linux",
        repository_root=repository_root,
    )

    assert summary == {
        "cases": 4,
        "passed": 3,
        "approved_skips": 1,
        "platform": "linux",
        "python_version": "3.12",
    }
    workflow = (Path(__file__).parents[2] / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    setup_command = "python scripts/full_suite_shards.py prepare-environment"
    # Three lanes prepare a private root: the Linux shards, the Linux contract
    # aggregation, and the macOS Darwin lane.
    assert workflow.count(setup_command) == 3
    shard_job, contract_job = workflow.split("  full-suite-contract:\n", maxsplit=1)
    assert setup_command in shard_job.split("  full-suite-shard:\n", maxsplit=1)[1]
    assert setup_command in contract_job.split("  full-suite-darwin-lane:\n", maxsplit=1)[0]
    lane_job = contract_job.split("  full-suite-darwin-lane:\n", maxsplit=1)[1]
    lane_job = lane_job.split("  r07-differential-gate-py311:\n", maxsplit=1)[0]
    assert setup_command in lane_job
    assert "runs-on: macos-14" in lane_job
    assert "timeout-minutes: 30" in lane_job
    assert "--profile darwin-lane" in lane_job
    assert "--platform darwin" in lane_job
    assert "--platform linux" in contract_job
    assert workflow.count('--basetemp "${RQUANT_CI_PYTEST_BASETEMP}"') == 2
    assert "${RQUANT_CI_ROOT}/pt-full-shard-" not in workflow

    workflow_base = Path("/private/tmp") if Path("/private/tmp").is_dir() else Path("/tmp")
    github_environment = tmp_path / "github-env"
    assert (
        shards.main(
            [
                "prepare-environment",
                "--github-env",
                str(github_environment),
                "--base-dir",
                str(workflow_base),
                "--label",
                "py3.12-contract",
            ]
        )
        == 0
    )
    prepared = dict(
        line.split("=", maxsplit=1)
        for line in github_environment.read_text(encoding="utf-8").splitlines()
    )
    prepared_root = Path(prepared["RQUANT_CI_ROOT"])
    prepared_basetemp = Path(prepared["RQUANT_CI_PYTEST_BASETEMP"])
    assert prepared_root.parent == workflow_base
    assert prepared_root.resolve(strict=True) == prepared_root
    assert re.fullmatch(r"rqfs\.[A-Za-z0-9_-]{8}", prepared_root.name)
    assert stat.S_IMODE(prepared_root.stat().st_mode) == 0o700
    assert prepared_basetemp == prepared_root / "pt"
    assert prepared_basetemp.resolve(strict=True) == prepared_basetemp
    assert stat.S_IMODE(prepared_basetemp.stat().st_mode) == 0o700
    github_runner_basetemp = Path("/home/runner/work/_temp") / prepared_root.name / "pt"
    assert len(os.fsencode(github_runner_basetemp)) <= shards.MAX_CI_PYTEST_BASETEMP_BYTES
    assert prepared["RQUANT_DISABLE_DOTENV"] == "1"
    assert prepared["TUSHARE_TOKEN_MAIN"] == "0" * 32
    assert prepared["NOTIFY_ENABLED"] == "false"

    second_environment = tmp_path / "github-env-second"
    assert (
        shards.main(
            [
                "prepare-environment",
                "--github-env",
                str(second_environment),
                "--base-dir",
                str(workflow_base),
                "--label",
                "py3.12-contract",
            ]
        )
        == 0
    )
    second = dict(
        line.split("=", maxsplit=1)
        for line in second_environment.read_text(encoding="utf-8").splitlines()
    )
    assert second["RQUANT_CI_ROOT"] != prepared["RQUANT_CI_ROOT"]
    if sys.platform == "darwin":
        # The subject tests fake sys.platform, but they still call the real
        # kqueue and plutil, so only a Darwin host can run this contract.
        _assert_linux_darwin_contract(Path(__file__).parents[2])
    full_suite = json.loads(
        (Path(__file__).parents[1] / "manifests/full-suite-v1/index.json").read_text(
            encoding="utf-8"
        )
    )["full_suite"]
    assert full_suite["cases"] == 13653
    # The skip count is the Linux approved-skip map's size, and the manifest
    # loader already refuses any other value; this pins the number a reviewer
    # sees in the index.
    assert full_suite["skips"] == 55


def test_validator_rejects_an_unapproved_skip(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest_root = tmp_path / "manifest"
    index = _write_bundle(manifest_root)
    artifacts = tmp_path / "artifacts"
    _write_artifacts(artifacts, index, shard_with_skip=1)

    with pytest.raises(validator.ContractError, match="not approved"):
        _validate(manifest_root, artifacts, monkeypatch)


def test_validator_rejects_an_approved_skip_that_actually_passed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The two directions of map rot are symmetric: an unregistered skip is
    rejected above, and a registered nodeid that ran green is rejected here."""

    manifest_root = tmp_path / "manifest"
    index = _write_bundle(manifest_root)
    artifacts = tmp_path / "artifacts"
    _write_artifacts(artifacts, index, shard_with_skip=None)

    with pytest.raises(validator.ContractError, match="registered as an approved skip"):
        _validate(manifest_root, artifacts, monkeypatch)


def test_validator_rejects_an_approved_skip_with_a_different_reason(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest_root = tmp_path / "manifest"
    index = _write_bundle(manifest_root)
    artifacts = tmp_path / "artifacts"
    _write_artifacts(artifacts, index, skip_reason="some other reason")

    with pytest.raises(validator.ContractError, match="unapproved reason"):
        _validate(manifest_root, artifacts, monkeypatch)


def test_validator_rejects_a_missing_or_unexpected_nodeid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest_root = tmp_path / "manifest"
    index = _write_bundle(manifest_root)
    artifacts = tmp_path / "artifacts"
    _write_artifacts(artifacts, index, rename_case=(2, "test_renamed"))

    with pytest.raises(validator.ContractError, match="not in the manifest"):
        _validate(manifest_root, artifacts, monkeypatch)


def test_validator_rejects_a_platform_the_skip_map_does_not_cover(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest_root = tmp_path / "manifest"
    index = _write_bundle(manifest_root)
    artifacts = tmp_path / "artifacts"
    _write_artifacts(artifacts, index)

    with pytest.raises(validator.ContractError, match="unsupported contract platform"):
        _validate(manifest_root, artifacts, monkeypatch, platform="windows")

    # The same evidence is not approved on the other supported platform either:
    # the Darwin side of the map does not list the skipped nodeid.
    with pytest.raises(validator.ContractError, match="not approved"):
        _validate(manifest_root, artifacts, monkeypatch, platform="darwin")


def test_manifest_rejects_a_skip_map_that_drifted_from_the_index(
    tmp_path: Path,
) -> None:
    manifest_root = tmp_path / "manifest"
    _write_bundle(manifest_root)
    repository_root = _repository_for_manifest(manifest_root)

    _write_approved_skips(manifest_root, linux={NODEIDS[0]: "another reason"})
    with pytest.raises(shards.ContractError, match="digest differs"):
        shards.load_manifest(manifest_root, repository_root=repository_root)

    _write_approved_skips(manifest_root, linux={"tests/z.py::test_unknown": "unknown"})
    with pytest.raises(shards.ContractError, match="digest differs|unknown linux nodeids"):
        shards.load_manifest(manifest_root, repository_root=repository_root)


@pytest.mark.parametrize("outcome", ("failure", "error"))
def test_validator_rejects_a_failed_or_errored_testcase(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    outcome: str,
) -> None:
    manifest_root = tmp_path / "manifest"
    index = _write_bundle(manifest_root)
    artifacts = tmp_path / "artifacts"
    _write_artifacts(artifacts, index, outcome=outcome)

    with pytest.raises(validator.ContractError, match="reported .* as (failed|error)"):
        _validate(manifest_root, artifacts, monkeypatch)


def test_validator_rejects_missing_or_mixed_shard_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest_root = tmp_path / "manifest"
    index = _write_bundle(manifest_root)
    artifacts = tmp_path / "artifacts"
    _write_artifacts(artifacts, index)
    (artifacts / "full-suite-evidence-py3.12-shard1").rename(artifacts / "wrong-shard")

    with pytest.raises(validator.ContractError, match="artifact directories"):
        _validate(manifest_root, artifacts, monkeypatch)

    (artifacts / "wrong-shard").rename(artifacts / "full-suite-evidence-py3.12-shard1")
    (artifacts / "full-suite-evidence-py3.12-shard1" / "junit.xml").unlink()
    with pytest.raises(validator.ContractError, match="malformed JUnit"):
        _validate(manifest_root, artifacts, monkeypatch)


def test_validator_rejects_malformed_junit_and_python_version_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest_root = tmp_path / "manifest"
    index = _write_bundle(manifest_root)
    artifacts = tmp_path / "artifacts"
    _write_artifacts(artifacts, index)
    target = artifacts / "full-suite-evidence-py3.12-shard0"
    (target / "junit.xml").write_text("<not-junit>", encoding="utf-8")

    with pytest.raises(validator.ContractError, match="malformed|JUnit"):
        _validate(manifest_root, artifacts, monkeypatch)

    _write_artifacts(artifacts, index, python_version="3.11")
    with pytest.raises(validator.ContractError, match="artifact directories|python"):
        _validate(manifest_root, artifacts, monkeypatch)


def test_checked_in_manifest_matches_exact_collection_without_missing_or_duplicate_cases() -> None:
    manifest_root = Path(__file__).parents[1] / "manifests/full-suite-v1"
    with shards._isolated_pytest_environment() as environment:
        loaded = shards.validate_manifest(
            manifest_root,
            repository_root=Path(__file__).parents[2],
            environment=environment,
        )
    nodeids = tuple(nodeid for group in loaded.groups for nodeid in group)
    assert len(nodeids) == loaded.index["full_suite"]["cases"]
    assert len(nodeids) == len(set(nodeids))
    assert shards.nodeid_digest(nodeids) == loaded.index["full_suite"]["sha256"]
    assert set(loaded.junit) == set(nodeids)
    for nodeid, identity in loaded.junit.items():
        assert identity == shards.junit_identity(nodeid)
    linux_skips = loaded.approved_skips["linux"]
    assert len(linux_skips) == loaded.index["full_suite"]["skips"]
    assert set(linux_skips) <= set(nodeids)
    assert set(loaded.approved_skips["darwin"]) <= set(nodeids)
