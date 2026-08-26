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

LINUX_DARWIN_CONTRACT_NODEIDS = (
    "tests/unit/test_contained_subprocess.py::test_darwin_registration_rejects_hooks_before_initialized_queue_side_effects",
    "tests/unit/test_contained_subprocess.py::test_darwin_register_root_rechecks_hooks_after_registration_handoffs",
    "tests/unit/test_contained_subprocess.py::test_darwin_register_root_rejects_non_pristine_tracker_without_side_effects",
    "tests/unit/test_contained_subprocess.py::test_darwin_register_root_serializes_concurrent_callers",
    "tests/unit/test_contained_subprocess.py::test_darwin_register_root_discards_tainted_preinitialized_queue_for_retry",
    "tests/unit/test_contained_subprocess.py::test_darwin_register_root_retains_live_failed_start_and_first_error",
    "tests/unit/test_contained_subprocess.py::test_darwin_close_reports_join_error_after_safe_queue_cleanup",
    "tests/unit/test_contained_subprocess.py::test_darwin_close_retains_owner_until_thread_stop_is_verified",
    "tests/unit/test_contained_subprocess.py::test_darwin_failed_preinitialized_queue_close_blocks_registration_until_retry",
    "tests/unit/test_contained_subprocess.py::test_darwin_registration_reentrant_close_fails_and_rolls_back",
    "tests/unit/test_contained_subprocess.py::test_darwin_poll_rejects_hooks_at_every_state_handoff",
    "tests/unit/test_contained_subprocess.py::test_darwin_registration_rechecks_deadline_after_every_handoff",
    "tests/unit/test_contained_subprocess.py::test_darwin_track_direct_call_rejects_hooks_before_state_changes",
    "tests/unit/test_contained_subprocess.py::test_darwin_register_root_reports_startup_thread_hooks",
    "tests/unit/test_contained_subprocess.py::test_darwin_track_stops_before_next_control_when_hook_activates",
    "tests/unit/test_contained_subprocess.py::test_darwin_track_preserves_first_error_across_later_control_failure",
    "tests/unit/test_contained_subprocess.py::test_darwin_track_rechecks_hooks_after_kernel_handoffs",
    "tests/unit/test_lab_launchd.py::test_lab_launchd_plists_pass_plutil_lint",
)


def _repository_for_manifest(root: Path) -> Path:
    return root.parent / "repository"


def _write_bundle(root: Path) -> dict[str, object]:
    repository_root = _repository_for_manifest(root)
    for nodeid in NODEIDS:
        path = repository_root / nodeid.partition("::")[0]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("def test_case():\n    assert True\n", encoding="utf-8")
    return shards.write_manifest_bundle(
        root,
        selector=(),
        shard_nodeids=tuple((nodeid,) for nodeid in NODEIDS),
        expected_skips=1,
        repository_root=repository_root,
    )


def _validate(
    manifest_root: Path,
    artifacts: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, int]:
    repository_root = _repository_for_manifest(manifest_root)

    def collect(_selector: tuple[str, ...], *, repository_root: Path) -> tuple[str, ...]:
        assert repository_root == _repository_for_manifest(manifest_root)
        return NODEIDS

    monkeypatch.setattr(shards, "collect_nodeids", collect)
    return validator.validate_artifacts(
        manifest_root,
        artifacts,
        expected_python="3.12",
        repository_root=repository_root,
    )


def _write_artifacts(
    root: Path,
    index: dict[str, object],
    *,
    python_version: str = "3.12",
    shard_with_skip: int | None = 3,
    outcome: str = "pass",
) -> None:
    full = index["full_suite"]
    for shard in index["shards"]:
        shard_id = shard["id"]
        artifact = root / f"full-suite-evidence-py{python_version}-shard{shard_id}"
        artifact.mkdir(parents=True)
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
            child = "<skipped/>"
            skipped = 1
        if shard_id == 0 and outcome in {"failure", "error"}:
            child = f"<{outcome}/>"
        failures = int(outcome == "failure" and shard_id == 0)
        errors = int(outcome == "error" and shard_id == 0)
        cases = "".join(
            (
                f'<testcase classname="tests.shard{shard_id}" name="case{case_id}">'
                f"{child if case_id == 0 else ''}</testcase>"
            )
            for case_id in range(shard["count"])
        )
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
    assert "57 passed" in output
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

    assert summary == {"cases": 4, "skipped": 1, "failures": 0, "errors": 0}


def test_clean_environment_aggregate_uses_shared_private_collect_setup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository_root = (tmp_path / "clean-repository").resolve()
    _write_clean_environment_repository(repository_root)
    manifest_root = tmp_path / "manifest"
    index = shards.write_manifest_bundle(
        manifest_root,
        selector=(),
        shard_nodeids=tuple((nodeid,) for nodeid in CLEAN_ENV_NODEIDS),
        expected_skips=1,
        repository_root=repository_root,
    )
    artifacts = tmp_path / "artifacts"
    _write_artifacts(artifacts, index)
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
        repository_root=repository_root,
    )

    assert summary == {"cases": 4, "skipped": 1, "failures": 0, "errors": 0}
    workflow = (Path(__file__).parents[2] / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    setup_command = "python scripts/full_suite_shards.py prepare-environment"
    assert workflow.count(setup_command) == 2
    shard_job, contract_job = workflow.split("  full-suite-contract:\n", maxsplit=1)
    assert setup_command in shard_job.split("  full-suite-shard:\n", maxsplit=1)[1]
    assert setup_command in contract_job.split("  runtime-fd-exec-linux:\n", maxsplit=1)[0]
    assert workflow.count('--basetemp "${RQUANT_CI_PYTEST_BASETEMP}"') == 1
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
    _assert_linux_darwin_contract(Path(__file__).parents[2])
    full_suite = json.loads(
        (Path(__file__).parents[1] / "manifests/full-suite-v1/index.json").read_text(
            encoding="utf-8"
        )
    )["full_suite"]
    assert full_suite["cases"] == 13051
    assert full_suite["skips"] == 49


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

    with pytest.raises(validator.ContractError, match="failure|error"):
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
        index, groups = shards.validate_manifest(
            manifest_root,
            repository_root=Path(__file__).parents[2],
            environment=environment,
        )
    nodeids = tuple(nodeid for group in groups for nodeid in group)
    assert len(nodeids) == index["full_suite"]["cases"]
    assert len(nodeids) == len(set(nodeids))
    assert shards.nodeid_digest(nodeids) == index["full_suite"]["sha256"]
