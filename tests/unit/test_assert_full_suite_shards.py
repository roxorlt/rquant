"""Fail-closed aggregation contracts for full-suite shard CI evidence."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import full_suite_shards as shards
from tests.support import assert_full_suite_shards as validator


def _write_bundle(root: Path) -> dict[str, object]:
    return shards.write_manifest_bundle(
        root,
        selector=(),
        shard_nodeids=(
            ("tests/a.py::test_a",),
            ("tests/b.py::test_b",),
            ("tests/c.py::test_c",),
            ("tests/d.py::test_d",),
        ),
        expected_skips=1,
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


def test_validator_aggregates_real_testcases_and_skips(tmp_path: Path) -> None:
    manifest_root = tmp_path / "manifest"
    index = _write_bundle(manifest_root)
    artifacts = tmp_path / "artifacts"
    _write_artifacts(artifacts, index)

    summary = validator.validate_artifacts(manifest_root, artifacts, expected_python="3.12")

    assert summary == {"cases": 4, "skipped": 1, "failures": 0, "errors": 0}


@pytest.mark.parametrize("outcome", ("failure", "error"))
def test_validator_rejects_a_failed_or_errored_testcase(tmp_path: Path, outcome: str) -> None:
    manifest_root = tmp_path / "manifest"
    index = _write_bundle(manifest_root)
    artifacts = tmp_path / "artifacts"
    _write_artifacts(artifacts, index, outcome=outcome)

    with pytest.raises(validator.ContractError, match="failure|error"):
        validator.validate_artifacts(manifest_root, artifacts, expected_python="3.12")


def test_validator_rejects_missing_or_mixed_shard_artifacts(tmp_path: Path) -> None:
    manifest_root = tmp_path / "manifest"
    index = _write_bundle(manifest_root)
    artifacts = tmp_path / "artifacts"
    _write_artifacts(artifacts, index)
    (artifacts / "full-suite-evidence-py3.12-shard1").rename(artifacts / "wrong-shard")

    with pytest.raises(validator.ContractError, match="artifact directories"):
        validator.validate_artifacts(manifest_root, artifacts, expected_python="3.12")

    (artifacts / "wrong-shard").rename(artifacts / "full-suite-evidence-py3.12-shard1")
    (artifacts / "full-suite-evidence-py3.12-shard1" / "junit.xml").unlink()
    with pytest.raises(validator.ContractError, match="malformed JUnit"):
        validator.validate_artifacts(manifest_root, artifacts, expected_python="3.12")


def test_validator_rejects_malformed_junit_and_python_version_mismatch(tmp_path: Path) -> None:
    manifest_root = tmp_path / "manifest"
    index = _write_bundle(manifest_root)
    artifacts = tmp_path / "artifacts"
    _write_artifacts(artifacts, index)
    target = artifacts / "full-suite-evidence-py3.12-shard0"
    (target / "junit.xml").write_text("<not-junit>", encoding="utf-8")

    with pytest.raises(validator.ContractError, match="malformed|JUnit"):
        validator.validate_artifacts(manifest_root, artifacts, expected_python="3.12")

    _write_artifacts(artifacts, index, python_version="3.11")
    with pytest.raises(validator.ContractError, match="artifact directories|python"):
        validator.validate_artifacts(manifest_root, artifacts, expected_python="3.12")
