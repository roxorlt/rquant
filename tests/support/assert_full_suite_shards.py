"""Fail-closed JUnit aggregation for the four full-suite CI shards."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

from scripts import full_suite_shards as shards

SCHEMA_VERSION = 1
ContractError = shards.ContractError


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ContractError(f"cannot read selection evidence {path}") from exc
    if not isinstance(value, dict):
        raise ContractError(f"selection evidence is not an object: {path}")
    return value


def _nonnegative(value: str | None, *, field: str) -> int:
    try:
        parsed = int(value) if value is not None else -1
    except ValueError as exc:
        raise ContractError(f"JUnit has invalid {field}") from exc
    if parsed < 0:
        raise ContractError(f"JUnit has invalid {field}")
    return parsed


def _parse_junit(path: Path) -> dict[str, int]:
    try:
        root = ElementTree.parse(path).getroot()
    except (OSError, ElementTree.ParseError) as exc:
        raise ContractError(f"malformed JUnit report {path}") from exc
    if root.tag == "testsuite":
        suite = root
    elif root.tag == "testsuites" and len(root) == 1 and root[0].tag == "testsuite":
        suite = root[0]
    else:
        raise ContractError("JUnit must contain exactly one testsuite")
    cases = suite.findall("testcase")
    failed_cases = 0
    errored_cases = 0
    skipped_cases = 0
    for case in cases:
        if not case.attrib.get("classname") or not case.attrib.get("name"):
            raise ContractError("JUnit testcase lacks classname or name")
        failures = case.findall("failure")
        errors = case.findall("error")
        skipped = case.findall("skipped")
        if len(failures) > 1 or len(errors) > 1 or len(skipped) > 1:
            raise ContractError("JUnit testcase has duplicate outcome elements")
        if sum(bool(item) for item in (failures, errors, skipped)) > 1:
            raise ContractError("JUnit testcase has conflicting outcomes")
        failed_cases += bool(failures)
        errored_cases += bool(errors)
        skipped_cases += bool(skipped)
    actual = {
        "cases": len(cases),
        "failures": failed_cases,
        "errors": errored_cases,
        "skipped": skipped_cases,
    }
    expected = {
        "cases": _nonnegative(suite.attrib.get("tests"), field="tests"),
        "failures": _nonnegative(suite.attrib.get("failures"), field="failures"),
        "errors": _nonnegative(suite.attrib.get("errors"), field="errors"),
        "skipped": _nonnegative(suite.attrib.get("skipped"), field="skipped"),
    }
    if actual != expected:
        raise ContractError("JUnit summary differs from actual testcase outcomes")
    return actual


def validate_artifacts(
    manifest_root: Path,
    artifact_root: Path,
    *,
    expected_python: str,
) -> dict[str, int]:
    index, groups = shards.load_manifest(manifest_root)
    shards.validate_manifest(manifest_root, tuple(nodeid for group in groups for nodeid in group))
    expected_names = {
        f"full-suite-evidence-py{expected_python}-shard{shard_id}"
        for shard_id in range(shards.SHARD_COUNT)
    }
    try:
        actual_names = {path.name for path in artifact_root.iterdir() if path.is_dir()}
    except OSError as exc:
        raise ContractError(f"cannot read artifact directory {artifact_root}") from exc
    if actual_names != expected_names:
        raise ContractError("artifact directories do not match the expected Python/shard matrix")
    full = index["full_suite"]
    totals = {"cases": 0, "skipped": 0, "failures": 0, "errors": 0}
    evidence_fields = {
        "schema_version",
        "python_version",
        "shard",
        "full_count",
        "full_digest",
        "shard_count",
        "shard_digest",
    }
    for shard in index["shards"]:
        shard_id = shard["id"]
        artifact = artifact_root / f"full-suite-evidence-py{expected_python}-shard{shard_id}"
        evidence = _read_json(artifact / "selection.json")
        if set(evidence) != evidence_fields:
            raise ContractError("selection evidence fields are invalid")
        expected_evidence = {
            "schema_version": SCHEMA_VERSION,
            "python_version": expected_python,
            "shard": shard_id,
            "full_count": full["cases"],
            "full_digest": full["sha256"],
            "shard_count": shard["count"],
            "shard_digest": shard["sha256"],
        }
        if evidence != expected_evidence:
            raise ContractError(f"selection evidence differs for shard {shard_id}")
        parsed = _parse_junit(artifact / "junit.xml")
        if parsed["cases"] != shard["count"]:
            raise ContractError(f"JUnit testcase count differs for shard {shard_id}")
        for key, value in parsed.items():
            totals[key] += value
    if totals["cases"] != full["cases"] or totals["skipped"] != full["skips"]:
        raise ContractError("JUnit aggregate cases or skips differs from the full-suite contract")
    if totals["failures"] or totals["errors"]:
        raise ContractError("JUnit aggregate contains a failure or error")
    return totals


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest-dir", type=Path, required=True)
    parser.add_argument("--artifact-dir", type=Path, required=True)
    parser.add_argument("--expected-python", required=True)
    parser.add_argument("--summary-json", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    summary = validate_artifacts(
        args.manifest_dir,
        args.artifact_dir,
        expected_python=args.expected_python,
    )
    args.summary_json.parent.mkdir(parents=True, exist_ok=True)
    args.summary_json.write_text(json.dumps(summary, sort_keys=True) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
