"""Fail-closed, per-nodeid JUnit aggregation for the full-suite CI shards."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

# Run as a script, `sys.path[0]` is this file's directory, so the repository root — and with
# it the `scripts` package — is not importable. The CI aggregation step invokes this file by
# path, so the root goes on the path here rather than being left to the caller.
_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
if str(_REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPOSITORY_ROOT))

from scripts import full_suite_shards as shards  # noqa: E402

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


def _parse_junit(path: Path) -> dict[tuple[str, str], tuple[str, str]]:
    """Return every reported testcase keyed by its (classname, name) identity."""
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
    outcomes: dict[tuple[str, str], tuple[str, str]] = {}
    counts = {"cases": len(cases), "failures": 0, "errors": 0, "skipped": 0}
    for case in cases:
        classname = case.attrib.get("classname")
        name = case.attrib.get("name")
        if not classname or not name:
            raise ContractError("JUnit testcase lacks classname or name")
        failures = case.findall("failure")
        errors = case.findall("error")
        skipped = case.findall("skipped")
        if len(failures) > 1 or len(errors) > 1 or len(skipped) > 1:
            raise ContractError("JUnit testcase has duplicate outcome elements")
        if sum(bool(item) for item in (failures, errors, skipped)) > 1:
            raise ContractError("JUnit testcase has conflicting outcomes")
        if failures:
            outcome = ("failed", failures[0].attrib.get("message", ""))
            counts["failures"] += 1
        elif errors:
            outcome = ("error", errors[0].attrib.get("message", ""))
            counts["errors"] += 1
        elif skipped:
            outcome = ("skipped", skipped[0].attrib.get("message", ""))
            counts["skipped"] += 1
        else:
            outcome = ("passed", "")
        identity = (classname, name)
        if identity in outcomes:
            raise ContractError(f"JUnit reports {classname}::{name} more than once")
        outcomes[identity] = outcome
    expected = {
        "cases": _nonnegative(suite.attrib.get("tests"), field="tests"),
        "failures": _nonnegative(suite.attrib.get("failures"), field="failures"),
        "errors": _nonnegative(suite.attrib.get("errors"), field="errors"),
        "skipped": _nonnegative(suite.attrib.get("skipped"), field="skipped"),
    }
    if counts != expected:
        raise ContractError("JUnit summary differs from actual testcase outcomes")
    return outcomes


def _check_shard_outcomes(
    *,
    shard_id: int,
    nodeids: tuple[str, ...],
    junit_identities: dict[str, dict[str, str]],
    outcomes: dict[tuple[str, str], tuple[str, str]],
    approved: dict[str, str],
) -> tuple[int, int]:
    """Match every manifest nodeid to exactly one reported outcome."""
    expected: dict[tuple[str, str], str] = {}
    for nodeid in nodeids:
        identity = junit_identities[nodeid]
        key = (identity["classname"], identity["name"])
        if key in expected:
            raise ContractError(f"shard {shard_id} maps two nodeids onto {key[0]}::{key[1]}")
        expected[key] = nodeid
    unexpected = sorted(outcomes.keys() - expected.keys())
    if unexpected:
        classname, name = unexpected[0]
        raise ContractError(
            f"shard {shard_id} reported {classname}::{name}, which is not in the manifest"
        )
    passed = 0
    approved_skips = 0
    for key, nodeid in sorted(expected.items()):
        reported = outcomes.get(key)
        if reported is None:
            raise ContractError(f"shard {shard_id} never reported {nodeid}")
        status, message = reported
        if status == "passed":
            passed += 1
            continue
        if status != "skipped":
            raise ContractError(f"shard {shard_id} reported {nodeid} as {status}")
        reason = approved.get(nodeid)
        if reason is None:
            raise ContractError(f"shard {shard_id} skipped {nodeid}, which is not approved")
        if message != reason:
            raise ContractError(
                f"shard {shard_id} skipped {nodeid} with an unapproved reason: {message!r}"
            )
        approved_skips += 1
    return passed, approved_skips


def validate_artifacts(
    manifest_root: Path,
    artifact_root: Path,
    *,
    expected_python: str,
    platform: str,
    repository_root: Path = shards.REPOSITORY_ROOT,
    profile: shards.ManifestProfile = shards.FULL_SUITE_PROFILE,
    artifact_prefix: str = "full-suite-evidence",
) -> dict[str, Any]:
    if platform not in shards.APPROVED_SKIP_PLATFORMS:
        raise ContractError(f"unsupported contract platform {platform}")
    loaded = shards.validate_manifest(
        manifest_root,
        repository_root=repository_root,
        profile=profile,
    )
    index = loaded.index
    expected_names = {
        f"{artifact_prefix}-py{expected_python}-shard{shard_id}"
        for shard_id in range(profile.shard_count)
    }
    try:
        actual_names = {path.name for path in artifact_root.iterdir() if path.is_dir()}
    except OSError as exc:
        raise ContractError(f"cannot read artifact directory {artifact_root}") from exc
    if actual_names != expected_names:
        raise ContractError("artifact directories do not match the expected Python/shard matrix")
    full = index["full_suite"]
    approved = loaded.approved_skips[platform]
    totals = {"cases": 0, "passed": 0, "approved_skips": 0}
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
        artifact = artifact_root / f"{artifact_prefix}-py{expected_python}-shard{shard_id}"
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
        outcomes = _parse_junit(artifact / "junit.xml")
        if len(outcomes) != shard["count"]:
            raise ContractError(f"JUnit testcase count differs for shard {shard_id}")
        passed, approved_skips = _check_shard_outcomes(
            shard_id=shard_id,
            nodeids=loaded.groups[shard_id],
            junit_identities=loaded.junit,
            outcomes=outcomes,
            approved=approved,
        )
        totals["cases"] += len(outcomes)
        totals["passed"] += passed
        totals["approved_skips"] += approved_skips
    if totals["cases"] != full["cases"]:
        raise ContractError("JUnit aggregate case count differs from the full-suite contract")
    return {
        "cases": totals["cases"],
        "passed": totals["passed"],
        "approved_skips": totals["approved_skips"],
        "platform": platform,
        "python_version": expected_python,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest-dir", type=Path, required=True)
    parser.add_argument("--artifact-dir", type=Path, required=True)
    parser.add_argument("--expected-python", required=True)
    parser.add_argument("--platform", required=True, choices=shards.APPROVED_SKIP_PLATFORMS)
    parser.add_argument(
        "--profile",
        choices=sorted(shards.MANIFEST_PROFILES),
        default=shards.FULL_SUITE_PROFILE.name,
    )
    parser.add_argument("--artifact-prefix", default="full-suite-evidence")
    parser.add_argument("--summary-json", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    summary = validate_artifacts(
        args.manifest_dir,
        args.artifact_dir,
        expected_python=args.expected_python,
        platform=args.platform,
        profile=shards.MANIFEST_PROFILES[args.profile],
        artifact_prefix=args.artifact_prefix,
    )
    args.summary_json.parent.mkdir(parents=True, exist_ok=True)
    args.summary_json.write_text(json.dumps(summary, sort_keys=True) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
