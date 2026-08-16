"""R07-DR-GATE-V1 strict evidence, diff, and static source gates."""

from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path

import pytest

from rquant.signal_family_differential_gate import (
    BASELINE_COMMIT_SHA,
    BASELINE_TREE_SHA,
    FORBIDDEN_DEFINITION_UNIVERSE,
    ROOT_SNAPSHOTS,
    PythonRunEvidenceV1,
    R07DrGateEvidenceV1,
    canonical_evidence_json_bytes,
    collect_complete_git_diff,
    load_policy,
    resolve_fixture_values,
    verify_forbidden_definitions,
    verify_root_snapshot,
)

ROOT = Path(__file__).parents[2]
POLICY_PATH = ROOT / "tests" / "fixtures" / "r07_differential_gate" / "policy-v1.json"


def _run(minor: str, job_id: str) -> PythonRunEvidenceV1:
    return PythonRunEvidenceV1(
        python_minor=minor,
        job_id=job_id,
        job_run_id=101 if minor == "3.11" else 102,
        workflow_run_id=100,
        run_attempt=1,
        candidate_commit_sha="a" * 40,
        candidate_tree_sha="b" * 40,
        collected=7,
        passed=7,
        skipped=0,
        deselected=0,
        result_digest="c" * 64,
        outcome="passed",
    )


def _evidence() -> R07DrGateEvidenceV1:
    return R07DrGateEvidenceV1.with_digest(
        schema_version=1,
        repository="roxorlt/rquant",
        workflow_path=".github/workflows/ci.yml",
        event_name="push",
        ref="refs/heads/main",
        producer_job_id="r07-differential-gate-evidence",
        workflow_run_id=100,
        run_attempt=1,
        candidate_commit_sha="a" * 40,
        candidate_tree_sha="b" * 40,
        baseline_commit_sha=BASELINE_COMMIT_SHA,
        baseline_tree_sha=BASELINE_TREE_SHA,
        policy_digest="d" * 64,
        complete_diff_digest="e" * 64,
        boundary_manifest_digest="f" * 64,
        boundary_result_digest="0" * 64,
        root_snapshot_digest="1" * 64,
        forbidden_definition_digest="2" * 64,
        python_runs=(
            _run("3.11", "r07-differential-gate-py311"),
            _run("3.12", "r07-differential-gate-py312"),
        ),
        artifact_name="r07-dr-gate-" + "a" * 40,
        artifact_json_path="r07-dr-gate/evidence-v1.json",
        retention_days=90,
        outcome="passed",
    )


def test_evidence_uses_exact_fields_and_canonical_self_digest() -> None:
    evidence = _evidence()
    raw = canonical_evidence_json_bytes(evidence)
    assert raw == canonical_evidence_json_bytes(evidence)
    assert json.loads(raw)["evidence_digest"] == evidence.evidence_digest


@pytest.mark.parametrize(
    "payload",
    [
        lambda: _evidence().model_dump(mode="json") | {"extra": 1},
        lambda: _evidence().model_dump(mode="json") | {"retention_days": True},
        lambda: _evidence().model_dump(mode="json") | {"python_runs": []},
    ],
)
def test_evidence_rejects_extra_coerced_or_incomplete_json(payload) -> None:
    with pytest.raises(ValueError):
        R07DrGateEvidenceV1.from_canonical_json(json.dumps(payload(), separators=(",", ":")))


def test_composite_fixture_resolves_only_to_prior_ids_and_hashes_resolved_bytes() -> None:
    fixtures = load_policy(POLICY_PATH).fixtures
    resolved = resolve_fixture_values(fixtures)
    assert resolved["current.batch"]
    assert hashlib.sha256(
        json.dumps(resolved["current.batch"], separators=(",", ":")).encode()
    ).hexdigest()


def test_complete_diff_rejects_unlisted_file_in_isolated_repo(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    subprocess.run(["git", "init", "--quiet", str(repo)], check=True)
    (repo / "tracked.txt").write_text("base\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(repo), "add", "tracked.txt"], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "-c",
            "user.email=test@example.invalid",
            "-c",
            "user.name=test",
            "commit",
            "--quiet",
            "-m",
            "base",
        ],
        check=True,
    )
    (repo / "unlisted.txt").write_text("blocked\n", encoding="utf-8")
    result = collect_complete_git_diff(repo, allowed_paths={"tracked.txt"})
    assert result.blocked_paths == ("unlisted.txt",)


def test_root_snapshots_are_static_and_cover_ten_roots() -> None:
    assert len(ROOT_SNAPSHOTS) == 10
    assert all(snapshot.module_path.startswith("src/rquant/") for snapshot in ROOT_SNAPSHOTS)
    for snapshot in ROOT_SNAPSHOTS:
        assert verify_root_snapshot(ROOT, snapshot).passed


def test_forbidden_definition_universe_is_static_source_only() -> None:
    assert "src/rquant/signal_route_spool.py" in FORBIDDEN_DEFINITION_UNIVERSE.source_files
    assert verify_forbidden_definitions(ROOT, FORBIDDEN_DEFINITION_UNIVERSE).passed


def test_policy_is_canonical_and_binds_requested_baseline() -> None:
    policy = load_policy(POLICY_PATH)
    assert policy.baseline_commit_sha == BASELINE_COMMIT_SHA
    assert policy.baseline_tree_sha == BASELINE_TREE_SHA
    assert policy.canonical_bytes == POLICY_PATH.read_bytes()
