"""R07-DR-GATE-V1 strict evidence, diff, and static source gates."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

import rquant.signal_family_differential_gate as differential_gate
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
    verify_boundary_probe_source,
    verify_forbidden_definitions,
    verify_production_declaration,
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
    assert resolved["batch.current-routed"] == ("record.current-routed",)
    assert hashlib.sha256(
        json.dumps(resolved["batch.current-routed"], separators=(",", ":")).encode()
    ).hexdigest()


def _commit(repo: Path, message: str) -> str:
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
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
            message,
        ],
        check=True,
    )
    return subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def test_normative_baseline_pair_and_candidate_repository_identity() -> None:
    assert BASELINE_COMMIT_SHA == "45d0b57c4c5cbab1700fa5e3c386c6756892a7d6"
    assert BASELINE_TREE_SHA == "4f67e67192855874e82baa13dc343a1d6939bd67"
    candidate = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    candidate_tree = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", f"{candidate}^{{tree}}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    policy = load_policy(POLICY_PATH)
    result = differential_gate.verify_candidate_gate(
        ROOT,
        policy=policy,
        candidate_commit=candidate,
        candidate_tree=candidate_tree,
    )
    assert result.passed
    assert result.candidate_tree_sha == candidate_tree
    with pytest.raises(ValueError, match="candidate tree does not match"):
        differential_gate.verify_candidate_gate(
            ROOT,
            policy=policy,
            candidate_commit=candidate,
            candidate_tree="0" * 40,
        )


def test_complete_diff_uses_explicit_commit_range_and_raw_change_identity(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    subprocess.run(["git", "init", "--quiet", str(repo)], check=True)
    for name in ("modified.txt", "deleted.txt", "renamed.txt", "mode.txt", "typed"):
        (repo / name).write_text(f"{name}\n", encoding="utf-8")
    baseline = _commit(repo, "baseline")

    (repo / "modified.txt").write_text("changed\n", encoding="utf-8")
    (repo / "deleted.txt").unlink()
    (repo / "renamed.txt").rename(repo / "renamed-new.txt")
    (repo / "added.txt").write_text("added\n", encoding="utf-8")
    os.chmod(repo / "mode.txt", 0o755)
    (repo / "typed").unlink()
    (repo / "typed").symlink_to("modified.txt")
    candidate = _commit(repo, "candidate")
    (repo / "head-only.txt").write_text("not in candidate\n", encoding="utf-8")
    _commit(repo, "later head")
    (repo / "modified.txt").write_text("dirty worktree only\n", encoding="utf-8")
    (repo / "untracked.txt").write_text("blocked\n", encoding="utf-8")

    result = collect_complete_git_diff(
        repo,
        baseline_commit=baseline,
        candidate_commit=candidate,
        include_untracked=True,
    )
    observed = {
        (entry.status, entry.path, entry.old_mode, entry.new_mode) for entry in result.entries
    }
    assert ("A", "added.txt", "000000", "100644") in observed
    assert ("M", "modified.txt", "100644", "100644") in observed
    assert ("D", "deleted.txt", "100644", "000000") in observed
    assert ("D", "renamed.txt", "100644", "000000") in observed
    assert ("A", "renamed-new.txt", "000000", "100644") in observed
    assert ("M", "mode.txt", "100644", "100755") in observed
    assert ("T", "typed", "100644", "120000") in observed
    assert ("?", "untracked.txt", "000000", "100644") in observed
    assert not any(entry.path == "head-only.txt" for entry in result.entries)


def test_actual_candidate_gate_rejects_any_unlisted_change() -> None:
    candidate = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    candidate_tree = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", f"{candidate}^{{tree}}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    policy = load_policy(POLICY_PATH)
    narrowed = policy.model_copy(update={"allowed_diff": policy.allowed_diff[:-1]})
    result = differential_gate.verify_candidate_gate(
        ROOT,
        policy=narrowed,
        candidate_commit=candidate,
        candidate_tree=candidate_tree,
    )
    assert not result.passed
    assert result.blocked_entries or result.missing_entries


def test_root_snapshots_are_static_and_cover_ten_roots() -> None:
    assert len(ROOT_SNAPSHOTS) == 10
    assert all(snapshot.module_path.startswith("src/rquant/") for snapshot in ROOT_SNAPSHOTS)
    for snapshot in ROOT_SNAPSHOTS:
        assert verify_root_snapshot(ROOT, snapshot).passed


def test_forbidden_definition_universe_is_static_source_only() -> None:
    assert "src/rquant/signal_route_spool.py" in FORBIDDEN_DEFINITION_UNIVERSE.source_files
    assert verify_forbidden_definitions(ROOT, FORBIDDEN_DEFINITION_UNIVERSE).passed
    policy = load_policy(POLICY_PATH)
    assert len(policy.source_file_snapshots) == 9
    verifier = getattr(differential_gate, "verify_top_level_source_closure", None)
    assert verifier is not None
    assert verifier(ROOT, policy.source_file_snapshots).passed


@pytest.mark.parametrize(
    "tamper",
    [
        "registry[''.join(('v3_', 'writer'))] = object()\n",
        "def unrelated_top_level_function() -> None:\n    return None\n",
        "unrelated_top_level_assignment = 1\n",
        "dynamic_module = __import__('rquant.signal_route_spool')\n",
        "__all__ = tuple(name for name in ('build_builtin_registry',))\n",
        "registry['legacy'] = unresolved_registry_alias\n",
    ],
    ids=(
        "computed-v3-writer",
        "function",
        "assign",
        "dynamic-import",
        "dynamic-export",
        "unresolved-alias",
    ),
)
def test_top_level_source_closure_blocks_dynamic_and_unlisted_declarations(
    tmp_path: Path,
    tamper: str,
) -> None:
    policy = load_policy(POLICY_PATH)
    for snapshot in policy.source_file_snapshots:
        source = ROOT / snapshot.module_path
        target = tmp_path / snapshot.module_path
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
    target = tmp_path / "src/rquant/runtime_service_main.py"
    target.write_text(target.read_text(encoding="utf-8") + "\n" + tamper, encoding="utf-8")
    result = differential_gate.verify_top_level_source_closure(
        tmp_path,
        policy.source_file_snapshots,
    )
    assert not result.passed


def test_forbidden_definition_gate_detects_symbols_and_registry_string_keys(
    tmp_path: Path,
) -> None:
    module_path = Path("src/rquant/runtime_service_main.py")
    source_path = tmp_path / module_path
    source_path.parent.mkdir(parents=True)
    source_path.write_text(
        "v3_writer = object()\nregistry = {'v3_overlay': v3_writer}\n",
        encoding="utf-8",
    )
    universe = FORBIDDEN_DEFINITION_UNIVERSE.model_copy(
        update={"source_files": (module_path.as_posix(),)}
    )
    result = verify_forbidden_definitions(tmp_path, universe)
    assert not result.passed
    assert result.reasons == ("src/rquant/runtime_service_main.py: v3_overlay,v3_writer",)


def test_policy_completeness_rejects_any_declaration_or_universe_omission() -> None:
    policy = load_policy(POLICY_PATH)
    assert policy.production_declarations
    assert all(declaration.module_path for declaration in policy.production_declarations)
    assert all(declaration.symbol for declaration in policy.production_declarations)
    assert all(declaration.source_span for declaration in policy.production_declarations)
    assert all(declaration.normalized_ast_sha256 for declaration in policy.production_declarations)
    assert all(declaration.role for declaration in policy.production_declarations)
    assert policy.root_snapshots and all(snapshot.exports for snapshot in policy.root_snapshots)
    universe = policy.forbidden_definition_universe
    assert (
        universe.source_files and universe.symbols and universe.exports and universe.registry_keys
    )
    assert policy.fixtures_digest == differential_gate.fixture_manifest_digest(
        policy.fixtures,
        policy.current_fixtures,
    )
    duplicated_diff = policy.model_copy(
        update={"allowed_diff": (*policy.allowed_diff, policy.allowed_diff[0])}
    )
    assert not differential_gate.verify_policy_completeness(duplicated_diff).passed

    for field in ("source_files", "symbols", "exports", "registry_keys"):
        value = getattr(universe, field)
        narrowed_universe = universe.model_copy(update={field: value[:-1]})
        narrowed = policy.model_copy(update={"forbidden_definition_universe": narrowed_universe})
        assert not differential_gate.verify_policy_completeness(narrowed).passed

    narrowed = policy.model_copy(
        update={"production_declarations": policy.production_declarations[:-1]}
    )
    assert not differential_gate.verify_policy_completeness(narrowed).passed
    narrowed = policy.model_copy(update={"root_snapshots": policy.root_snapshots[:-1]})
    assert not differential_gate.verify_policy_completeness(narrowed).passed
    narrowed = policy.model_copy(update={"current_fixtures": policy.current_fixtures[:-1]})
    assert not differential_gate.verify_policy_completeness(narrowed).passed
    narrowed = policy.model_copy(
        update={"source_file_snapshots": policy.source_file_snapshots[:-1]}
    )
    assert not differential_gate.verify_policy_completeness(narrowed).passed
    first_snapshot = policy.source_file_snapshots[0]
    incomplete_snapshot = first_snapshot.model_copy(
        update={"declarations": first_snapshot.declarations[:-1]}
    )
    assert not differential_gate.verify_top_level_source_closure(
        ROOT,
        (incomplete_snapshot, *policy.source_file_snapshots[1:]),
    ).passed
    changed_probe = policy.boundary_probes[0].model_copy(update={"behavior_test": "wrong"})
    narrowed = policy.model_copy(
        update={"boundary_probes": (changed_probe, *policy.boundary_probes[1:])}
    )
    assert not differential_gate.verify_policy_completeness(narrowed).passed
    tampered_fixture = policy.fixtures[0].model_copy(update={"sha256": "0" * 64})
    tampered = policy.model_copy(update={"fixtures": (tampered_fixture, *policy.fixtures[1:])})
    assert not differential_gate.verify_policy_completeness(tampered).passed


def test_production_declaration_spans_and_normalized_asts_are_exact() -> None:
    policy = load_policy(POLICY_PATH)
    for declaration in policy.production_declarations:
        assert verify_production_declaration(ROOT, declaration).passed


def test_boundary_source_spans_and_normalized_asts_are_exact() -> None:
    policy = load_policy(POLICY_PATH)
    for probe in policy.boundary_probes:
        assert verify_boundary_probe_source(ROOT, probe).passed


def test_policy_is_canonical_and_binds_requested_baseline() -> None:
    policy = load_policy(POLICY_PATH)
    assert policy.baseline_commit_sha == BASELINE_COMMIT_SHA
    assert policy.baseline_tree_sha == BASELINE_TREE_SHA
    assert policy.canonical_bytes == POLICY_PATH.read_bytes()
