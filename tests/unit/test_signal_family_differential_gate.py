"""R07-DR-GATE-V1 strict evidence, diff, and static source gates."""

from __future__ import annotations

import ast
import copy
import hashlib
import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass, replace
from pathlib import Path

import pytest

import rquant.signal_family_differential_gate as differential_gate
from rquant.signal_family_differential_gate import (
    BASELINE_COMMIT_SHA,
    BASELINE_TREE_SHA,
    FORBIDDEN_DEFINITION_UNIVERSE,
    R07_CI_EVIDENCE_PRODUCER_IMPLEMENTED,
    ROOT_SNAPSHOTS,
    BoundaryProbeResultV1,
    CandidateGateResult,
    PythonRunEvidenceV1,
    R07DrGateEvidenceWireV1,
    R07PolicyV1,
    R07StaticGateResult,
    VerifiedR07DrGateEvidenceV1,
    canonical_evidence_json_bytes,
    collect_complete_git_diff,
    load_policy,
    python_run_result_digest,
    resolve_fixture_values,
    verify_boundary_probe_source,
    verify_forbidden_definitions,
    verify_production_declaration,
    verify_r07_static_gate,
    verify_root_snapshot,
    verify_wire,
)
from tests.r07_differential_probe_runner import run_boundary_probe_subprocess

ROOT = Path(__file__).parents[2]
POLICY_PATH = ROOT / "tests" / "fixtures" / "r07_differential_gate" / "policy-v1.json"


def _run(
    minor: str,
    job_id: str,
    *,
    candidate_commit: str,
    candidate_tree: str,
) -> PythonRunEvidenceV1:
    run = PythonRunEvidenceV1(
        python_minor=minor,
        job_id=job_id,
        job_run_id=101 if minor == "3.11" else 102,
        workflow_run_id=100,
        run_attempt=1,
        candidate_commit_sha=candidate_commit,
        candidate_tree_sha=candidate_tree,
        collected=7,
        passed=7,
        skipped=0,
        deselected=0,
        result_digest="0" * 64,
        outcome="passed",
    )
    return run.model_copy(update={"result_digest": python_run_result_digest(run)})


@dataclass(frozen=True)
class _EvidenceBundle:
    evidence: VerifiedR07DrGateEvidenceV1
    policy: R07PolicyV1
    candidate_gate: CandidateGateResult
    static_result: R07StaticGateResult
    boundary_results: tuple[BoundaryProbeResultV1, ...]
    python_runs: tuple[PythonRunEvidenceV1, PythonRunEvidenceV1]


@pytest.fixture(scope="module")
def evidence_bundle(tmp_path_factory: pytest.TempPathFactory) -> _EvidenceBundle:
    policy = load_policy(POLICY_PATH)
    candidate = _head()
    candidate_tree = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", f"{candidate}^{{tree}}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    candidate_gate = differential_gate.verify_candidate_gate(
        ROOT,
        policy=policy,
        candidate_commit=candidate,
        candidate_tree=candidate_tree,
    )
    assert candidate_gate.passed
    static_result = verify_r07_static_gate(
        ROOT,
        policy=policy,
        candidate_commit=candidate,
        candidate_tree=candidate_tree,
    )
    assert static_result.passed
    probe_root = tmp_path_factory.mktemp("r07-evidence-probes")
    boundary_results = tuple(
        BoundaryProbeResultV1.model_validate(
            run_boundary_probe_subprocess(
                policy_path=POLICY_PATH,
                inventory_id=f"R07-B{index:02d}",
                tmp_path=probe_root / f"b{index:02d}",
            )
        )
        for index in range(1, 18)
    )
    python_runs = (
        _run(
            "3.11",
            "r07-differential-gate-py311",
            candidate_commit=candidate,
            candidate_tree=candidate_tree,
        ),
        _run(
            "3.12",
            "r07-differential-gate-py312",
            candidate_commit=candidate,
            candidate_tree=candidate_tree,
        ),
    )
    evidence = VerifiedR07DrGateEvidenceV1.from_gate_results(
        repo=ROOT,
        policy=policy,
        candidate_gate=candidate_gate,
        boundary_results=boundary_results,
        static_result=static_result,
        python_runs=python_runs,
        workflow_run_id=100,
        run_attempt=1,
    )
    return _EvidenceBundle(
        evidence=evidence,
        policy=policy,
        candidate_gate=candidate_gate,
        static_result=static_result,
        boundary_results=boundary_results,
        python_runs=python_runs,
    )


def test_evidence_uses_exact_fields_and_canonical_self_digest(
    evidence_bundle: _EvidenceBundle,
) -> None:
    assert "candidate_binding_digest" in R07DrGateEvidenceWireV1.model_fields
    assert hasattr(VerifiedR07DrGateEvidenceV1, "from_gate_results")
    assert not hasattr(VerifiedR07DrGateEvidenceV1, "from_canonical_json")
    assert R07_CI_EVIDENCE_PRODUCER_IMPLEMENTED is True
    evidence = evidence_bundle.evidence
    assert (
        evidence.candidate_binding_digest == evidence_bundle.candidate_gate.candidate_binding_digest
    )
    raw = canonical_evidence_json_bytes(evidence)
    assert raw == canonical_evidence_json_bytes(evidence)
    assert json.loads(raw)["evidence_digest"] == evidence.evidence_digest
    with pytest.raises((TypeError, ValueError)):
        VerifiedR07DrGateEvidenceV1(**evidence.model_dump(mode="python"))

    forged_gate = replace(evidence_bundle.candidate_gate, candidate_tree_sha="0" * 40)
    with pytest.raises(ValueError, match="candidate tree"):
        VerifiedR07DrGateEvidenceV1.from_gate_results(
            repo=ROOT,
            policy=evidence_bundle.policy,
            candidate_gate=forged_gate,
            boundary_results=evidence_bundle.boundary_results,
            static_result=evidence_bundle.static_result,
            python_runs=evidence_bundle.python_runs,
            workflow_run_id=100,
            run_attempt=1,
        )
    forged_diff = replace(evidence_bundle.candidate_gate, diff_digest="0" * 64)
    with pytest.raises(ValueError, match="wire|recomputed"):
        VerifiedR07DrGateEvidenceV1.from_gate_results(
            repo=ROOT,
            policy=evidence_bundle.policy,
            candidate_gate=forged_diff,
            boundary_results=evidence_bundle.boundary_results,
            static_result=evidence_bundle.static_result,
            python_runs=evidence_bundle.python_runs,
            workflow_run_id=100,
            run_attempt=1,
        )
    forged_static = replace(evidence_bundle.static_result, candidate_tree_sha="0" * 40)
    with pytest.raises(ValueError, match="wire|static"):
        VerifiedR07DrGateEvidenceV1.from_gate_results(
            repo=ROOT,
            policy=evidence_bundle.policy,
            candidate_gate=evidence_bundle.candidate_gate,
            boundary_results=evidence_bundle.boundary_results,
            static_result=forged_static,
            python_runs=evidence_bundle.python_runs,
            workflow_run_id=100,
            run_attempt=1,
        )


def test_private_verifier_rejects_self_consistent_fake_and_wrong_binding_wires(
    evidence_bundle: _EvidenceBundle,
) -> None:
    wire = evidence_bundle.evidence.wire
    fake_commit = "0" * 40
    fake_tree = "1" * 40
    fake_runs = tuple(
        run.model_copy(
            update={"candidate_commit_sha": fake_commit, "candidate_tree_sha": fake_tree}
        )
        for run in wire.python_runs
    )
    fake_values = wire.model_dump(mode="python")
    fake_values.update(
        {
            "candidate_commit_sha": fake_commit,
            "candidate_tree_sha": fake_tree,
            "candidate_binding_digest": differential_gate._candidate_binding_digest_values(
                baseline_commit_sha=wire.baseline_commit_sha,
                baseline_tree_sha=wire.baseline_tree_sha,
                candidate_commit_sha=fake_commit,
                candidate_tree_sha=fake_tree,
                complete_diff_digest=wire.complete_diff_digest,
            ),
            "python_runs": fake_runs,
            "artifact_name": f"r07-dr-gate-{fake_commit}",
            "evidence_digest": "0" * 64,
        }
    )
    provisional = R07DrGateEvidenceWireV1.model_construct(**fake_values)
    fake_values["evidence_digest"] = differential_gate._digest_without_field(
        provisional,
        "evidence_digest",
    )
    fake_wire = R07DrGateEvidenceWireV1.model_validate(fake_values)
    with pytest.raises(ValueError, match="Git binding"):
        verify_wire(ROOT, evidence_bundle.policy, fake_wire)

    wrong_binding = wire.model_copy(update={"candidate_binding_digest": "0" * 64})
    with pytest.raises(ValueError):
        verify_wire(ROOT, evidence_bundle.policy, wrong_binding)


@pytest.mark.parametrize(
    "payload",
    [
        lambda evidence: evidence.model_dump(mode="json") | {"extra": 1},
        lambda evidence: evidence.model_dump(mode="json") | {"retention_days": True},
        lambda evidence: evidence.model_dump(mode="json") | {"python_runs": []},
    ],
)
def test_evidence_rejects_extra_coerced_or_incomplete_json(
    payload,
    evidence_bundle: _EvidenceBundle,
) -> None:
    with pytest.raises(ValueError):
        R07DrGateEvidenceWireV1.from_canonical_json(
            json.dumps(payload(evidence_bundle.evidence), separators=(",", ":"))
        )


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


def _head(repo: Path = ROOT) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


@pytest.mark.parametrize(
    "source",
    (
        "def sample(value: int) -> int:\n    return value\n",
        "async def sample(value: int) -> int:\n    return value\n",
        "class Sample:\n    value = 1\n",
    ),
    ids=("function", "async-function", "class"),
)
def test_normalized_ast_dump_treats_synthetic_311_and_312_shapes_as_equal(
    source: str,
) -> None:
    empty_type_params_shape = ast.parse(source).body[0]
    for node in ast.walk(empty_type_params_shape):
        if type(node).__name__ in {"FunctionDef", "AsyncFunctionDef", "ClassDef"}:
            if "type_params" not in node._fields:
                node._fields = (*node._fields, "type_params")
            node.type_params = []
    absent_type_params_shape = copy.deepcopy(empty_type_params_shape)
    for node in ast.walk(absent_type_params_shape):
        if type(node).__name__ in {"FunctionDef", "AsyncFunctionDef", "ClassDef"}:
            node._fields = tuple(field for field in node._fields if field != "type_params")
            if hasattr(node, "type_params"):
                delattr(node, "type_params")
    nonempty_type_params_shape = copy.deepcopy(empty_type_params_shape)
    for node in ast.walk(nonempty_type_params_shape):
        if type(node).__name__ in {"FunctionDef", "AsyncFunctionDef", "ClassDef"}:
            node.type_params = [ast.Name(id="T", ctx=ast.Load())]

    assert ast.dump(absent_type_params_shape, include_attributes=False) != ast.dump(
        empty_type_params_shape,
        include_attributes=False,
    )
    assert differential_gate.normalized_ast_dump(
        absent_type_params_shape
    ) == differential_gate.normalized_ast_dump(empty_type_params_shape)
    assert differential_gate.normalized_ast_dump(
        nonempty_type_params_shape
    ) != differential_gate.normalized_ast_dump(empty_type_params_shape)


def test_python311_normalizer_runs_when_local_runtime_is_usable_or_records_ci_need() -> None:
    source = "def sample(value: int) -> int:\n    return value\n"
    expected = differential_gate.normalized_ast_dump(ast.parse(source).body[0])
    if sys.version_info[:2] == (3, 11):
        assert differential_gate.normalized_ast_dump(ast.parse(source).body[0]) == expected
        return

    executable = shutil.which("python3.11")
    if executable is None:
        assert "r07-differential-gate-py311" in load_policy(POLICY_PATH).evidence_channel.jobs
        return
    completed = subprocess.run(
        [
            executable,
            "-c",
            (
                "import ast; "
                "from rquant.signal_family_differential_gate import normalized_ast_dump; "
                f"print(normalized_ast_dump(ast.parse({source!r}).body[0]))"
            ),
        ],
        cwd=ROOT,
        env={
            "PATH": os.environ.get("PATH", ""),
            "PYTHONPATH": os.pathsep.join((str(ROOT / "src"), str(ROOT))),
            "PYTHONNOUSERSITE": "1",
            "RQUANT_DISABLE_DOTENV": "1",
            "TUSHARE_TOKEN_MAIN": "0" * 32,
        },
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        assert completed.stderr
        assert "r07-differential-gate-py311" in load_policy(POLICY_PATH).evidence_channel.jobs
        return
    assert completed.stdout.strip() == expected


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
    assert result.diff_entries
    assert len(result.candidate_binding_digest) == 64
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
    differential_gate.validate_complete_diff_objects(repo, result)
    modified = next(entry for entry in result.entries if entry.path == "modified.txt")
    assert (
        modified.old_object
        == subprocess.run(
            ["git", "-C", str(repo), "rev-parse", f"{baseline}:modified.txt"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    )
    assert (
        modified.new_object
        == subprocess.run(
            ["git", "-C", str(repo), "rev-parse", f"{candidate}:modified.txt"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    )
    forged = replace(
        result,
        entries=(
            replace(modified, new_object="f" * 40),
            *(entry for entry in result.entries if entry is not modified),
        ),
    )
    with pytest.raises(ValueError, match="object|blob"):
        differential_gate.validate_complete_diff_objects(repo, forged)
    forged_status = replace(
        result,
        entries=(
            replace(modified, status="T"),
            *(entry for entry in result.entries if entry is not modified),
        ),
    )
    with pytest.raises(ValueError, match="status"):
        differential_gate.validate_complete_diff_objects(repo, forged_status)


def test_exact_commit_static_source_ignores_dirty_tracked_worktree(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    subprocess.run(["git", "init", "--quiet", str(repo)], check=True)
    module_path = "src/rquant/runtime_service_main.py"
    source_path = repo / module_path
    source_path.parent.mkdir(parents=True)
    source_path.write_text("registry = {}\ndef build_builtin_registry():\n    return registry\n")
    candidate = _commit(repo, "candidate")
    snapshot = differential_gate.source_file_snapshot(repo, candidate, module_path)

    source_path.write_text("registry = {}\nv3_writer = object()\n")

    assert differential_gate.source_file_snapshot(repo, candidate, module_path) == snapshot
    assert differential_gate.verify_top_level_source_closure(
        repo,
        candidate,
        (snapshot,),
    ).passed


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
        assert verify_root_snapshot(ROOT, _head(), snapshot).passed


def test_forbidden_definition_universe_is_static_source_only() -> None:
    assert "src/rquant/signal_route_spool.py" in FORBIDDEN_DEFINITION_UNIVERSE.source_files
    assert verify_forbidden_definitions(
        ROOT,
        _head(),
        FORBIDDEN_DEFINITION_UNIVERSE,
    ).passed
    policy = load_policy(POLICY_PATH)
    assert len(policy.source_file_snapshots) == 9
    verifier = getattr(differential_gate, "verify_top_level_source_closure", None)
    assert verifier is not None
    assert verifier(ROOT, _head(), policy.source_file_snapshots).passed


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
    subprocess.run(["git", "init", "--quiet", str(tmp_path)], check=True)
    policy = load_policy(POLICY_PATH)
    for snapshot in policy.source_file_snapshots:
        source = ROOT / snapshot.module_path
        target = tmp_path / snapshot.module_path
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
    _commit(tmp_path, "frozen")
    target = tmp_path / "src/rquant/runtime_service_main.py"
    target.write_text(target.read_text(encoding="utf-8") + "\n" + tamper, encoding="utf-8")
    candidate = _commit(tmp_path, "tamper")
    result = differential_gate.verify_top_level_source_closure(
        tmp_path,
        candidate,
        policy.source_file_snapshots,
    )
    assert not result.passed


def test_forbidden_definition_gate_detects_symbols_and_registry_string_keys(
    tmp_path: Path,
) -> None:
    subprocess.run(["git", "init", "--quiet", str(tmp_path)], check=True)
    module_path = Path("src/rquant/runtime_service_main.py")
    source_path = tmp_path / module_path
    source_path.parent.mkdir(parents=True)
    source_path.write_text(
        "v3_writer = object()\nregistry = {'v3_overlay': v3_writer}\n",
        encoding="utf-8",
    )
    candidate = _commit(tmp_path, "forbidden")
    universe = FORBIDDEN_DEFINITION_UNIVERSE.model_copy(
        update={"source_files": (module_path.as_posix(),)}
    )
    result = verify_forbidden_definitions(tmp_path, candidate, universe)
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
        _head(),
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
        assert verify_production_declaration(ROOT, _head(), declaration).passed


def test_boundary_source_spans_and_normalized_asts_are_exact() -> None:
    policy = load_policy(POLICY_PATH)
    for probe in policy.boundary_probes:
        assert verify_boundary_probe_source(ROOT, _head(), probe).passed


def test_policy_is_canonical_and_binds_requested_baseline() -> None:
    policy = load_policy(POLICY_PATH)
    assert policy.baseline_commit_sha == BASELINE_COMMIT_SHA
    assert policy.baseline_tree_sha == BASELINE_TREE_SHA
    assert policy.canonical_bytes == POLICY_PATH.read_bytes()


def test_production_category_is_reserved_for_declaration_scanned_sources() -> None:
    policy = load_policy(POLICY_PATH)
    production_paths = {
        entry.path for entry in policy.allowed_diff if entry.category == "production"
    }
    assert production_paths
    assert all(path.startswith("src/rquant/") for path in production_paths)
    scanned = {declaration.module_path for declaration in policy.production_declarations}
    scanned |= set(policy.forbidden_definition_universe.source_files)
    scanned |= {snapshot.module_path for snapshot in policy.source_file_snapshots}
    assert scanned
    assert all(path.startswith("src/rquant/") for path in scanned)
    architecture_paths = {
        entry.path for entry in policy.allowed_diff if entry.category == "architecture"
    }
    assert "scripts/r07_ci_evidence.py" in architecture_paths
    assert ".github/workflows/ci.yml" in architecture_paths
