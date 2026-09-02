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
from collections.abc import Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import pytest

import rquant.signal_family_differential_gate as differential_gate
from rquant.signal_family_differential_gate import (
    BASELINE_COMMIT_SHA,
    BASELINE_TREE_SHA,
    FORBIDDEN_DEFINITION_UNIVERSE,
    HISTORICAL_BASELINE_COMMIT_SHA,
    R07_CI_EVIDENCE_PRODUCER_IMPLEMENTED,
    ROOT_SNAPSHOTS,
    BoundaryProbeResultV1,
    CandidateGateResult,
    PythonRunEvidenceV1,
    R07DrGateEvidenceWireV1,
    R07PolicyV1,
    R07StaticGateResult,
    VerifiedR07DrGateEvidenceV1,
    boundary_probe_results_digest,
    candidate_gate_digest,
    canonical_evidence_json_bytes,
    check_inventory_digest,
    collect_complete_git_diff,
    expected_gate_check_total,
    gate_check_inventory,
    load_policy,
    parse_git_version,
    python_run_result_digest,
    require_merge_tree_git_version,
    resolve_fixture_values,
    resolve_merge_provenance,
    static_gate_result_digest,
    verify_boundary_probe_source,
    verify_diff_scope_forbidden_definitions,
    verify_forbidden_definitions,
    verify_production_declaration,
    verify_r07_static_gate,
    verify_root_snapshot,
    verify_wire,
)
from tests.r07_differential_probe_runner import run_boundary_probe_subprocess
from tests.support.r07_git_fixtures import merge_fixture_repo, write_github_event

ROOT = Path(__file__).parents[2]
POLICY_PATH = ROOT / "tests" / "fixtures" / "r07_differential_gate" / "policy-v1.json"
_GATE_CHECK_TOTAL = expected_gate_check_total(load_policy(POLICY_PATH))


def _run(
    minor: str,
    job_id: str,
    *,
    candidate_commit: str,
    candidate_tree: str,
    gate_digests: dict[str, str],
) -> PythonRunEvidenceV1:
    run = PythonRunEvidenceV1(
        python_minor=minor,
        job_id=job_id,
        job_run_id=101 if minor == "3.11" else 102,
        workflow_run_id=100,
        run_attempt=1,
        candidate_commit_sha=candidate_commit,
        candidate_tree_sha=candidate_tree,
        collected=_GATE_CHECK_TOTAL,
        passed=_GATE_CHECK_TOTAL,
        skipped=0,
        deselected=0,
        candidate_gate_digest=gate_digests["candidate_gate_digest"],
        static_result_digest=gate_digests["static_result_digest"],
        boundary_result_digest=gate_digests["boundary_result_digest"],
        check_inventory_digest=gate_digests["check_inventory_digest"],
        result_digest="0" * 64,
        outcome="passed",
    )
    return run.model_copy(update={"result_digest": python_run_result_digest(run)})


def _shared_clone(destination: Path) -> Path:
    """Clone the repository under test so no test ever writes into its object store.

    ``--shared`` points the clone at the original object database through an alternate, so
    the clone costs milliseconds and still resolves every existing object, while anything a
    test writes lands only in the throwaway clone.
    """

    subprocess.run(
        [
            "git",
            "clone",
            "--quiet",
            "--shared",
            "--no-checkout",
            str(ROOT),
            str(destination),
        ],
        check=True,
        capture_output=True,
    )
    return destination


def _newest_non_merge_commit() -> str:
    """The newest single-parent commit reachable from the checkout.

    Work packages are merged back into the integration branch with real merge commits, so the
    tip itself is a two-parent object most of the time. Tests that need the shape a squash or
    a direct push leaves behind have to name a non-merge commit explicitly.
    """

    return subprocess.run(
        ["git", "-C", str(ROOT), "rev-list", "--no-merges", "-n", "1", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _synthetic_merge_candidate(repo: Path) -> str:
    """Write the exact object GitHub's "Create a merge commit" would create for this branch.

    The commit is dangling: no ref moves, no working tree changes, and its fixed identity
    keeps the object content-addressed and stable across runs. It is written into a
    throwaway clone, never into the repository under test.
    """

    head = _head(repo)
    tree = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "--verify", f"{head}^{{tree}}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    environment = {
        **os.environ,
        "GIT_AUTHOR_NAME": "r07-merge-fixture",
        "GIT_AUTHOR_EMAIL": "r07@example.invalid",
        "GIT_COMMITTER_NAME": "r07-merge-fixture",
        "GIT_COMMITTER_EMAIL": "r07@example.invalid",
        "GIT_AUTHOR_DATE": "2026-01-01T00:00:00+00:00",
        "GIT_COMMITTER_DATE": "2026-01-01T00:00:00+00:00",
    }
    return subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "commit-tree",
            tree,
            "-p",
            BASELINE_COMMIT_SHA,
            "-p",
            head,
            "-m",
            "Merge pull request into main",
        ],
        check=True,
        capture_output=True,
        text=True,
        env=environment,
    ).stdout.strip()


@dataclass(frozen=True)
class _EvidenceBundle:
    evidence: VerifiedR07DrGateEvidenceV1
    repo: Path
    policy: R07PolicyV1
    candidate_gate: CandidateGateResult
    static_result: R07StaticGateResult
    boundary_results: tuple[BoundaryProbeResultV1, ...]
    python_runs: tuple[PythonRunEvidenceV1, PythonRunEvidenceV1]


@pytest.fixture(scope="module")
def evidence_bundle(tmp_path_factory: pytest.TempPathFactory) -> _EvidenceBundle:
    policy = load_policy(POLICY_PATH)
    repo = _shared_clone(tmp_path_factory.mktemp("r07-candidate-repo") / "clone")
    candidate = _synthetic_merge_candidate(repo)
    candidate_tree = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", f"{candidate}^{{tree}}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    candidate_gate = differential_gate.verify_candidate_gate(
        repo,
        policy=policy,
        candidate_commit=candidate,
        candidate_tree=candidate_tree,
    )
    assert candidate_gate.passed
    static_result = verify_r07_static_gate(
        repo,
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
    gate_digests = {
        "candidate_gate_digest": candidate_gate_digest(candidate_gate),
        "static_result_digest": static_gate_result_digest(static_result),
        "boundary_result_digest": boundary_probe_results_digest(policy, boundary_results),
        "check_inventory_digest": check_inventory_digest(
            gate_check_inventory(policy, static_result, boundary_results)
        ),
    }
    python_runs = (
        _run(
            "3.11",
            "r07-differential-gate-py311",
            candidate_commit=candidate,
            candidate_tree=candidate_tree,
            gate_digests=gate_digests,
        ),
        _run(
            "3.12",
            "r07-differential-gate-py312",
            candidate_commit=candidate,
            candidate_tree=candidate_tree,
            gate_digests=gate_digests,
        ),
    )
    evidence = VerifiedR07DrGateEvidenceV1.from_gate_results(
        repo=repo,
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
        repo=repo,
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
            repo=evidence_bundle.repo,
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
            repo=evidence_bundle.repo,
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
            repo=evidence_bundle.repo,
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
            "merge_tree_sha": fake_tree,
            "candidate_parent_commits": (BASELINE_COMMIT_SHA, fake_commit),
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
        verify_wire(evidence_bundle.repo, evidence_bundle.policy, fake_wire)

    wrong_binding = wire.model_copy(update={"candidate_binding_digest": "0" * 64})
    with pytest.raises(ValueError):
        verify_wire(evidence_bundle.repo, evidence_bundle.policy, wrong_binding)


def test_private_verifier_requires_policy_derived_python_run_counts(
    evidence_bundle: _EvidenceBundle,
) -> None:
    wire = evidence_bundle.evidence.wire
    assert wire.python_runs[0].collected == _GATE_CHECK_TOTAL
    forged_runs = tuple(
        run.model_copy(update={"collected": 1, "passed": 1}) for run in wire.python_runs
    )
    forged_runs = tuple(
        run.model_copy(update={"result_digest": python_run_result_digest(run)})
        for run in forged_runs
    )
    values = wire.model_dump(mode="python")
    values.update({"python_runs": forged_runs, "evidence_digest": "0" * 64})
    provisional = R07DrGateEvidenceWireV1.model_construct(**values)
    values["evidence_digest"] = differential_gate._digest_without_field(
        provisional,
        "evidence_digest",
    )
    forged_wire = R07DrGateEvidenceWireV1.model_validate(values)

    with pytest.raises(ValueError, match="check count"):
        verify_wire(evidence_bundle.repo, evidence_bundle.policy, forged_wire)


def test_verified_evidence_binds_the_final_merge_tree_and_its_exact_parents(
    evidence_bundle: _EvidenceBundle,
) -> None:
    wire = evidence_bundle.evidence.wire

    assert wire.merge_base_commit_sha == BASELINE_COMMIT_SHA
    assert wire.merge_base_tree_sha == BASELINE_TREE_SHA
    assert wire.candidate_parent_commits == (BASELINE_COMMIT_SHA, _head())
    assert wire.merge_tree_sha == wire.candidate_tree_sha
    assert (
        subprocess.run(
            [
                "git",
                "-C",
                str(evidence_bundle.repo),
                "merge-tree",
                "--write-tree",
                BASELINE_COMMIT_SHA,
                _head(),
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        == wire.merge_tree_sha
    )
    # R2B-SPEC-09: the candidate object lives only in the throwaway clone.
    assert (
        subprocess.run(
            ["git", "-C", str(ROOT), "cat-file", "-e", f"{wire.candidate_commit_sha}^{{commit}}"],
            check=False,
            capture_output=True,
        ).returncode
        != 0
    )
    assert (
        subprocess.run(
            [
                "git",
                "-C",
                str(evidence_bundle.repo),
                "cat-file",
                "-e",
                f"{wire.candidate_commit_sha}^{{commit}}",
            ],
            check=False,
            capture_output=True,
        ).returncode
        == 0
    )


def test_private_verifier_refuses_a_candidate_that_is_not_a_merge_commit() -> None:
    # The branch tip stops being single-parent as soon as a work package is merged back into
    # it, so the direct-push shape under test is taken from the newest non-merge commit rather
    # than from HEAD.
    non_merge = _newest_non_merge_commit()
    parents = subprocess.run(
        ["git", "-C", str(ROOT), "rev-list", "--parents", "-n", "1", non_merge],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.split()

    assert len(parents) == 2  # that commit has exactly one parent

    with pytest.raises(ValueError, match="two-parent merge commit"):
        resolve_merge_provenance(
            ROOT,
            candidate_commit=non_merge,
            merge_base_commit=BASELINE_COMMIT_SHA,
        )


def test_candidate_gate_requires_the_merge_base_to_be_a_candidate_ancestor(
    tmp_path: Path,
) -> None:
    """The merge-base ancestry check is load bearing on its own, not only through provenance."""

    repo = _shared_clone(tmp_path / "unrelated")
    unrelated = subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "commit-tree",
            BASELINE_TREE_SHA,
            "-m",
            "a commit that never descended from the frozen merge base",
        ],
        check=True,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "GIT_AUTHOR_NAME": "r07-unrelated",
            "GIT_AUTHOR_EMAIL": "r07@example.invalid",
            "GIT_COMMITTER_NAME": "r07-unrelated",
            "GIT_COMMITTER_EMAIL": "r07@example.invalid",
            "GIT_AUTHOR_DATE": "2026-01-01T00:00:00+00:00",
            "GIT_COMMITTER_DATE": "2026-01-01T00:00:00+00:00",
        },
    ).stdout.strip()
    assert (
        subprocess.run(
            [
                "git",
                "-C",
                str(repo),
                "merge-base",
                "--is-ancestor",
                BASELINE_COMMIT_SHA,
                unrelated,
            ],
            check=False,
            capture_output=True,
        ).returncode
        != 0
    )

    with pytest.raises(ValueError, match="does not descend from the policy baseline merge base"):
        differential_gate.verify_candidate_gate(
            repo,
            policy=load_policy(POLICY_PATH),
            candidate_commit=unrelated,
            candidate_tree=BASELINE_TREE_SHA,
        )


def test_wire_rejects_divergent_dual_python_gate_digests(
    evidence_bundle: _EvidenceBundle,
) -> None:
    wire = evidence_bundle.evidence.wire
    diverged = wire.python_runs[1].model_copy(update={"boundary_result_digest": "9" * 64})
    diverged = diverged.model_copy(update={"result_digest": python_run_result_digest(diverged)})
    values = wire.model_dump(mode="python")
    values.update({"python_runs": (wire.python_runs[0], diverged), "evidence_digest": "0" * 64})
    provisional = R07DrGateEvidenceWireV1.model_construct(**values)
    values["evidence_digest"] = differential_gate._digest_without_field(
        provisional,
        "evidence_digest",
    )

    with pytest.raises(ValueError, match="diverge"):
        R07DrGateEvidenceWireV1.model_validate(values)

    forged = R07DrGateEvidenceWireV1.model_construct(**values)
    with pytest.raises(ValueError, match="strict validation"):
        verify_wire(evidence_bundle.repo, evidence_bundle.policy, forged)


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
    # Each pull request refreezes the baseline to the merge commit its predecessor left on
    # main; this one moves it from PR #170's to PR #171's. It is the merge base of the
    # endpoints an R07 run states, not something rediscovered from a ref.
    assert BASELINE_COMMIT_SHA == "53ec2b043c42e143df04c17f80172624f6dfabff"
    assert BASELINE_TREE_SHA == "02f2b587fae81bd54f2a3ea194b5cb530aedf9a9"
    assert HISTORICAL_BASELINE_COMMIT_SHA == "45d0b57c4c5cbab1700fa5e3c386c6756892a7d6"
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


def test_the_frozen_baseline_is_the_merge_base_this_checkout_resolves_for_itself() -> None:
    """Renamed from ``..._of_origin_main_and_the_candidate``: that name described the bug.

    Asking ``origin/main`` is exactly what broke. Once the pull request that freezes a
    baseline merges, ``origin/main`` is the candidate, ``merge_base(origin/main, HEAD)`` is
    HEAD, and the assertion can only ever have held before the merge it existed to protect.
    The question it was really asking - what does this candidate's reviewed diff start from -
    is answered here from the candidate's own structure, so it gives the same answer on a
    branch tip, in a fresh clone, and on main itself right after the merge.
    """

    resolution = differential_gate.resolve_baseline_context(ROOT, environ={})

    assert resolution.baseline_commit_sha == BASELINE_COMMIT_SHA
    assert resolution.baseline_tree_sha == BASELINE_TREE_SHA
    assert resolution.context.candidate_sha == _head()
    # Which source answers is decided by this checkout's shape, and both shapes are normal:
    # a developer's branch tip has one parent, every CI checkout has two (a pull request
    # builds the synthesized merge ref, a push to main is the merge commit itself). So the
    # expectation is derived from the shape rather than written down as one of two options -
    # that keeps the assertion discriminating on whichever shape is actually running. Each
    # shape is pinned exactly, against fixture repositories, by
    # test_the_resolution_summary_names_the_source_each_checkout_shape_produces.
    parents = subprocess.run(
        ["git", "-C", str(ROOT), "rev-list", "--parents", "-n", "1", _head()],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.split()[1:]
    expected_source = "git_first_parent" if len(parents) == 2 else "frozen_baseline_fallback"
    assert resolution.context.base_source == expected_source
    assert (
        subprocess.run(
            ["git", "-C", str(ROOT), "rev-parse", "--verify", f"{BASELINE_COMMIT_SHA}^{{tree}}"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        == BASELINE_TREE_SHA
    )
    diff_paths = collect_complete_git_diff(
        ROOT,
        baseline_commit=BASELINE_COMMIT_SHA,
        candidate_commit=_head(),
        include_untracked=False,
    ).entries

    assert len(diff_paths) == len(load_policy(POLICY_PATH).allowed_diff)


def test_candidate_gate_requires_the_historical_baseline_to_remain_an_ancestor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex Git hard constraint 3: the pre-amendment baseline must stay reachable.

    The construction this test used before is gone, and the reason matters. Under the old
    baseline ``9699827b``, ``45d0b57c`` was *not* its ancestor, so a candidate could descend
    from the baseline while having lost the historical one, and passing the baseline itself as
    the candidate reached exactly that state. Every baseline from Release B's ``2df97ed`` on -
    including this topic's ``53ec2b0`` - does have ``45d0b57c`` behind it, so on this
    repository the historical check is now implied by the
    baseline-descent check and cannot be reached through it - asserted below, so nobody reads
    the change as the constraint having been relaxed.

    The constraint itself still has to hold, and it is the second line of defence against a
    squash: a squash's tree is byte-identical to the merge's, and only ancestry and parent
    structure tell them apart. So the check is exercised against a historical baseline the
    candidate demonstrably lacks - a dangling orphan written into a throwaway clone - which
    drives the same code path with the same policy and the same real candidate.
    """

    policy = load_policy(POLICY_PATH)
    candidate = _head()
    candidate_tree = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "--verify", f"{candidate}^{{tree}}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    repo = _shared_clone(tmp_path / "historical-baseline")

    def _is_ancestor(ancestor: str, descendant: str) -> bool:
        return (
            subprocess.run(
                ["git", "-C", str(repo), "merge-base", "--is-ancestor", ancestor, descendant],
                check=False,
                capture_output=True,
            ).returncode
            == 0
        )

    assert _is_ancestor(HISTORICAL_BASELINE_COMMIT_SHA, BASELINE_COMMIT_SHA)
    assert _is_ancestor(BASELINE_COMMIT_SHA, candidate)
    assert _is_ancestor(HISTORICAL_BASELINE_COMMIT_SHA, candidate)

    orphan = subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "commit-tree",
            BASELINE_TREE_SHA,
            "-m",
            "a historical baseline this candidate never contained",
        ],
        check=True,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "GIT_AUTHOR_NAME": "r07-historical-fixture",
            "GIT_AUTHOR_EMAIL": "r07@example.invalid",
            "GIT_COMMITTER_NAME": "r07-historical-fixture",
            "GIT_COMMITTER_EMAIL": "r07@example.invalid",
            "GIT_AUTHOR_DATE": "2026-01-01T00:00:00+00:00",
            "GIT_COMMITTER_DATE": "2026-01-01T00:00:00+00:00",
        },
    ).stdout.strip()
    assert not _is_ancestor(orphan, candidate)
    monkeypatch.setattr(differential_gate, "HISTORICAL_BASELINE_COMMIT_SHA", orphan)

    with pytest.raises(ValueError, match="historical baseline"):
        differential_gate.verify_candidate_gate(
            repo,
            policy=policy,
            candidate_commit=candidate,
            candidate_tree=candidate_tree,
        )


def test_candidate_gate_blocks_when_one_allowlist_entry_is_missing() -> None:
    policy = load_policy(POLICY_PATH)
    candidate = _head()
    candidate_tree = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "--verify", f"{candidate}^{{tree}}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    narrowed = policy.model_copy(update={"allowed_diff": policy.allowed_diff[:-1]})

    result = differential_gate.verify_candidate_gate(
        ROOT,
        policy=narrowed,
        candidate_commit=candidate,
        candidate_tree=candidate_tree,
    )

    assert not result.passed
    assert len(result.blocked_entries) == 1
    assert result.blocked_entries[0].policy_key == policy.allowed_diff[-1].policy_key


def test_merge_provenance_accepts_a_real_merge_and_rejects_a_squash(tmp_path: Path) -> None:
    repo = tmp_path / "merge-repo"
    identities = merge_fixture_repo(repo)

    provenance = resolve_merge_provenance(
        repo,
        candidate_commit=identities["merge"],
        merge_base_commit=identities["main_tip"],
    )

    merge_tree = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "--verify", f"{identities['merge']}^{{tree}}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert provenance.candidate_parent_commits == (identities["main_tip"], identities["feature"])
    assert provenance.merge_tree_sha == merge_tree
    assert provenance.merge_base_commit_sha == identities["main_tip"]

    with pytest.raises(ValueError, match="two-parent merge commit"):
        resolve_merge_provenance(
            repo,
            candidate_commit=identities["squash"],
            merge_base_commit=identities["main_tip"],
        )

    with pytest.raises(ValueError, match="first parent"):
        resolve_merge_provenance(
            repo,
            candidate_commit=identities["merge"],
            merge_base_commit=identities["base"],
        )


def test_merge_provenance_rejects_a_declared_tree_or_parent_that_git_does_not_produce(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "merge-repo"
    identities = merge_fixture_repo(repo)
    resolved = resolve_merge_provenance(
        repo,
        candidate_commit=identities["merge"],
        merge_base_commit=identities["main_tip"],
    )

    assert (
        differential_gate.verify_merge_provenance(
            repo,
            candidate_commit=identities["merge"],
            candidate_tree=resolved.merge_tree_sha,
            merge_base_commit=identities["main_tip"],
            merge_base_tree=resolved.merge_base_tree_sha,
            declared_parents=resolved.candidate_parent_commits,
            declared_merge_tree=resolved.merge_tree_sha,
        )
        == resolved
    )
    with pytest.raises(ValueError, match="merge tree"):
        differential_gate.verify_merge_provenance(
            repo,
            candidate_commit=identities["merge"],
            candidate_tree="0" * 40,
            merge_base_commit=identities["main_tip"],
            merge_base_tree=resolved.merge_base_tree_sha,
            declared_parents=resolved.candidate_parent_commits,
            declared_merge_tree="0" * 40,
        )
    with pytest.raises(ValueError, match="parent"):
        differential_gate.verify_merge_provenance(
            repo,
            candidate_commit=identities["merge"],
            candidate_tree=resolved.merge_tree_sha,
            merge_base_commit=identities["main_tip"],
            merge_base_tree=resolved.merge_base_tree_sha,
            declared_parents=(identities["main_tip"], identities["main_tip"]),
            declared_merge_tree=resolved.merge_tree_sha,
        )


def test_merge_tree_replay_requires_a_git_that_can_write_it() -> None:
    assert parse_git_version("git version 2.39.2 (Apple Git-143)") == (2, 39, 2)
    assert parse_git_version("git version 2.43.0\n") == (2, 43, 0)
    require_merge_tree_git_version("git version 2.38.0")

    with pytest.raises(ValueError, match="2.38"):
        require_merge_tree_git_version("git version 2.37.9")
    with pytest.raises(ValueError, match="Git version"):
        require_merge_tree_git_version("not a git version")


def test_diff_scope_forbidden_definition_scan_covers_every_diffed_source_file(
    tmp_path: Path,
) -> None:
    policy = load_policy(POLICY_PATH)
    scanned = differential_gate.diff_scope_source_paths(policy)
    # Re-aimed for Release B. These two used to read "the scan is broader than the frozen
    # nine" and "it contains all nine", which were true of the previous baseline's 282-file
    # diff and are shape facts about that particular change set, not properties of the scan.
    # Refreezing the baseline shrinks the diff to this release's own edits, and neither
    # sentence then has a truth value. The property they were protecting - the scan is
    # derived from the reviewed diff, drops nothing from it and invents nothing - is stated
    # directly instead, so it holds for any allowlist.
    expected = {
        entry.path
        for entry in policy.allowed_diff
        if entry.status != "D"
        and entry.path.startswith("src/rquant/")
        and entry.path.endswith(".py")
    }

    assert expected
    assert set(scanned) == expected
    assert scanned == tuple(sorted(scanned))
    assert len(scanned) == len(set(scanned))
    # Whatever part of the frozen nine this diff touches must land inside the scan; the old
    # unconditional containment was the same claim on a diff that happened to touch all nine.
    universe = set(policy.forbidden_definition_universe.source_files)
    assert universe & expected == universe & set(scanned)
    assert all(path.startswith("src/rquant/") and path.endswith(".py") for path in scanned)
    assert verify_diff_scope_forbidden_definitions(ROOT, _head(), policy).passed

    repo = tmp_path / "scope-repo"
    subprocess.run(["git", "init", "--quiet", str(repo)], check=True)
    module_path = "src/rquant/runtime_builder_signal.py"
    source_path = repo / module_path
    source_path.parent.mkdir(parents=True)
    source_path.write_text(
        "def current_signal_writer() -> None:\n    return None\n",
        encoding="utf-8",
    )
    candidate = _commit(repo, "forbidden definition outside the frozen nine")
    narrowed = policy.model_copy(
        update={
            "allowed_diff": (
                policy.allowed_diff[0].model_copy(
                    update={"path": module_path, "category": "production"}
                ),
            )
        }
    )
    result = verify_diff_scope_forbidden_definitions(repo, candidate, narrowed)

    assert not result.passed
    assert "current_signal_writer" in result.reasons[0]


def _diff_scope_scan(tmp_path: Path, name: str, source: str) -> object:
    """Run the diff-scope forbidden-definition scan over one synthetic candidate module."""

    repo = tmp_path / name
    subprocess.run(["git", "init", "--quiet", str(repo)], check=True)
    module_path = "src/rquant/runtime_builder_signal.py"
    source_path = repo / module_path
    source_path.parent.mkdir(parents=True)
    source_path.write_text(source, encoding="utf-8")
    candidate = _commit(repo, "candidate")
    policy = load_policy(POLICY_PATH)
    narrowed = policy.model_copy(
        update={
            "allowed_diff": (
                policy.allowed_diff[0].model_copy(
                    update={"path": module_path, "category": "production"}
                ),
            )
        }
    )
    return verify_diff_scope_forbidden_definitions(repo, candidate, narrowed)


@pytest.mark.parametrize(
    ("name", "source", "expected"),
    (
        pytest.param(
            "nested-def",
            "class Builder:\n    def publish_v3(self) -> None:\n        return None\n",
            "publish_v3",
            id="nested-method-definition",
        ),
        pytest.param(
            "nested-class",
            "def factory() -> None:\n    class CurrentSignalRouteSpoolWriter:\n        pass\n",
            "CurrentSignalRouteSpoolWriter",
            id="nested-class-definition",
        ),
        pytest.param(
            "renamed-import",
            "from rquant.signal_route_spool import publish_v3 as safe_publish\n",
            "publish_v3",
            id="import-renamed-away",
        ),
        pytest.param(
            "alias-assignment",
            "from rquant import signal_route_spool\n\nsafe = signal_route_spool.publish_v3\n",
            "publish_v3",
            id="attribute-alias-assignment",
        ),
        pytest.param(
            "alias-name-assignment",
            "def _load():\n    return None\n\n\npublish_v3 = _load\nsafe = publish_v3\n",
            "publish_v3",
            id="name-alias-assignment",
        ),
        pytest.param(
            "walrus-target",
            "if (publish_v3 := object()):\n    pass\n",
            "publish_v3",
            id="walrus-target-binding",
        ),
        pytest.param(
            "walrus-alias",
            (
                "from rquant import signal_route_spool\n"
                "\n"
                "if (safe := signal_route_spool.publish_v3):\n"
                "    pass\n"
            ),
            "publish_v3",
            id="walrus-alias-binding",
        ),
        pytest.param(
            "dict-literal-symbol-key",
            'HANDLERS = {"publish_v3": None}\n',
            "publish_v3",
            id="dict-literal-symbol-key",
        ),
        pytest.param(
            "dict-literal-registry-key",
            'HANDLERS = {"r07_overlay": None}\n',
            "r07_overlay",
            id="dict-literal-registry-key",
        ),
        pytest.param(
            "annotated-exports",
            '__all__: list[str] = ["publish_v3"]\n',
            "publish_v3",
            id="annotated-all-export",
        ),
        pytest.param(
            "augmented-exports",
            '__all__ = ["ok"]\n__all__ += ["publish_v3"]\n',
            "publish_v3",
            id="augmented-all-reexport",
        ),
    ),
)
def test_diff_scope_scan_blocks_nested_alias_and_reexport_bindings(
    tmp_path: Path,
    name: str,
    source: str,
    expected: str,
) -> None:
    result = _diff_scope_scan(tmp_path, name, source)

    assert not result.passed
    assert expected in result.reasons[0]


def test_diff_scope_scan_still_ignores_mentions_that_bind_nothing(tmp_path: Path) -> None:
    """The declared semantic boundary: mentioning a forbidden name is not a definition."""

    source = (
        '"""This module must never define publish_v3 or v3_writer."""\n'
        "\n"
        "# r07_overlay and CurrentSignalRouteSpoolWriter are forbidden here.\n"
        'NOTE = "r07_activation is a forbidden registry key"\n'
        'FORBIDDEN_NAMES = ("publish_v3", "v3_writer")\n'
    )

    result = _diff_scope_scan(tmp_path, "mentions-only", source)

    assert result.passed
    assert result.reasons == ()


def test_static_gate_counts_the_diff_scope_check_in_the_frozen_inventory() -> None:
    policy = load_policy(POLICY_PATH)

    assert differential_gate.FIXED_STATIC_CHECK_NAMES == (
        "policy-completeness",
        "top-level-source-closure",
        "forbidden-definitions",
        "diff-scope-forbidden-definitions",
    )
    assert expected_gate_check_total(policy) == 60
    static_result = verify_r07_static_gate(
        ROOT,
        policy=policy,
        candidate_commit=_head(),
        candidate_tree=subprocess.run(
            ["git", "-C", str(ROOT), "rev-parse", "--verify", "HEAD^{tree}"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip(),
    )

    assert static_result.passed
    assert "diff-scope-forbidden-definitions" in dict(static_result.checks)


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
    # Re-aimed twice now, for the same reason and by the same rule. Release B dropped
    # scripts/r07_deploy_gate.py from this spot: it was in the previous baseline's diff only
    # because that release created it, so a refrozen baseline left the sentence with no truth
    # value. scripts/r07_ci_evidence.py and .github/workflows/ci.yml were named here on the
    # same false premise and are dropped by the refreeze to 53ec2b0, which this topic does not
    # touch either file across. Naming any path here asserts the shape of one particular diff,
    # and the shape is not the property. The property - tooling that runs in the production
    # chain but lives outside the declaration-scanned universe is categorized architecture,
    # never production - is asserted below against whatever tooling this diff does contain,
    # and the category rule for those two exact paths is pinned directly, independently of any
    # diff, by tests/unit/test_r07_policy_regenerate.py::test_diff_category_rules_are_frozen.
    tooling = {
        entry.path
        for entry in policy.allowed_diff
        if entry.path.startswith(("scripts/", ".github/", "deploy/", "docs/"))
    }
    assert tooling
    assert tooling <= architecture_paths
    assert not {path for path in architecture_paths if path.startswith(("src/", "tests/"))}
    # The negative control: an allowlist that quietly files one tooling path under a
    # declaration-scanned category really does break the containment above. It relabels the
    # tooling this diff actually has rather than assuming a scripts/ entry: with a baseline
    # refrozen onto a topic that changed no script, "every scripts/ path" is the empty set,
    # the copy is identical to the policy, and the control passes itself instead of the code.
    relabelled = policy.model_copy(
        update={
            "allowed_diff": tuple(
                entry.model_copy(update={"category": "production"})
                if entry.path in tooling
                else entry
                for entry in policy.allowed_diff
            )
        }
    )
    relabelled_architecture = {
        entry.path for entry in relabelled.allowed_diff if entry.category == "architecture"
    }
    assert not tooling <= relabelled_architecture


class _GitCommandRecorder:
    """Records every Git argument vector the resolver runs, then delegates to the real one."""

    def __init__(self) -> None:
        self.commands: list[list[str]] = []
        self.CalledProcessError = subprocess.CalledProcessError

    def run(
        self,
        arguments: Sequence[str],
        **keywords: Any,
    ) -> subprocess.CompletedProcess[bytes]:
        self.commands.append([str(part) for part in arguments])
        return subprocess.run(arguments, **keywords)


_GIT_NON_REVISION_TOKENS = frozenset(
    {"git", "rev-parse", "rev-list", "merge-base", "merge-tree", "cat-file", "diff"}
)


def _revision_arguments(commands: list[list[str]]) -> list[str]:
    """Every argument a Git invocation could resolve as a revision."""

    tokens: list[str] = []
    for command in commands:
        for index, part in enumerate(command):
            previous = command[index - 1] if index else ""
            if part in _GIT_NON_REVISION_TOKENS or part.startswith("-") or previous in {"-C", "-n"}:
                continue
            tokens.append(part)
    return tokens


def test_parse_baseline_cli_arguments_validates_what_a_workflow_expression_substitutes() -> None:
    """The workflow can only be exercised by pushing it, so its inputs are decided here.

    A GitHub expression that resolves to nothing substitutes an empty string rather than
    dropping the argument, and ``github.event.before`` is the null commit the first time a
    branch is pushed. Both reach the CLI as ordinary strings, so both are rejected here.
    """

    declared = differential_gate.parse_baseline_cli_arguments(
        event="pull_request",
        base_sha="a" * 40,
        candidate_sha="b" * 40,
    )
    assert declared == differential_gate.DeclaredBaselineArgumentsV1(
        event="pull_request",
        base_sha="a" * 40,
        candidate_sha="b" * 40,
        event_before_sha=None,
    )
    push = differential_gate.parse_baseline_cli_arguments(
        event="push",
        candidate_sha="c" * 40,
        event_before_sha="d" * 40,
    )
    assert (push.base_sha, push.candidate_sha, push.event_before_sha) == (
        None,
        "c" * 40,
        "d" * 40,
    )
    # An expression that yields nothing is absence, not a base.
    assert (
        differential_gate.parse_baseline_cli_arguments(
            event="push",
            base_sha="",
            candidate_sha="c" * 40,
            event_before_sha="",
        ).base_sha
        is None
    )

    with pytest.raises(ValueError, match="must state its base SHA"):
        differential_gate.parse_baseline_cli_arguments(
            event="pull_request",
            base_sha="",
            candidate_sha="b" * 40,
        )
    with pytest.raises(ValueError, match="no semantics for"):
        differential_gate.parse_baseline_cli_arguments(event="workflow_dispatch")
    with pytest.raises(ValueError, match="lowercase 40-hex"):
        differential_gate.parse_baseline_cli_arguments(
            event="pull_request",
            base_sha="origin/main",
            candidate_sha="b" * 40,
        )
    with pytest.raises(ValueError, match="lowercase 40-hex"):
        differential_gate.parse_baseline_cli_arguments(
            event="pull_request",
            base_sha=("A" * 40),
            candidate_sha="b" * 40,
        )
    with pytest.raises(ValueError, match="null commit"):
        differential_gate.parse_baseline_cli_arguments(
            event="push",
            candidate_sha="c" * 40,
            event_before_sha="0" * 40,
        )
    with pytest.raises(ValueError, match="only a push context"):
        differential_gate.parse_baseline_cli_arguments(
            event="pull_request",
            base_sha="a" * 40,
            candidate_sha="b" * 40,
            event_before_sha="d" * 40,
        )


def test_baseline_context_resolves_a_pull_request_from_its_two_explicit_endpoints(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "pr-repo"
    identities = merge_fixture_repo(repo)

    resolution = differential_gate.resolve_baseline_context(
        repo,
        event="pull_request",
        base_sha=identities["main_tip"],
        candidate_sha=identities["feature"],
        environ={},
        expected_baseline=identities["main_tip"],
    )

    assert resolution.baseline_commit_sha == identities["main_tip"]
    assert resolution.context.event == "pull_request"
    assert resolution.context.base_source == "explicit_cli"
    assert resolution.context.candidate_sha == identities["feature"]
    assert (
        resolution.baseline_tree_sha
        == subprocess.run(
            ["git", "-C", str(repo), "rev-parse", "--verify", f"{identities['main_tip']}^{{tree}}"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    )


def test_a_pull_request_proves_its_merge_base_against_the_head_not_the_synthesized_merge_ref(
    tmp_path: Path,
) -> None:
    """``github.sha`` on a pull request is the merge ref, and its first parent is the base.

    That makes ``merge_base(base, github.sha) == base`` hold for every base whatsoever, so a
    gate that used it would assert nothing. Here the pull request head forked from an older
    commit, so the real merge base is ``base`` and the frozen ``main_tip`` is refused, while
    the same check against the merge ref waves it through.
    """

    repo = tmp_path / "merge-ref-repo"
    identities = merge_fixture_repo(repo)

    with pytest.raises(ValueError, match="not the merge base of this run's stated endpoints"):
        differential_gate.resolve_baseline_context(
            repo,
            event="pull_request",
            base_sha=identities["main_tip"],
            candidate_sha=identities["stale_feature"],
            environ={},
            expected_baseline=identities["main_tip"],
        )

    vacuous = differential_gate.resolve_baseline_context(
        repo,
        event="pull_request",
        base_sha=identities["main_tip"],
        candidate_sha=identities["stale_merge"],
        environ={},
        expected_baseline=identities["main_tip"],
    )
    assert vacuous.baseline_commit_sha == identities["main_tip"]


def test_baseline_context_fails_closed_on_a_wrong_absent_or_unresolvable_base(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "bad-base-repo"
    identities = merge_fixture_repo(repo)

    # F6: an older ancestor is a real commit and a real merge base, just not the frozen one.
    with pytest.raises(ValueError, match="not the merge base of this run's stated endpoints"):
        differential_gate.resolve_baseline_context(
            repo,
            event="pull_request",
            base_sha=identities["base"],
            candidate_sha=identities["feature"],
            environ={},
            expected_baseline=identities["main_tip"],
        )
    # F7: no common ancestor at all must refuse, never fall back to an ancestry test.
    with pytest.raises(ValueError, match="no computable merge base"):
        differential_gate.resolve_baseline_context(
            repo,
            event="pull_request",
            base_sha=identities["orphan"],
            candidate_sha=identities["feature"],
            environ={},
            expected_baseline=identities["main_tip"],
        )
    # F1: a well-formed SHA that names no object, on each endpoint in turn. Failing closed
    # is not the whole requirement - the refusal has to name which endpoint was wrong, or the
    # next person reads a bare git command line and reaches for --no-verify.
    with pytest.raises(ValueError, match="base SHA does not name a commit in this repository"):
        differential_gate.resolve_baseline_context(
            repo,
            event="pull_request",
            base_sha="0" * 40,
            candidate_sha=identities["feature"],
            environ={},
            expected_baseline=identities["main_tip"],
        )
    with pytest.raises(
        ValueError,
        match="candidate SHA does not name a commit in this repository",
    ):
        differential_gate.resolve_baseline_context(
            repo,
            event="pull_request",
            base_sha=identities["main_tip"],
            candidate_sha="0" * 40,
            environ={},
            expected_baseline=identities["main_tip"],
        )
    # ...and the malformed shape on the candidate side too, not only the base side.
    with pytest.raises(ValueError, match="candidate SHA is not a lowercase 40-hex"):
        differential_gate.resolve_baseline_context(
            repo,
            event="pull_request",
            base_sha=identities["main_tip"],
            candidate_sha="refs/pull/1/head",
            environ={},
            expected_baseline=identities["main_tip"],
        )
    # F10: an empty reviewed diff is not a passing gate.
    with pytest.raises(ValueError, match="two distinct commits"):
        differential_gate.resolve_baseline_context(
            repo,
            event="pull_request",
            base_sha=identities["main_tip"],
            candidate_sha=identities["main_tip"],
            environ={},
            expected_baseline=identities["main_tip"],
        )
    # An endpoint without an event is a caller that has not decided which semantics it wants.
    with pytest.raises(ValueError, match="without an event"):
        differential_gate.resolve_baseline_context(
            repo,
            base_sha=identities["main_tip"],
            environ={},
            expected_baseline=identities["main_tip"],
        )


def test_baseline_context_resolves_a_push_from_the_first_parent_and_cross_checks_before(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "push-repo"
    identities = merge_fixture_repo(repo)

    resolution = differential_gate.resolve_baseline_context(
        repo,
        event="push",
        candidate_sha=identities["merge"],
        event_before_sha=identities["main_tip"],
        environ={},
        expected_baseline=identities["main_tip"],
    )

    assert resolution.baseline_commit_sha == identities["main_tip"]
    assert resolution.context.event == "push"
    assert resolution.context.base_source == "git_first_parent"
    assert resolution.context.event_before_sha == identities["main_tip"]

    # F2: a squash has one parent, so a push of it produces no release interval.
    with pytest.raises(ValueError, match="two-parent merge commit"):
        differential_gate.resolve_baseline_context(
            repo,
            event="push",
            candidate_sha=identities["squash"],
            environ={},
            expected_baseline=identities["main_tip"],
        )
    # F3: a stated base that is not the first parent.
    with pytest.raises(ValueError, match="first parent is not the recorded merge base"):
        differential_gate.resolve_baseline_context(
            repo,
            event="push",
            base_sha=identities["base"],
            candidate_sha=identities["merge"],
            environ={},
            expected_baseline=identities["base"],
        )
    # F4: GitHub's own claim about the interval start disagrees with the commit's structure.
    with pytest.raises(ValueError, match="before SHA is not the first parent"):
        differential_gate.resolve_baseline_context(
            repo,
            event="push",
            candidate_sha=identities["merge"],
            event_before_sha=identities["base"],
            environ={},
            expected_baseline=identities["main_tip"],
        )
    # F5: the null commit means the branch was created or reset, never a release interval.
    with pytest.raises(ValueError, match="null commit"):
        differential_gate.resolve_baseline_context(
            repo,
            event="push",
            candidate_sha=identities["merge"],
            event_before_sha="0" * 40,
            environ={},
            expected_baseline=identities["main_tip"],
        )


def test_baseline_context_reads_the_github_event_payload_when_no_arguments_are_given(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "payload-repo"
    identities = merge_fixture_repo(repo)
    payload_path = tmp_path / "event.json"

    write_github_event(
        payload_path,
        {
            "pull_request": {
                "base": {"sha": identities["main_tip"]},
                "head": {"sha": identities["feature"]},
            },
            # A decoy: the merge ref GitHub also exposes must not be picked up as the head.
            "sha": identities["merge"],
        },
    )
    pull_request = differential_gate.resolve_baseline_context(
        repo,
        environ={
            "GITHUB_ACTIONS": "true",
            "GITHUB_EVENT_NAME": "pull_request",
            "GITHUB_EVENT_PATH": str(payload_path),
        },
        expected_baseline=identities["main_tip"],
    )
    assert pull_request.context.candidate_sha == identities["feature"]
    assert pull_request.context.base_source == "github_event_payload"

    write_github_event(
        payload_path,
        {"before": identities["main_tip"], "after": identities["merge"]},
    )
    push = differential_gate.resolve_baseline_context(
        repo,
        environ={
            "GITHUB_ACTIONS": "true",
            "GITHUB_EVENT_NAME": "push",
            "GITHUB_EVENT_PATH": str(payload_path),
        },
        expected_baseline=identities["main_tip"],
    )
    assert push.context.candidate_sha == identities["merge"]
    assert push.context.event_before_sha == identities["main_tip"]
    assert push.baseline_commit_sha == identities["main_tip"]

    for payload, message in (
        ({"pull_request": {"head": {"sha": identities["feature"]}}}, "base.sha"),
        ({"pull_request": {"base": {"sha": "not-a-sha"}}}, "base.sha"),
        ({}, "no pull_request section"),
    ):
        write_github_event(payload_path, payload)
        with pytest.raises(ValueError, match=message):
            differential_gate.resolve_baseline_context(
                repo,
                environ={
                    "GITHUB_EVENT_NAME": "pull_request",
                    "GITHUB_EVENT_PATH": str(payload_path),
                },
                expected_baseline=identities["main_tip"],
            )

    write_github_event(payload_path, {"before": identities["main_tip"]})
    with pytest.raises(ValueError, match="after is not a lowercase"):
        differential_gate.resolve_baseline_context(
            repo,
            environ={
                "GITHUB_EVENT_NAME": "push",
                "GITHUB_EVENT_PATH": str(payload_path),
            },
            expected_baseline=identities["main_tip"],
        )
    with pytest.raises(ValueError, match="GITHUB_EVENT_PATH"):
        differential_gate.resolve_baseline_context(
            repo,
            environ={"GITHUB_EVENT_NAME": "push"},
            expected_baseline=identities["main_tip"],
        )
    write_github_event(payload_path, {})
    with pytest.raises(ValueError, match="no semantics for"):
        differential_gate.resolve_baseline_context(
            repo,
            environ={
                "GITHUB_EVENT_NAME": "schedule",
                "GITHUB_EVENT_PATH": str(payload_path),
            },
            expected_baseline=identities["main_tip"],
        )


def test_a_two_parent_head_resolves_itself_without_naming_origin_main_or_any_ref(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The case the old semantics could not survive: HEAD is the merge that is also main.

    ``merge_base(origin/main, HEAD)`` collapses to HEAD here and denies every frozen constant.
    Reading HEAD's own parents instead answers the same question — what did this release
    interval start from — from the candidate's structure alone, so nothing has to be read back
    off a ref that has already moved.
    """

    repo = tmp_path / "head-is-main-repo"
    identities = merge_fixture_repo(repo)
    subprocess.run(["git", "-C", str(repo), "checkout", "--quiet", "main"], check=True)
    assert _head(repo) == identities["merge"]

    recorder = _GitCommandRecorder()
    monkeypatch.setattr(differential_gate, "subprocess", recorder)
    resolution = differential_gate.resolve_baseline_context(
        repo,
        environ={},
        expected_baseline=identities["main_tip"],
    )

    assert resolution.baseline_commit_sha == identities["main_tip"]
    assert resolution.context.event == "push"
    assert resolution.context.base_source == "git_first_parent"
    assert resolution.context.candidate_sha == identities["merge"]
    assert recorder.commands
    forbidden = ("origin/main", "origin/HEAD", "origin/", "refs/remotes", "@{u}", "@{upstream}")
    assert not [
        command
        for command in recorder.commands
        for part in command
        if any(token in part for token in forbidden)
    ]
    # The single ref this path is allowed to read is the checkout's own HEAD; every other
    # revision it names is an explicit 40-hex object.
    revisions = _revision_arguments(recorder.commands)
    assert revisions.count("HEAD^{commit}") == 1
    assert all(
        differential_gate._is_lower_hex(
            token.removesuffix("^{tree}").removesuffix("^{commit}"),
            length=40,
        )
        for token in revisions
        if token != "HEAD^{commit}"
    )


def test_a_release_merge_on_main_resolves_its_own_baseline_instead_of_failing_by_construction(
    tmp_path: Path,
) -> None:
    """The exact failure this work package exists for, on the real repository.

    Once a release merges, main's tip *is* the merge commit and ``origin/main`` points at it,
    so ``merge_base(origin/main, HEAD)`` is HEAD and no frozen constant can match. That is the
    state every full-suite shard runs in on a push to main, and it is why those tests failed
    deterministically rather than intermittently. The object GitHub's "Create a merge commit"
    would write is materialized in a throwaway clone, HEAD is moved onto it, and the resolver
    is asked with no arguments at all - which is how a shard asks it.
    """

    repo = _shared_clone(tmp_path / "post-merge-main")
    candidate = _synthetic_merge_candidate(repo)
    subprocess.run(
        ["git", "-C", str(repo), "update-ref", "--no-deref", "HEAD", candidate],
        check=True,
        capture_output=True,
    )

    resolution = differential_gate.resolve_baseline_context(repo, environ={})

    assert resolution.context.candidate_sha == candidate
    assert resolution.context.event == "push"
    assert resolution.context.base_source == "git_first_parent"
    assert resolution.baseline_commit_sha == BASELINE_COMMIT_SHA
    assert resolution.baseline_tree_sha == BASELINE_TREE_SHA


def test_a_derived_push_baseline_that_is_not_the_frozen_constant_is_refused(
    tmp_path: Path,
) -> None:
    """The branch that carries the whole push semantics, tested on its own.

    When no base is stated, the base is taken from the candidate's first parent, so the only
    thing standing between "any merge commit at all" and "the merge that continues this
    release" is the equality against the frozen constant at the end. The other push cases
    stop earlier - a squash has one parent, a stated base that disagrees with the first parent
    trips merge provenance - so none of them reaches that equality. Short-circuiting it would
    leave every one of them still green.
    """

    repo = tmp_path / "derived-push-repo"
    identities = merge_fixture_repo(repo)
    subprocess.run(
        ["git", "-C", str(repo), "checkout", "--quiet", "main"],
        check=True,
        capture_output=True,
    )
    assert _head(repo) == identities["merge"]
    first_parent = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "--verify", f"{identities['merge']}^1"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert first_parent == identities["main_tip"]

    # Everything about this candidate is structurally sound - two parents, a real merge tree,
    # a first parent that is the merge base of the pair. It is simply not a continuation of
    # the release the frozen baseline names.
    with pytest.raises(
        ValueError,
        match="first parent is not the frozen R07 baseline",
    ):
        differential_gate.resolve_baseline_context(
            repo,
            environ={},
            expected_baseline=identities["base"],
        )


def test_the_resolution_summary_names_the_source_each_checkout_shape_produces(
    tmp_path: Path,
) -> None:
    """Both checkout shapes, each pinned exactly, against a repository built for the purpose.

    Which source answers is a property of the checkout, not of the code: a branch tip has one
    parent and reaches the frozen-baseline fallback, while every CI checkout has two and
    reaches the first parent. Asserting either one against the repository under test passes on
    a laptop and fails in CI, and asserting "one of these two" stops telling them apart. Both
    are built here instead, so the summary line is pinned character for character on each.
    """

    repo = tmp_path / "shape-repo"
    identities = merge_fixture_repo(repo)

    subprocess.run(
        ["git", "-C", str(repo), "checkout", "--quiet", "main"],
        check=True,
        capture_output=True,
    )
    assert _head(repo) == identities["merge"]
    merged = differential_gate.baseline_resolution_summary(
        differential_gate.resolve_baseline_context(
            repo,
            environ={},
            expected_baseline=identities["main_tip"],
        )
    )
    assert merged == (
        f"R07 baseline: event=push base={identities['main_tip']} "
        f"candidate={identities['merge']} base_source=git_first_parent"
    )

    subprocess.run(
        ["git", "-C", str(repo), "checkout", "--quiet", identities["feature"]],
        check=True,
        capture_output=True,
    )
    assert _head(repo) == identities["feature"]
    tip = differential_gate.baseline_resolution_summary(
        differential_gate.resolve_baseline_context(
            repo,
            environ={},
            expected_baseline=identities["main_tip"],
        )
    )
    assert tip == (
        f"R07 baseline: event=pull_request base={identities['main_tip']} "
        f"candidate={identities['feature']} base_source=frozen_baseline_fallback"
    )
    # The line has to separate them; a summary that read the same either way would be no
    # more useful than the unread label it replaced.
    assert merged != tip

    with pytest.raises(TypeError, match="exact R07BaselineResolutionV1"):
        differential_gate.baseline_resolution_summary(object())  # type: ignore[arg-type]


def test_the_branch_tip_fallback_is_labelled_locally_and_refused_inside_github_actions(
    tmp_path: Path,
) -> None:
    """A single-parent tip has no second endpoint, so the frozen baseline is the base.

    That makes the merge-base equality degenerate into "the baseline is an ancestor of HEAD",
    which is why the mode is labelled and why CI may never take it: in CI the event always
    states both endpoints, so a resolver that silently fell back here would be reporting a
    weaker check under the same name.
    """

    repo = tmp_path / "branch-tip-repo"
    identities = merge_fixture_repo(repo)
    subprocess.run(
        ["git", "-C", str(repo), "checkout", "--quiet", identities["feature"]],
        check=True,
        capture_output=True,
    )

    resolution = differential_gate.resolve_baseline_context(
        repo,
        environ={},
        expected_baseline=identities["main_tip"],
    )
    assert resolution.baseline_commit_sha == identities["main_tip"]
    assert resolution.context.base_source == "frozen_baseline_fallback"
    assert resolution.context.event == "pull_request"

    with pytest.raises(ValueError, match="refused inside GitHub Actions"):
        differential_gate.resolve_baseline_context(
            repo,
            environ={"GITHUB_ACTIONS": "true"},
            expected_baseline=identities["main_tip"],
        )


def test_the_baseline_context_model_rejects_a_malformed_or_mismatched_declaration() -> None:
    with pytest.raises(ValueError, match="two distinct commits"):
        differential_gate.R07BaselineContextV1(
            event="push",
            base_sha="a" * 40,
            candidate_sha="a" * 40,
            base_source="explicit_cli",
        )
    with pytest.raises(ValueError, match="only a push context"):
        differential_gate.R07BaselineContextV1(
            event="pull_request",
            base_sha="a" * 40,
            candidate_sha="b" * 40,
            base_source="explicit_cli",
            event_before_sha="c" * 40,
        )
    with pytest.raises(ValueError, match="null commit"):
        differential_gate.R07BaselineContextV1(
            event="push",
            base_sha="a" * 40,
            candidate_sha="b" * 40,
            base_source="git_first_parent",
            event_before_sha="0" * 40,
        )
    with pytest.raises(ValueError):
        differential_gate.R07BaselineContextV1(
            event="pull_request",
            base_sha="origin/main",
            candidate_sha="b" * 40,
            base_source="explicit_cli",
        )
    with pytest.raises(ValueError):
        differential_gate.R07BaselineContextV1(
            event="pull_request",
            base_sha="a" * 40,
            candidate_sha="b" * 40,
            base_source="whatever_i_want",
        )
    with pytest.raises(TypeError, match="exact R07BaselineContextV1"):
        differential_gate.verify_baseline_context(ROOT, object())  # type: ignore[arg-type]
