"""R07 dual-Python CI evidence producer and workflow contracts."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

import rquant.signal_family_differential_gate as differential_gate
from rquant.signal_family_differential_gate import R07_CI_EVIDENCE_PRODUCER_IMPLEMENTED
from scripts import r07_ci_evidence as ci_evidence
from tests.unit.test_signal_family_differential_gate import (
    _EvidenceBundle,
    _newest_non_merge_commit,
    _shared_clone,
    _synthetic_merge_candidate,
)
from tests.unit.test_signal_family_differential_gate import (
    evidence_bundle as _evidence_bundle_fixture,
)

evidence_bundle = _evidence_bundle_fixture


ROOT = Path(__file__).parents[2]
PRODUCER_PATH = ROOT / "scripts" / "r07_ci_evidence.py"
COMMIT_SHA = "a" * 40
TREE_SHA = "b" * 40
BEFORE_SHA = "e" * 40
WORKFLOW_PATH = ROOT / ".github" / "workflows" / "ci.yml"


def _context(
    *,
    job_id: str = "r07-differential-gate-py311",
) -> ci_evidence.GitHubRunContextV1:
    return ci_evidence.GitHubRunContextV1(
        repository="roxorlt/rquant",
        workflow_path=".github/workflows/ci.yml",
        event_name="push",
        ref="refs/heads/main",
        event_before_sha=BEFORE_SHA,
        event_after_sha=COMMIT_SHA,
        checkout_sha=COMMIT_SHA,
        workflow_run_id=100,
        run_attempt=2,
        job_id=job_id,
        job_run_id=101,
    )


GATE_DIGESTS = ci_evidence.PythonRunGateDigests(
    candidate_gate_digest="1" * 64,
    static_result_digest="2" * 64,
    boundary_result_digest="3" * 64,
    check_inventory_digest="4" * 64,
)


def _emit_summary(
    path: Path,
    *,
    minor: str = "3.11",
    context: ci_evidence.GitHubRunContextV1 | None = None,
    gate_digests: ci_evidence.PythonRunGateDigests = GATE_DIGESTS,
) -> None:
    ci_evidence.emit_python_run_summary(
        path,
        context=context or _context(),
        python_minor=minor,
        observed_commit_sha=COMMIT_SHA,
        observed_tree_sha=TREE_SHA,
        collected=20,
        passed=20,
        skipped=0,
        deselected=0,
        gate_digests=gate_digests,
    )


def _workflow() -> dict[str, object]:
    program = (
        "payload = YAML.safe_load(File.read(ARGV.fetch(0)), aliases: true); "
        "STDOUT.write(JSON.generate(payload))"
    )
    completed = subprocess.run(
        ["ruby", "-ryaml", "-rjson", "-e", program, str(WORKFLOW_PATH)],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(completed.stdout)
    assert type(payload) is dict
    return payload


def _actual_identity() -> tuple[str, str]:
    commit = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    tree = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "HEAD^{tree}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return commit, tree


def test_r07_ci_evidence_producer_is_present_and_complete() -> None:
    assert PRODUCER_PATH.is_file()
    assert R07_CI_EVIDENCE_PRODUCER_IMPLEMENTED is True


def test_direct_script_runtime_loads_candidate_tree_probe_facade(tmp_path: Path) -> None:
    site_packages = (
        Path(sys.prefix)
        / "lib"
        / f"python{sys.version_info.major}.{sys.version_info.minor}"
        / "site-packages"
    ).resolve(strict=True)
    commit, _tree = _actual_identity()
    temporary_root = tmp_path / "tmp"
    temporary_root.mkdir()
    bootstrap = f"""
import importlib.util
import sys
from pathlib import Path

producer = Path({str(PRODUCER_PATH)!r})
spec = importlib.util.spec_from_file_location("r07_ci_direct", producer)
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)

gate = module.differential_gate
with gate._materialize_candidate_tree(Path({str(ROOT)!r}), {commit!r}) as candidate_root:
    runner = gate._load_candidate_probe_runner(candidate_root)
    loaded = Path(runner.__code__.co_filename).resolve()
    expected = (candidate_root / "tests" / "r07_differential_probe_runner.py").resolve()
    assert loaded == expected, (loaded, expected)
    assert loaded.read_bytes() == (
        Path({str(ROOT)!r}) / "tests" / "r07_differential_probe_runner.py"
    ).read_bytes()

assert "tests" not in sys.modules
assert "tests.r07_differential_probe_runner" not in sys.modules
assert {str(ROOT)!r} not in sys.path
print("candidate-tree-facade")
"""
    completed = subprocess.run(
        [sys.executable, "-c", bootstrap],
        cwd=tmp_path,
        env={
            "LANG": "C",
            "LC_ALL": "C",
            "TMPDIR": str(temporary_root),
            "TMP": str(temporary_root),
            "TEMP": str(temporary_root),
            "PYTHONPATH": os.pathsep.join((str(ROOT / "src"), str(site_packages))),
            "PYTHONNOUSERSITE": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
            "RQUANT_DISABLE_DOTENV": "1",
            "TUSHARE_TOKEN_MAIN": "0" * 32,
        },
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert completed.stdout.strip() == "candidate-tree-facade"


@pytest.mark.parametrize(
    ("minor", "job_id"),
    (
        ("3.11", "r07-differential-gate-py311"),
        ("3.12", "r07-differential-gate-py312"),
    ),
)
def test_python_jobs_emit_strict_canonical_bound_summaries(
    tmp_path: Path,
    minor: str,
    job_id: str,
) -> None:
    path = tmp_path / f"py{minor.replace('.', '')}.json"
    context = _context(job_id=job_id).model_copy(
        update={"job_run_id": 311 if minor == "3.11" else 312}
    )

    _emit_summary(path, minor=minor, context=context)

    summary = ci_evidence.load_python_run_summary(path)
    assert summary.python_minor == minor
    assert summary.job_id == job_id
    assert summary.job_run_id == context.job_run_id
    assert summary.workflow_run_id == context.workflow_run_id
    assert summary.run_attempt == context.run_attempt
    assert (summary.candidate_commit_sha, summary.candidate_tree_sha) == (
        COMMIT_SHA,
        TREE_SHA,
    )
    assert summary.collected == summary.passed == 20
    assert summary.skipped == summary.deselected == 0
    assert summary.outcome == "passed"
    assert path.read_bytes() == ci_evidence.canonical_python_run_summary_bytes(summary)
    assert not path.with_suffix(".json.tmp").exists()


@pytest.mark.parametrize(
    ("updates", "match"),
    (
        ({"event_name": "pull_request"}, "push"),
        ({"event_name": "workflow_dispatch"}, "push"),
        ({"ref": "refs/heads/feature"}, "main"),
        ({"ref": "refs/tags/v1.0.0"}, "main"),
        ({"repository": "fork/rquant"}, "repository"),
        ({"workflow_path": ".github/workflows/other.yml"}, "workflow"),
        ({"event_after_sha": "c" * 40}, "after"),
        ({"event_before_sha": "0" * 40}, "null commit"),
        ({"event_before_sha": COMMIT_SHA}, "nothing was pushed"),
        ({"checkout_sha": "d" * 40}, "checkout"),
        ({"job_id": "matrix-job"}, "job"),
    ),
)
def test_python_summary_event_ref_sha_and_job_gating_fails_without_output(
    tmp_path: Path,
    updates: dict[str, object],
    match: str,
) -> None:
    output = tmp_path / "summary.json"
    context = _context().model_copy(update=updates)

    with pytest.raises(ValueError, match=match):
        _emit_summary(output, context=context)

    assert not output.exists()


@pytest.mark.parametrize(
    "mutation",
    (
        pytest.param(lambda value: value | {"extra": "blocked"}, id="extra-field"),
        pytest.param(
            lambda value: {key: item for key, item in value.items() if key != "run_attempt"},
            id="missing-run-attempt",
        ),
        pytest.param(lambda value: value | {"collected": True}, id="bool-coerced-collected"),
        pytest.param(lambda value: value | {"passed": 19}, id="passed-below-collected"),
        pytest.param(lambda value: value | {"skipped": 1}, id="nonzero-skipped"),
        pytest.param(lambda value: value | {"deselected": 1}, id="nonzero-deselected"),
        pytest.param(
            lambda value: value | {"result_digest": "0" * 64},
            id="tampered-result-digest",
        ),
    ),
)
def test_python_summary_parser_rejects_missing_extra_coerced_or_tampered_fields(
    tmp_path: Path,
    mutation,
) -> None:
    path = tmp_path / "summary.json"
    _emit_summary(path)
    payload = json.loads(path.read_bytes())
    path.write_text(
        json.dumps(mutation(payload), sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )

    with pytest.raises(ValueError):
        ci_evidence.load_python_run_summary(path)


def test_python_summary_parser_rejects_duplicate_fields(tmp_path: Path) -> None:
    path = tmp_path / "summary.json"
    _emit_summary(path)
    raw = path.read_text(encoding="utf-8")
    path.write_text(
        raw.replace(
            '"candidate_commit_sha":',
            f'"candidate_commit_sha":"{COMMIT_SHA}","candidate_commit_sha":',
            1,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError):
        ci_evidence.load_python_run_summary(path)


def test_verified_aggregate_writes_only_fixed_artifact_layout(
    tmp_path: Path,
    evidence_bundle: _EvidenceBundle,
) -> None:
    artifact_root = tmp_path / "artifact-root"

    output = ci_evidence.write_verified_evidence_artifact(
        artifact_root,
        evidence_bundle.evidence,
    )

    assert output == artifact_root / "r07-dr-gate" / "evidence-v1.json"
    assert output.read_bytes() == ci_evidence.canonical_evidence_json_bytes(
        evidence_bundle.evidence
    )
    assert tuple(
        path.relative_to(artifact_root).as_posix()
        for path in sorted(artifact_root.rglob("*"))
        if path.is_file()
    ) == ("r07-dr-gate/evidence-v1.json",)


@pytest.mark.parametrize(
    "updates",
    (
        {"event_name": "pull_request"},
        {"event_name": "workflow_dispatch"},
        {"ref": "refs/heads/topic"},
        {"ref": "refs/tags/v0.29.0"},
    ),
)
def test_aggregate_rejects_non_push_main_before_reading_or_writing(
    tmp_path: Path,
    updates: dict[str, object],
) -> None:
    commit, _tree = _actual_identity()
    context = _context(job_id="r07-differential-gate-evidence").model_copy(
        update={
            "event_after_sha": commit,
            "checkout_sha": commit,
            "job_run_id": 103,
            **updates,
        }
    )
    artifact_root = tmp_path / "artifact-root"

    with pytest.raises(ValueError):
        ci_evidence.aggregate_evidence_from_summaries(
            ROOT,
            context=context,
            py311_summary_path=tmp_path / "missing-311.json",
            py312_summary_path=tmp_path / "missing-312.json",
            artifact_root=artifact_root,
        )

    assert not artifact_root.exists()


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("workflow_run_id", 999),
        ("run_attempt", 3),
        ("candidate_commit_sha", "c" * 40),
        ("candidate_tree_sha", "d" * 40),
        ("job_id", "r07-differential-gate-py311"),
        ("python_minor", "3.11"),
    ),
)
def test_aggregate_rejects_job_run_candidate_and_order_cross_binding(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    py311_path = tmp_path / "py311.json"
    py312_path = tmp_path / "py312.json"
    _emit_summary(py311_path)
    _emit_summary(
        py312_path,
        minor="3.12",
        context=_context(job_id="r07-differential-gate-py312").model_copy(
            update={"job_run_id": 102}
        ),
    )
    py311 = ci_evidence.load_python_run_summary(py311_path)
    py312 = ci_evidence.load_python_run_summary(py312_path)
    mutated = py312.model_copy(update={field: value})
    mutated = mutated.model_copy(
        update={"result_digest": ci_evidence.python_run_result_digest(mutated)}
    )
    context = _context(job_id="r07-differential-gate-evidence").model_copy(
        update={"job_run_id": 103}
    )

    with pytest.raises(ValueError):
        ci_evidence.validate_python_run_pair(
            context,
            (py311, mutated),
            observed_commit_sha=COMMIT_SHA,
            observed_tree_sha=TREE_SHA,
        )


def test_aggregate_rejects_duplicate_summary_input_path(tmp_path: Path) -> None:
    summary_path = tmp_path / "summary.json"
    context = _context(job_id="r07-differential-gate-evidence").model_copy(
        update={"job_run_id": 103}
    )

    with pytest.raises(ValueError, match="distinct"):
        ci_evidence.aggregate_evidence_from_summaries(
            ROOT,
            context=context,
            py311_summary_path=summary_path,
            py312_summary_path=summary_path,
            artifact_root=tmp_path / "artifact-root",
        )


def test_aggregate_rejects_dual_python_gate_digest_divergence(tmp_path: Path) -> None:
    py311_path = tmp_path / "py311.json"
    py312_path = tmp_path / "py312.json"
    _emit_summary(py311_path)
    _emit_summary(
        py312_path,
        minor="3.12",
        context=_context(job_id="r07-differential-gate-py312").model_copy(
            update={"job_run_id": 102}
        ),
        gate_digests=ci_evidence.PythonRunGateDigests(
            candidate_gate_digest=GATE_DIGESTS.candidate_gate_digest,
            static_result_digest=GATE_DIGESTS.static_result_digest,
            boundary_result_digest="9" * 64,
            check_inventory_digest=GATE_DIGESTS.check_inventory_digest,
        ),
    )
    context = _context(job_id="r07-differential-gate-evidence").model_copy(
        update={"job_run_id": 103}
    )

    with pytest.raises(ValueError, match="diverge"):
        ci_evidence.validate_python_run_pair(
            context,
            (
                ci_evidence.load_python_run_summary(py311_path),
                ci_evidence.load_python_run_summary(py312_path),
            ),
            observed_commit_sha=COMMIT_SHA,
            observed_tree_sha=TREE_SHA,
        )


def test_push_main_without_a_merge_commit_produces_no_summary(tmp_path: Path) -> None:
    """Ruling 9 stand-in enforcement: a squashed or direct push to main yields no evidence.

    The checkout tip is a merge commit once a work package has been merged back, so the
    single-parent shape a squash or a direct push leaves behind is materialized on purpose:
    a throwaway ``--shared`` clone whose detached HEAD is the newest non-merge commit.
    """

    repo = _shared_clone(tmp_path / "direct-push")
    commit = _newest_non_merge_commit()
    subprocess.run(
        ["git", "-C", str(repo), "update-ref", "--no-deref", "HEAD", commit],
        check=True,
        capture_output=True,
    )
    output = tmp_path / "summary.json"
    minor = f"{sys.version_info.major}.{sys.version_info.minor}"
    arguments = [
        "run-python",
        "--repo",
        str(repo),
        "--python-minor",
        minor,
        "--output",
        str(output),
        "--repository",
        "roxorlt/rquant",
        "--workflow-path",
        ".github/workflows/ci.yml",
        "--event-name",
        "push",
        "--ref",
        "refs/heads/main",
        "--event-before-sha",
        differential_gate.BASELINE_COMMIT_SHA,
        "--event-after-sha",
        commit,
        "--checkout-sha",
        commit,
        "--workflow-run-id",
        "100",
        "--run-attempt",
        "1",
        "--job-id",
        f"r07-differential-gate-py{minor.replace('.', '')}",
        "--job-run-id",
        "101",
    ]

    with pytest.raises(ValueError, match="two-parent merge commit"):
        ci_evidence.main(arguments)

    assert not output.exists()


def test_workflow_has_static_dual_python_jobs_and_strict_aggregate_contract() -> None:
    workflow = _workflow()
    trigger = workflow.get("on", workflow.get("true"))
    assert trigger == {"pull_request": None, "push": {"branches": ["main"]}}
    jobs = workflow["jobs"]
    assert type(jobs) is dict
    expected = (
        "r07-differential-gate-py311",
        "r07-differential-gate-py312",
        "r07-differential-gate-evidence",
    )
    assert all(job_id in jobs for job_id in expected)
    py311 = jobs[expected[0]]
    py312 = jobs[expected[1]]
    aggregate = jobs[expected[2]]
    assert "matrix" not in py311.get("strategy", {})
    assert "matrix" not in py312.get("strategy", {})
    assert "matrix" not in aggregate.get("strategy", {})
    assert "strategy" not in aggregate
    assert aggregate["needs"] == [expected[0], expected[1]]
    assert "github.event_name == 'push'" in aggregate["if"]
    assert "github.ref == 'refs/heads/main'" in aggregate["if"]

    upload = next(
        step
        for step in aggregate["steps"]
        if str(step.get("uses", "")).startswith("actions/upload-artifact@")
    )
    assert upload["with"] == {
        "name": "r07-dr-gate-${{ github.event.after }}",
        "path": "r07-evidence-artifact-root/",
        "if-no-files-found": "error",
        "retention-days": 90,
    }


def test_workflow_pins_every_action_to_a_full_commit_sha() -> None:
    jobs = _workflow()["jobs"]
    uses = [
        step["uses"] for job in jobs.values() for step in job.get("steps", ()) if "uses" in step
    ]
    assert uses
    assert all(re.fullmatch(r"[^@]+@[0-9a-f]{40}", value) for value in uses)


@pytest.fixture(scope="module")
def gate_execution() -> ci_evidence._GateExecution:
    commit, tree = _actual_identity()
    return ci_evidence._execute_exact_gate(ROOT, candidate_commit=commit, candidate_tree=tree)


def test_gate_execution_counts_derive_from_the_executed_check_inventory() -> None:
    assert not hasattr(ci_evidence, "_GATE_CHECK_COUNT")
    execution = ci_evidence._GateExecution(
        policy=None,
        candidate_gate=None,
        boundary_results=(),
        static_result=None,
        executed_checks=("policy-load", "candidate-diff-gate", "static:policy-completeness"),
    )

    assert execution.collected == 3
    assert execution.passed == 3
    assert execution.skipped == 0
    assert execution.deselected == 0


def test_exact_gate_reports_every_executed_check_instead_of_a_constant(
    gate_execution: ci_evidence._GateExecution,
) -> None:
    expected = (
        ("policy-load:tests/fixtures/r07_differential_gate/policy-v1.json",)
        + ("candidate-diff-gate",)
        + tuple(f"static:{name}" for name, _result in gate_execution.static_result.checks)
        + tuple(
            f"boundary-probe:{result.inventory_id}" for result in gate_execution.boundary_results
        )
        + ("boundary-probe-results-digest",)
    )

    assert gate_execution.executed_checks == expected
    assert len(set(expected)) == len(expected)
    assert gate_execution.collected == gate_execution.passed == len(expected)
    assert gate_execution.skipped == gate_execution.deselected == 0
    assert gate_execution.collected == ci_evidence.expected_gate_check_total(
        gate_execution.policy
    )
    assert gate_execution.collected != 20
    assert len(gate_execution.static_result.checks) == len(
        differential_gate.FIXED_STATIC_CHECK_NAMES
    ) + len(
        gate_execution.policy.root_snapshots
    ) + len(gate_execution.policy.production_declarations) + len(
        gate_execution.policy.boundary_probes
    )
    assert len(gate_execution.boundary_results) == ci_evidence.BOUNDARY_PROBE_COUNT


def test_workflow_never_cancels_push_to_main_runs() -> None:
    concurrency = _workflow()["concurrency"]

    assert type(concurrency) is dict
    assert concurrency["group"] == "ci-${{ github.workflow }}-${{ github.ref }}"
    assert concurrency["cancel-in-progress"] == "${{ github.event_name == 'pull_request' }}"


def test_workflow_guards_every_summary_step_on_push_to_main_only() -> None:
    jobs = _workflow()["jobs"]
    push_main = "github.event_name == 'push' && github.ref == 'refs/heads/main'"
    guarded: list[str] = []
    for job_id in ("r07-differential-gate-py311", "r07-differential-gate-py312"):
        for step in jobs[job_id]["steps"]:
            name = str(step.get("name", ""))
            if name.startswith(("Produce bound Python", "Upload Python")):
                assert step["if"] == push_main, (job_id, name, step.get("if"))
                guarded.append(f"{job_id}:{name}")
            elif name.startswith("Validate pull request"):
                assert step["if"] == "github.event_name == 'pull_request'"
            else:
                assert "if" not in step, (job_id, name)

    assert guarded == [
        "r07-differential-gate-py311:Produce bound Python 3.11 summary",
        "r07-differential-gate-py311:Upload Python 3.11 summary",
        "r07-differential-gate-py312:Produce bound Python 3.12 summary",
        "r07-differential-gate-py312:Upload Python 3.12 summary",
    ]


def test_workflow_scopes_summary_artifacts_to_one_run_attempt() -> None:
    jobs = _workflow()["jobs"]
    scope = "${{ github.run_id }}-${{ github.run_attempt }}"
    uploaded = {
        "r07-differential-gate-py311": f"r07-dr-gate-summary-py311-{scope}",
        "r07-differential-gate-py312": f"r07-dr-gate-summary-py312-{scope}",
    }
    for job_id, expected_name in uploaded.items():
        upload = next(
            step
            for step in jobs[job_id]["steps"]
            if str(step.get("uses", "")).startswith("actions/upload-artifact@")
        )
        assert upload["with"]["name"] == expected_name
        assert upload["with"]["retention-days"] == 1

    downloads = [
        step["with"]["name"]
        for step in jobs["r07-differential-gate-evidence"]["steps"]
        if str(step.get("uses", "")).startswith("actions/download-artifact@")
    ]
    assert downloads == list(uploaded.values())


def test_artifact_writer_rejects_a_bare_observation_wire(
    tmp_path: Path,
    evidence_bundle: _EvidenceBundle,
) -> None:
    artifact_root = tmp_path / "artifact-root"

    with pytest.raises(TypeError):
        ci_evidence.write_verified_evidence_artifact(
            artifact_root,
            evidence_bundle.evidence.wire,
        )

    assert not artifact_root.exists()


def test_the_workflow_states_both_pull_request_endpoints_and_the_push_interval_start() -> None:
    """These expressions can only be exercised by pushing the workflow, so pin them here.

    ``github.sha`` on a pull request is the merge ref GitHub synthesizes, whose first parent
    is the base tip; using it as the candidate would make the merge-base claim true for every
    base and prove nothing. The pull request head has to be named explicitly, and so does the
    push interval start, because neither is available under any other name at run time.
    """

    jobs = _workflow()["jobs"]
    for job_id in ("r07-differential-gate-py311", "r07-differential-gate-py312"):
        steps = jobs[job_id]["steps"]
        validate = next(
            step
            for step in steps
            if str(step.get("name", "")).startswith("Validate pull request")
        )
        assert "--event 'pull_request'" in validate["run"]
        assert "--base-sha '${{ github.event.pull_request.base.sha }}'" in validate["run"]
        assert "--candidate-sha '${{ github.event.pull_request.head.sha }}'" in validate["run"]
        assert "--candidate-sha \"${GITHUB_SHA}\"" not in validate["run"]

        produce = next(
            step for step in steps if str(step.get("name", "")).startswith("Produce bound Python")
        )
        assert "--event-before-sha '${{ github.event.before }}'" in produce["run"]

    aggregate = jobs["r07-differential-gate-evidence"]
    summarize = next(
        step for step in aggregate["steps"] if str(step.get("name", "")).startswith("Aggregate")
    )
    assert "--event-before-sha '${{ github.event.before }}'" in summarize["run"]

    checkout = next(
        step
        for step in aggregate["steps"]
        if str(step.get("uses", "")).startswith("actions/checkout@")
    )
    # The same ref expression the two Python jobs use, so all three resolve identically.
    assert checkout["with"]["ref"] == (
        "${{ github.event_name == 'push' && github.event.after || github.sha }}"
    )


def test_a_push_whose_before_sha_is_not_the_first_parent_produces_no_summary(
    tmp_path: Path,
) -> None:
    """A force-push or an out-of-order delivery makes GitHub's claim and Git disagree.

    Only the first parent is a property of the commit itself, so it is what the interval
    start is taken from; ``github.event.before`` is cross-checked against it rather than
    trusted, and a disagreement is a refusal rather than a preference for one of them.
    """

    repo = _shared_clone(tmp_path / "before-mismatch")
    candidate = _synthetic_merge_candidate(repo)
    subprocess.run(
        ["git", "-C", str(repo), "update-ref", "--no-deref", "HEAD", candidate],
        check=True,
        capture_output=True,
    )
    output = tmp_path / "summary.json"
    minor = f"{sys.version_info.major}.{sys.version_info.minor}"

    def arguments(before: str) -> list[str]:
        return [
            "run-python",
            "--repo",
            str(repo),
            "--python-minor",
            minor,
            "--output",
            str(output),
            "--repository",
            "roxorlt/rquant",
            "--workflow-path",
            ".github/workflows/ci.yml",
            "--event-name",
            "push",
            "--ref",
            "refs/heads/main",
            "--event-before-sha",
            before,
            "--event-after-sha",
            candidate,
            "--checkout-sha",
            candidate,
            "--workflow-run-id",
            "100",
            "--run-attempt",
            "1",
            "--job-id",
            f"r07-differential-gate-py{minor.replace('.', '')}",
            "--job-run-id",
            "101",
        ]

    with pytest.raises(ValueError, match="before SHA is not the first parent"):
        ci_evidence.main(arguments(differential_gate.HISTORICAL_BASELINE_COMMIT_SHA))
    assert not output.exists()

    with pytest.raises(ValueError, match="null commit"):
        ci_evidence.main(arguments("0" * 40))
    assert not output.exists()


def test_the_pull_request_validation_states_its_base_or_refuses_to_run(tmp_path: Path) -> None:
    """A missing base is a workflow expression that resolved to nothing, not a default.

    There is no ref to read the pull request base back from inside the checkout - that is the
    whole failure this replaces - so the only safe answer to an absent base is to stop.
    """

    commit, tree = _actual_identity()
    base = [
        "validate",
        "--repo",
        str(ROOT),
        "--candidate-commit",
        commit,
        "--candidate-tree",
        tree,
        "--event",
        "pull_request",
    ]

    with pytest.raises(ValueError, match="must state its base SHA"):
        ci_evidence.main([*base, "--base-sha", "", "--candidate-sha", commit])
    with pytest.raises(ValueError, match="lowercase 40-hex"):
        ci_evidence.main([*base, "--base-sha", "origin/main", "--candidate-sha", commit])
    with pytest.raises(SystemExit):
        ci_evidence.main(
            [
                "validate",
                "--repo",
                str(ROOT),
                "--candidate-commit",
                commit,
                "--candidate-tree",
                tree,
            ]
        )
