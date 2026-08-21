"""R07 dual-Python CI evidence producer and workflow contracts."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

from rquant.signal_family_differential_gate import R07_CI_EVIDENCE_PRODUCER_IMPLEMENTED
from scripts import r07_ci_evidence as ci_evidence
from tests.unit.test_signal_family_differential_gate import (
    _EvidenceBundle,
)
from tests.unit.test_signal_family_differential_gate import (
    evidence_bundle as _evidence_bundle_fixture,
)

evidence_bundle = _evidence_bundle_fixture


ROOT = Path(__file__).parents[2]
PRODUCER_PATH = ROOT / "scripts" / "r07_ci_evidence.py"
COMMIT_SHA = "a" * 40
TREE_SHA = "b" * 40
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
        event_after_sha=COMMIT_SHA,
        checkout_sha=COMMIT_SHA,
        workflow_run_id=100,
        run_attempt=2,
        job_id=job_id,
        job_run_id=101,
    )


def _emit_summary(
    path: Path,
    *,
    minor: str = "3.11",
    context: ci_evidence.GitHubRunContextV1 | None = None,
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
        lambda value: value | {"extra": "blocked"},
        lambda value: {key: item for key, item in value.items() if key != "run_attempt"},
        lambda value: value | {"collected": True},
        lambda value: value | {"passed": 19},
        lambda value: value | {"skipped": 1},
        lambda value: value | {"deselected": 1},
        lambda value: value | {"result_digest": "0" * 64},
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
