"""Produce strict R07 dual-Python CI summaries and aggregate evidence."""

from __future__ import annotations

import argparse
import os
import stat
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr, model_validator

import rquant.signal_family_differential_gate as differential_gate
from rquant.signal_family_differential_gate import (
    BoundaryProbeResultV1,
    CandidateGateResult,
    PythonRunEvidenceV1,
    R07PolicyV1,
    R07StaticGateResult,
    VerifiedR07DrGateEvidenceV1,
    boundary_probe_results_digest,
    canonical_evidence_json_bytes,
    load_policy,
    python_run_result_digest,
    verify_candidate_gate,
    verify_r07_static_gate,
)
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads

_SHA1 = r"^[0-9a-f]{40}$"
_PYTHON_JOBS = {
    "3.11": "r07-differential-gate-py311",
    "3.12": "r07-differential-gate-py312",
}
_AGGREGATE_JOB = "r07-differential-gate-evidence"
_POLICY_RELATIVE_PATH = Path("tests/fixtures/r07_differential_gate/policy-v1.json")
_GATE_CHECK_COUNT = 20
_MAX_SUMMARY_BYTES = 16 * 1024


@dataclass(frozen=True)
class _GateExecution:
    policy: R07PolicyV1
    candidate_gate: CandidateGateResult
    boundary_results: tuple[BoundaryProbeResultV1, ...]
    static_result: R07StaticGateResult
    collected: int = _GATE_CHECK_COUNT
    passed: int = _GATE_CHECK_COUNT
    skipped: int = 0
    deselected: int = 0


class GitHubRunContextV1(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        str_strip_whitespace=False,
    )

    repository: StrictStr
    workflow_path: StrictStr
    event_name: StrictStr
    ref: StrictStr
    event_after_sha: StrictStr = Field(pattern=_SHA1)
    checkout_sha: StrictStr = Field(pattern=_SHA1)
    workflow_run_id: StrictInt = Field(gt=0)
    run_attempt: StrictInt = Field(gt=0)
    job_id: StrictStr
    job_run_id: StrictInt = Field(gt=0)

    @model_validator(mode="after")
    def validate_fixed_channel(self) -> Self:
        if self.repository != "roxorlt/rquant":
            raise ValueError("R07 evidence repository must be roxorlt/rquant")
        if self.workflow_path != ".github/workflows/ci.yml":
            raise ValueError("R07 evidence workflow path is fixed")
        return self


def _require_push_main_context(
    context: GitHubRunContextV1,
    *,
    expected_job_id: str,
    observed_commit_sha: str,
) -> None:
    if type(context) is not GitHubRunContextV1:
        raise TypeError("exact GitHubRunContextV1 is required")
    revalidated = GitHubRunContextV1.model_validate(context.model_dump(mode="python"))
    if revalidated != context:
        raise ValueError("R07 GitHub context is not strictly self-consistent")
    if context.event_name != "push":
        raise ValueError("R07 deployable evidence requires a push event")
    if context.ref != "refs/heads/main":
        raise ValueError("R07 deployable evidence requires refs/heads/main")
    if context.event_after_sha != observed_commit_sha:
        raise ValueError("GitHub event after SHA does not match observed HEAD")
    if context.checkout_sha != observed_commit_sha:
        raise ValueError("GitHub checkout SHA does not match observed HEAD")
    if context.job_id != expected_job_id:
        raise ValueError("R07 evidence job ID does not match the fixed producer")


def canonical_python_run_summary_bytes(summary: PythonRunEvidenceV1) -> bytes:
    if type(summary) is not PythonRunEvidenceV1:
        raise TypeError("exact PythonRunEvidenceV1 is required")
    if summary.result_digest != python_run_result_digest(summary):
        raise ValueError("R07 Python run result digest mismatch")
    raw = canonical_json_bytes(summary.model_dump(mode="json"))
    decoded = strict_canonical_json_loads(raw)
    if PythonRunEvidenceV1.model_validate(decoded) != summary:
        raise ValueError("R07 Python run summary does not round-trip")
    return raw


def _atomic_write_private(path: Path, payload: bytes) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path.parent, 0o700)
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        pass
    else:
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("R07 evidence output target must be a regular file")
        raise ValueError("R07 evidence output target already exists")

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb", closefd=True) as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def emit_python_run_summary(
    path: Path,
    *,
    context: GitHubRunContextV1,
    python_minor: str,
    observed_commit_sha: str,
    observed_tree_sha: str,
    collected: int,
    passed: int,
    skipped: int,
    deselected: int,
) -> PythonRunEvidenceV1:
    try:
        expected_job = _PYTHON_JOBS[python_minor]
    except KeyError as exc:
        raise ValueError("R07 Python minor must be exactly 3.11 or 3.12") from exc
    _require_push_main_context(
        context,
        expected_job_id=expected_job,
        observed_commit_sha=observed_commit_sha,
    )
    values = {
        "python_minor": python_minor,
        "job_id": context.job_id,
        "job_run_id": context.job_run_id,
        "workflow_run_id": context.workflow_run_id,
        "run_attempt": context.run_attempt,
        "candidate_commit_sha": observed_commit_sha,
        "candidate_tree_sha": observed_tree_sha,
        "collected": collected,
        "passed": passed,
        "skipped": skipped,
        "deselected": deselected,
        "result_digest": "0" * 64,
        "outcome": "passed",
    }
    provisional = PythonRunEvidenceV1.model_validate(values)
    summary = provisional.model_copy(
        update={"result_digest": python_run_result_digest(provisional)}
    )
    payload = canonical_python_run_summary_bytes(summary)
    _atomic_write_private(path, payload)
    return summary


def load_python_run_summary(path: Path) -> PythonRunEvidenceV1:
    try:
        summary_path = Path(path)
        metadata = summary_path.lstat()
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > _MAX_SUMMARY_BYTES:
            raise ValueError("R07 Python run summary must be a bounded regular file")
        raw = summary_path.read_bytes()
        decoded = strict_canonical_json_loads(raw)
        summary = PythonRunEvidenceV1.model_validate(decoded)
    except (OSError, TypeError, ValueError) as exc:
        raise ValueError("R07 Python run summary is invalid") from exc
    if canonical_python_run_summary_bytes(summary) != raw:
        raise ValueError("R07 Python run summary is not canonical")
    return summary


def write_verified_evidence_artifact(
    artifact_root: Path,
    evidence: VerifiedR07DrGateEvidenceV1,
) -> Path:
    if type(evidence) is not VerifiedR07DrGateEvidenceV1:
        raise TypeError("R07 artifact writer requires verified evidence")
    root = Path(artifact_root)
    if root.exists() and any(root.iterdir()):
        raise ValueError("R07 artifact root must be empty")
    output = root / "r07-dr-gate" / "evidence-v1.json"
    _atomic_write_private(output, canonical_evidence_json_bytes(evidence))
    return output


def _checkout_identity(repo: Path) -> tuple[Path, str, str]:
    repository = Path(repo).resolve(strict=True)
    commit = subprocess.run(
        ["git", "-C", str(repository), "rev-parse", "--verify", "HEAD^{commit}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    tree = subprocess.run(
        ["git", "-C", str(repository), "rev-parse", "--verify", "HEAD^{tree}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if not differential_gate._is_lower_hex(commit, length=40):
        raise ValueError("checkout HEAD is not a lowercase 40-hex commit")
    if not differential_gate._is_lower_hex(tree, length=40):
        raise ValueError("checkout tree is not a lowercase 40-hex tree")
    return repository, commit, tree


def _execute_exact_gate(
    repo: Path,
    *,
    candidate_commit: str,
    candidate_tree: str,
) -> _GateExecution:
    with differential_gate._materialize_candidate_tree(repo, candidate_commit) as candidate_root:
        try:
            run_boundary_probe_subprocess = differential_gate._load_candidate_probe_runner(
                candidate_root
            )
        except (ImportError, OSError, SyntaxError, ValueError) as exc:
            raise ValueError("R07 candidate probe runner facade is unavailable") from exc
        policy_path = candidate_root / _POLICY_RELATIVE_PATH
        policy = load_policy(policy_path)
        candidate_gate = verify_candidate_gate(
            repo,
            policy=policy,
            candidate_commit=candidate_commit,
            candidate_tree=candidate_tree,
        )
        if not candidate_gate.passed:
            raise ValueError("R07 candidate diff gate did not pass")
        static_result = verify_r07_static_gate(
            repo,
            policy=policy,
            candidate_commit=candidate_commit,
            candidate_tree=candidate_tree,
        )
        if not static_result.passed:
            raise ValueError("R07 B18/B19 static gate did not pass")
        with TemporaryDirectory(prefix="rquant-r07-ci-probes-") as directory:
            probe_root = Path(directory).resolve(strict=True)
            boundary_results = tuple(
                BoundaryProbeResultV1.model_validate(
                    run_boundary_probe_subprocess(
                        policy_path=policy_path,
                        candidate_root=candidate_root,
                        inventory_id=f"R07-B{index:02d}",
                        tmp_path=probe_root / f"b{index:02d}",
                    )
                )
                for index in range(1, 18)
            )
        boundary_probe_results_digest(policy, boundary_results)
    return _GateExecution(
        policy=policy,
        candidate_gate=candidate_gate,
        boundary_results=boundary_results,
        static_result=static_result,
    )


def validate_python_run_pair(
    context: GitHubRunContextV1,
    runs: tuple[PythonRunEvidenceV1, PythonRunEvidenceV1],
    *,
    observed_commit_sha: str,
    observed_tree_sha: str,
) -> tuple[PythonRunEvidenceV1, PythonRunEvidenceV1]:
    _require_push_main_context(
        context,
        expected_job_id=_AGGREGATE_JOB,
        observed_commit_sha=observed_commit_sha,
    )
    if type(runs) is not tuple or len(runs) != 2:
        raise ValueError("R07 aggregate requires exactly two Python summaries")
    expected = tuple(_PYTHON_JOBS.items())
    validated: list[PythonRunEvidenceV1] = []
    for run, (minor, job_id) in zip(runs, expected, strict=True):
        if type(run) is not PythonRunEvidenceV1:
            raise ValueError("R07 aggregate requires exact PythonRunEvidenceV1 values")
        parsed = PythonRunEvidenceV1.model_validate(run.model_dump(mode="python"))
        if parsed != run or parsed.result_digest != python_run_result_digest(parsed):
            raise ValueError("R07 Python summary is not strictly self-consistent")
        if (parsed.python_minor, parsed.job_id) != (minor, job_id):
            raise ValueError("R07 Python summaries are not the ordered fixed job pair")
        if (parsed.workflow_run_id, parsed.run_attempt) != (
            context.workflow_run_id,
            context.run_attempt,
        ):
            raise ValueError("R07 Python summary workflow run binding mismatch")
        if (parsed.candidate_commit_sha, parsed.candidate_tree_sha) != (
            observed_commit_sha,
            observed_tree_sha,
        ):
            raise ValueError("R07 Python summary candidate binding mismatch")
        validated.append(parsed)
    if validated[0].job_run_id == validated[1].job_run_id:
        raise ValueError("R07 Python summary job run IDs must be distinct")
    return validated[0], validated[1]


def aggregate_evidence_from_summaries(
    repo: Path,
    *,
    context: GitHubRunContextV1,
    py311_summary_path: Path,
    py312_summary_path: Path,
    artifact_root: Path,
) -> Path:
    first_path = Path(py311_summary_path).resolve(strict=False)
    second_path = Path(py312_summary_path).resolve(strict=False)
    if first_path == second_path:
        raise ValueError("R07 Python summary input paths must be distinct")
    repository, commit, tree = _checkout_identity(repo)
    _require_push_main_context(
        context,
        expected_job_id=_AGGREGATE_JOB,
        observed_commit_sha=commit,
    )
    runs = validate_python_run_pair(
        context,
        (
            load_python_run_summary(first_path),
            load_python_run_summary(second_path),
        ),
        observed_commit_sha=commit,
        observed_tree_sha=tree,
    )
    execution = _execute_exact_gate(
        repository,
        candidate_commit=commit,
        candidate_tree=tree,
    )
    evidence = VerifiedR07DrGateEvidenceV1.from_gate_results(
        repo=repository,
        policy=execution.policy,
        candidate_gate=execution.candidate_gate,
        boundary_results=execution.boundary_results,
        static_result=execution.static_result,
        python_runs=runs,
        workflow_run_id=context.workflow_run_id,
        run_attempt=context.run_attempt,
    )
    return write_verified_evidence_artifact(artifact_root, evidence)


def _context_from_args(args: argparse.Namespace) -> GitHubRunContextV1:
    return GitHubRunContextV1(
        repository=args.repository,
        workflow_path=args.workflow_path,
        event_name=args.event_name,
        ref=args.ref,
        event_after_sha=args.event_after_sha,
        checkout_sha=args.checkout_sha,
        workflow_run_id=args.workflow_run_id,
        run_attempt=args.run_attempt,
        job_id=args.job_id,
        job_run_id=args.job_run_id,
    )


def _add_context_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--repository", required=True)
    parser.add_argument("--workflow-path", required=True)
    parser.add_argument("--event-name", required=True)
    parser.add_argument("--ref", required=True)
    parser.add_argument("--event-after-sha", required=True)
    parser.add_argument("--checkout-sha", required=True)
    parser.add_argument("--workflow-run-id", required=True, type=int)
    parser.add_argument("--run-attempt", required=True, type=int)
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--job-run-id", required=True, type=int)


def _parse_args(arguments: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)

    validate = commands.add_parser("validate")
    validate.add_argument("--repo", required=True, type=Path)
    validate.add_argument("--candidate-commit", required=True)
    validate.add_argument("--candidate-tree", required=True)

    run = commands.add_parser("run-python")
    run.add_argument("--repo", required=True, type=Path)
    run.add_argument("--python-minor", required=True)
    run.add_argument("--output", required=True, type=Path)
    _add_context_arguments(run)

    aggregate = commands.add_parser("aggregate")
    aggregate.add_argument("--repo", required=True, type=Path)
    aggregate.add_argument("--py311-summary", required=True, type=Path)
    aggregate.add_argument("--py312-summary", required=True, type=Path)
    aggregate.add_argument("--artifact-root", required=True, type=Path)
    _add_context_arguments(aggregate)
    return parser.parse_args(arguments)


def main(arguments: list[str] | None = None) -> int:
    args = _parse_args(arguments)
    repository, commit, tree = _checkout_identity(args.repo)
    if args.command == "validate":
        if (args.candidate_commit, args.candidate_tree) != (commit, tree):
            raise ValueError("R07 validation candidate does not match checkout HEAD/tree")
        _execute_exact_gate(repository, candidate_commit=commit, candidate_tree=tree)
        return 0
    context = _context_from_args(args)
    if args.command == "run-python":
        actual_minor = f"{sys.version_info.major}.{sys.version_info.minor}"
        if args.python_minor != actual_minor:
            raise ValueError("R07 Python job runtime minor does not match its static job")
        _require_push_main_context(
            context,
            expected_job_id=_PYTHON_JOBS.get(args.python_minor, ""),
            observed_commit_sha=commit,
        )
        execution = _execute_exact_gate(
            repository,
            candidate_commit=commit,
            candidate_tree=tree,
        )
        emit_python_run_summary(
            args.output,
            context=context,
            python_minor=args.python_minor,
            observed_commit_sha=commit,
            observed_tree_sha=tree,
            collected=execution.collected,
            passed=execution.passed,
            skipped=execution.skipped,
            deselected=execution.deselected,
        )
        return 0
    aggregate_evidence_from_summaries(
        repository,
        context=context,
        py311_summary_path=args.py311_summary,
        py312_summary_path=args.py312_summary,
        artifact_root=args.artifact_root,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
