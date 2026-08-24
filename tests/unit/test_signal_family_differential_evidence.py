"""R07 evidence channel, deployment decision table, downloader, and cache tests."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

import rquant.signal_family_differential_gate as differential_gate
from rquant.signal_family_differential_gate import (
    BASELINE_COMMIT_SHA,
    BASELINE_TREE_SHA,
    BootstrapPredecessorV1,
    EvidenceChannelV1,
    PythonRunEvidenceV1,
    R07DrGateEvidenceWireV1,
    R07PolicyV1,
    expected_gate_check_total,
    load_policy,
)

ROOT = Path(__file__).resolve().parents[2]
POLICY_PATH = ROOT / "tests" / "fixtures" / "r07_differential_gate" / "policy-v1.json"
EXACT_JOBS = (
    "r07-differential-gate-py311",
    "r07-differential-gate-py312",
    "r07-differential-gate-evidence",
)
CACHE_PATH = "/home/lighthouse/rquant/var/r07-dr-evidence"


def _channel_values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "repository": "roxorlt/rquant",
        "workflow_path": ".github/workflows/ci.yml",
        "jobs": EXACT_JOBS,
        "artifact_json_path": "r07-dr-gate/evidence-v1.json",
        "retention_days": 90,
        "cache_path": CACHE_PATH,
        "deployment_mode": "disabled_for_bootstrap",
        "bootstrap_predecessor": None,
    }
    values.update(overrides)
    return values


def _policy() -> R07PolicyV1:
    return load_policy(POLICY_PATH)


def test_evidence_channel_declares_cache_mode_and_predecessor() -> None:
    channel = EvidenceChannelV1.model_validate(_channel_values())

    assert channel.cache_path == CACHE_PATH
    assert channel.deployment_mode == "disabled_for_bootstrap"
    assert channel.bootstrap_predecessor is None
    assert channel.jobs == EXACT_JOBS


def test_evidence_channel_pins_the_exact_server_cache_directory() -> None:
    with pytest.raises(ValidationError):
        EvidenceChannelV1.model_validate(
            _channel_values(cache_path="/home/lighthouse/rquant/data/r07-dr-evidence")
        )


@pytest.mark.parametrize(
    "jobs",
    [
        pytest.param(EXACT_JOBS[:2], id="missing-evidence-job"),
        pytest.param((*EXACT_JOBS, "r07-differential-gate-evidence"), id="extra-job"),
        pytest.param(
            (EXACT_JOBS[1], EXACT_JOBS[0], EXACT_JOBS[2]),
            id="python-jobs-swapped",
        ),
        pytest.param((EXACT_JOBS[0], EXACT_JOBS[0], EXACT_JOBS[2]), id="duplicate-py311"),
        pytest.param((), id="empty"),
    ],
)
def test_evidence_channel_requires_the_exact_ordered_job_triple(
    jobs: tuple[str, ...],
) -> None:
    with pytest.raises(ValidationError):
        EvidenceChannelV1.model_validate(_channel_values(jobs=jobs))


def test_enforced_channel_requires_an_exact_bootstrap_predecessor() -> None:
    with pytest.raises(ValidationError):
        EvidenceChannelV1.model_validate(
            _channel_values(deployment_mode="enforced", bootstrap_predecessor=None)
        )


def test_bootstrap_disabled_channel_forbids_a_bootstrap_predecessor() -> None:
    with pytest.raises(ValidationError):
        EvidenceChannelV1.model_validate(
            _channel_values(
                deployment_mode="disabled_for_bootstrap",
                bootstrap_predecessor=BootstrapPredecessorV1(
                    commit_sha="a" * 40,
                    tree_sha="b" * 40,
                ),
            )
        )


def test_enforced_channel_accepts_the_exact_predecessor_pair() -> None:
    channel = EvidenceChannelV1.model_validate(
        _channel_values(
            deployment_mode="enforced",
            bootstrap_predecessor=BootstrapPredecessorV1(
                commit_sha="a" * 40,
                tree_sha="b" * 40,
            ),
        )
    )

    assert channel.bootstrap_predecessor is not None
    assert channel.bootstrap_predecessor.commit_sha == "a" * 40


@pytest.mark.parametrize(
    "values",
    [
        pytest.param({"commit_sha": "A" * 40, "tree_sha": "b" * 40}, id="uppercase-commit"),
        pytest.param({"commit_sha": "a" * 39, "tree_sha": "b" * 40}, id="short-commit"),
        pytest.param({"commit_sha": "a" * 40, "tree_sha": "b" * 41}, id="long-tree"),
        pytest.param({"commit_sha": "a" * 40}, id="missing-tree"),
        pytest.param(
            {"commit_sha": "a" * 40, "tree_sha": "b" * 40, "extra": "x"},
            id="extra-field",
        ),
    ],
)
def test_bootstrap_predecessor_requires_the_exact_lowercase_pair(
    values: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        BootstrapPredecessorV1.model_validate(values)


def test_release_a_policy_fixture_declares_the_bootstrap_disabled_channel() -> None:
    channel = _policy().evidence_channel

    assert channel.cache_path == CACHE_PATH
    assert channel.deployment_mode == "disabled_for_bootstrap"
    assert channel.bootstrap_predecessor is None
    assert channel.jobs == EXACT_JOBS


def test_expected_gate_check_total_derives_from_the_frozen_policy() -> None:
    policy = _policy()

    assert expected_gate_check_total(policy) == (
        1
        + 1
        + 3
        + len(policy.root_snapshots)
        + len(policy.production_declarations)
        + len(policy.boundary_probes)
        + 17
        + 1
    )


def _run(job_id: str, minor: str, *, collected: int) -> PythonRunEvidenceV1:
    return PythonRunEvidenceV1.model_construct(
        python_minor=minor,
        job_id=job_id,
        job_run_id=1,
        workflow_run_id=7,
        run_attempt=1,
        candidate_commit_sha="a" * 40,
        candidate_tree_sha="b" * 40,
        collected=collected,
        passed=collected,
        skipped=0,
        deselected=0,
        result_digest="0" * 64,
        outcome="passed",
    )


def _wire(**overrides: object) -> R07DrGateEvidenceWireV1:
    total = expected_gate_check_total(_policy())
    values: dict[str, object] = {
        "schema_version": 1,
        "repository": "roxorlt/rquant",
        "workflow_path": ".github/workflows/ci.yml",
        "event_name": "push",
        "ref": "refs/heads/main",
        "producer_job_id": "r07-differential-gate-evidence",
        "workflow_run_id": 7,
        "run_attempt": 1,
        "candidate_commit_sha": "a" * 40,
        "candidate_tree_sha": "b" * 40,
        "baseline_commit_sha": BASELINE_COMMIT_SHA,
        "baseline_tree_sha": BASELINE_TREE_SHA,
        "policy_digest": "0" * 64,
        "complete_diff_digest": "0" * 64,
        "candidate_binding_digest": "0" * 64,
        "boundary_manifest_digest": "0" * 64,
        "boundary_result_digest": "0" * 64,
        "root_snapshot_digest": "0" * 64,
        "forbidden_definition_digest": "0" * 64,
        "python_runs": (
            _run("r07-differential-gate-py311", "3.11", collected=total),
            _run("r07-differential-gate-py312", "3.12", collected=total),
        ),
        "artifact_name": f"r07-dr-gate-{'a' * 40}",
        "artifact_json_path": "r07-dr-gate/evidence-v1.json",
        "retention_days": 90,
        "outcome": "passed",
        "evidence_digest": "0" * 64,
    }
    values.update(overrides)
    return R07DrGateEvidenceWireV1.model_construct(**values)


def test_channel_binding_accepts_the_exact_policy_channel_and_counts() -> None:
    differential_gate.verify_channel_binding(_policy(), _wire())


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"repository": "attacker/rquant"}, id="wire-repository"),
        pytest.param({"workflow_path": ".github/workflows/other.yml"}, id="wire-workflow-path"),
        pytest.param(
            {"producer_job_id": "r07-differential-gate-py311"},
            id="wire-producer-job",
        ),
        pytest.param(
            {"artifact_json_path": "r07-dr-gate/other.json"},
            id="wire-artifact-json-path",
        ),
        pytest.param({"retention_days": 30}, id="wire-retention"),
    ],
)
def test_channel_binding_rejects_wire_channel_drift(overrides: dict[str, object]) -> None:
    with pytest.raises(ValueError, match="channel"):
        differential_gate.verify_channel_binding(_policy(), _wire(**overrides))


@pytest.mark.parametrize(
    "jobs",
    [
        pytest.param(
            ("r07-differential-gate-py312", "r07-differential-gate-py311", EXACT_JOBS[2]),
            id="policy-python-jobs-swapped",
        ),
        pytest.param(
            (EXACT_JOBS[0], EXACT_JOBS[1], "r07-differential-gate-py311"),
            id="policy-producer-job-drift",
        ),
    ],
)
def test_channel_binding_rejects_policy_channel_drift(jobs: tuple[str, ...]) -> None:
    policy = _policy()
    tampered = policy.model_copy(
        update={
            "evidence_channel": EvidenceChannelV1.model_construct(
                **_channel_values(jobs=jobs),
            )
        }
    )

    with pytest.raises(ValueError, match="channel"):
        differential_gate.verify_channel_binding(tampered, _wire())


@pytest.mark.parametrize(
    "run_updates",
    [
        pytest.param({"collected": 1, "passed": 1}, id="self-consistent-single-check"),
        pytest.param({"collected": 10_000, "passed": 10_000}, id="inflated-check-total"),
        pytest.param({"skipped": 1}, id="skipped"),
        pytest.param({"deselected": 1}, id="deselected"),
    ],
)
def test_channel_binding_requires_policy_derived_check_counts(
    run_updates: dict[str, int],
) -> None:
    wire = _wire()
    runs = tuple(run.model_copy(update=run_updates) for run in wire.python_runs)

    with pytest.raises(ValueError, match="check count"):
        differential_gate.verify_channel_binding(_policy(), _wire(python_runs=runs))
