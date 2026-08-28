"""R07 evidence channel, deployment decision table, downloader, and cache tests."""

from __future__ import annotations

import hashlib
import io
import json
import os
import stat
import subprocess
import zipfile
from pathlib import Path

import pytest
from pydantic import ValidationError

import rquant.ops.r07_deploy_evidence as r07_deploy_evidence
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
        + len(differential_gate.FIXED_STATIC_CHECK_NAMES)
        + len(policy.root_snapshots)
        + len(policy.production_declarations)
        + len(policy.boundary_probes)
        + 17
        + 1
    )


GATE_DIGEST_FIELDS = {
    "candidate_gate_digest": "a" * 64,
    "static_result_digest": "b" * 64,
    "boundary_result_digest": "0" * 64,
    "check_inventory_digest": "c" * 64,
}
MERGE_PARENT_SHA = "e" * 40


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
        **GATE_DIGEST_FIELDS,
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
        "merge_base_commit_sha": BASELINE_COMMIT_SHA,
        "merge_base_tree_sha": BASELINE_TREE_SHA,
        "candidate_parent_commits": (BASELINE_COMMIT_SHA, MERGE_PARENT_SHA),
        "merge_tree_sha": "b" * 40,
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


class _FakeTransport:
    """Offline stand-in for the GitHub REST/artifact channel."""

    def __init__(self, responses: dict[str, bytes]) -> None:
        self.responses = responses
        self.requests: list[str] = []
        self.tokens: list[str] = []

    def get(self, url: str, *, token: str, accept: str) -> bytes:
        self.requests.append(url)
        self.tokens.append(token)
        if url not in self.responses:
            raise r07_deploy_evidence.R07EvidenceError(f"no route: {url}")
        return self.responses[url]


class _FakeVerifier:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def __call__(
        self,
        repo: Path,
        policy: R07PolicyV1,
        wire: R07DrGateEvidenceWireV1,
    ) -> object:
        self.calls.append(wire.candidate_commit_sha)
        return object()


class _RejectingVerifier:
    def __call__(
        self,
        repo: Path,
        policy: R07PolicyV1,
        wire: R07DrGateEvidenceWireV1,
    ) -> object:
        raise ValueError("candidate gate did not pass")


COMMIT_A = "a" * 40
TREE_A = "b" * 40
COMMIT_B = "c" * 40
TREE_B = "d" * 40
RUN_ID = 4242


def _valid_wire_bytes(
    commit_sha: str,
    tree_sha: str,
    *,
    policy: R07PolicyV1,
    workflow_run_id: int = RUN_ID,
    run_attempt: int = 2,
) -> bytes:
    total = expected_gate_check_total(policy)
    runs = tuple(
        PythonRunEvidenceV1.model_validate(
            {
                "python_minor": minor,
                "job_id": job_id,
                "job_run_id": index + 1,
                "workflow_run_id": workflow_run_id,
                "run_attempt": run_attempt,
                "candidate_commit_sha": commit_sha,
                "candidate_tree_sha": tree_sha,
                "collected": total,
                "passed": total,
                "skipped": 0,
                "deselected": 0,
                "candidate_gate_digest": "a" * 64,
                "static_result_digest": "b" * 64,
                "boundary_result_digest": "3" * 64,
                "check_inventory_digest": "c" * 64,
                "result_digest": "0" * 64,
                "outcome": "passed",
            }
        )
        for index, (minor, job_id) in enumerate(
            (("3.11", EXACT_JOBS[0]), ("3.12", EXACT_JOBS[1]))
        )
    )
    runs = tuple(
        run.model_copy(update={"result_digest": differential_gate.python_run_result_digest(run)})
        for run in runs
    )
    values: dict[str, object] = {
        "schema_version": 1,
        "repository": "roxorlt/rquant",
        "workflow_path": ".github/workflows/ci.yml",
        "event_name": "push",
        "ref": "refs/heads/main",
        "producer_job_id": EXACT_JOBS[2],
        "workflow_run_id": workflow_run_id,
        "run_attempt": run_attempt,
        "candidate_commit_sha": commit_sha,
        "candidate_tree_sha": tree_sha,
        "baseline_commit_sha": BASELINE_COMMIT_SHA,
        "baseline_tree_sha": BASELINE_TREE_SHA,
        "merge_base_commit_sha": BASELINE_COMMIT_SHA,
        "merge_base_tree_sha": BASELINE_TREE_SHA,
        "candidate_parent_commits": (BASELINE_COMMIT_SHA, MERGE_PARENT_SHA),
        "merge_tree_sha": tree_sha,
        "policy_digest": policy.policy_digest,
        "complete_diff_digest": "1" * 64,
        "candidate_binding_digest": differential_gate._candidate_binding_digest_values(
            baseline_commit_sha=BASELINE_COMMIT_SHA,
            baseline_tree_sha=BASELINE_TREE_SHA,
            candidate_commit_sha=commit_sha,
            candidate_tree_sha=tree_sha,
            complete_diff_digest="1" * 64,
        ),
        "boundary_manifest_digest": "2" * 64,
        "boundary_result_digest": "3" * 64,
        "root_snapshot_digest": "4" * 64,
        "forbidden_definition_digest": "5" * 64,
        "python_runs": runs,
        "artifact_name": f"r07-dr-gate-{commit_sha}",
        "artifact_json_path": "r07-dr-gate/evidence-v1.json",
        "retention_days": 90,
        "outcome": "passed",
        "evidence_digest": "0" * 64,
    }
    provisional = R07DrGateEvidenceWireV1.model_construct(**values)
    values["evidence_digest"] = differential_gate._digest_without_field(
        provisional,
        "evidence_digest",
    )
    wire = R07DrGateEvidenceWireV1.model_validate(values)
    return differential_gate.canonical_evidence_json_bytes(wire)


def _artifact_zip(entries: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as bundle:
        for name, payload in entries.items():
            bundle.writestr(name, payload)
    return buffer.getvalue()


def _run_payload(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "id": RUN_ID,
        "path": ".github/workflows/ci.yml",
        "event": "push",
        "head_branch": "main",
        "head_sha": COMMIT_B,
        "status": "completed",
        "conclusion": "success",
        "run_attempt": 2,
    }
    values.update(overrides)
    return values


def _artifact_payload(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "id": 91,
        "name": f"r07-dr-gate-{COMMIT_B}",
        "expired": False,
        "archive_download_url": "https://api.github.com/artifact/91/zip",
    }
    values.update(overrides)
    return values


def _channel_responses(
    *,
    runs: list[dict[str, object]] | None = None,
    artifacts: list[dict[str, object]] | None = None,
    archive: bytes | None = None,
    policy: R07PolicyV1 | None = None,
) -> dict[str, bytes]:
    resolved_policy = policy or _policy()
    payload_runs = [_run_payload()] if runs is None else runs
    payload_artifacts = [_artifact_payload()] if artifacts is None else artifacts
    zip_bytes = (
        _artifact_zip(
            {
                "r07-dr-gate/evidence-v1.json": _valid_wire_bytes(
                    COMMIT_B,
                    TREE_B,
                    policy=resolved_policy,
                )
            }
        )
        if archive is None
        else archive
    )
    return {
        r07_deploy_evidence.workflow_runs_url(COMMIT_B): json.dumps(
            {"total_count": len(payload_runs), "workflow_runs": payload_runs}
        ).encode(),
        r07_deploy_evidence.run_artifacts_url(RUN_ID): json.dumps(
            {"total_count": len(payload_artifacts), "artifacts": payload_artifacts}
        ).encode(),
        "https://api.github.com/artifact/91/zip": zip_bytes,
    }


def _state(
    kind: str,
    mode: str | None,
    *,
    commit_sha: str,
    tree_sha: str,
    predecessor: BootstrapPredecessorV1 | None = None,
) -> object:
    model = (
        r07_deploy_evidence.InstalledPolicyState
        if kind == "installed"
        else r07_deploy_evidence.TargetPolicyState
    )
    return model(
        commit_sha=commit_sha,
        tree_sha=tree_sha,
        deployment_mode=mode,
        bootstrap_predecessor=predecessor,
        policy_digest=None if mode is None else "0" * 64,
    )


def _installed(mode: str | None, **kwargs: object) -> object:
    return _state("installed", mode, commit_sha=COMMIT_A, tree_sha=TREE_A, **kwargs)


def _target(mode: str | None, **kwargs: object) -> object:
    return _state("target", mode, commit_sha=COMMIT_B, tree_sha=TREE_B, **kwargs)


def _installed_pair() -> BootstrapPredecessorV1:
    return BootstrapPredecessorV1(commit_sha=COMMIT_A, tree_sha=TREE_A)


@pytest.mark.parametrize(
    "field",
    [
        pytest.param("installed_commit_sha", id="installed-commit"),
        pytest.param("installed_tree_sha", id="installed-tree"),
        pytest.param("target_commit_sha", id="target-commit"),
        pytest.param("target_tree_sha", id="target-tree"),
    ],
)
def test_an_allowed_decision_requires_every_commit_and_tree_id(field: str) -> None:
    values = {
        "allowed": True,
        "gate": "bootstrap_disabled",
        "reason": "Release A bootstrap install from a pre-R07 checkout",
        "requires_evidence": False,
        "installed_mode": "absent",
        "target_mode": "disabled_for_bootstrap",
        "installed_commit_sha": COMMIT_A,
        "installed_tree_sha": TREE_A,
        "target_commit_sha": COMMIT_B,
        "target_tree_sha": TREE_B,
    }
    r07_deploy_evidence.R07DeployDecision.model_validate(values)

    with pytest.raises(ValidationError):
        r07_deploy_evidence.R07DeployDecision.model_validate({**values, field: ""})


def test_an_unresolved_rejection_may_omit_the_commit_and_tree_ids() -> None:
    decision = r07_deploy_evidence.R07DeployDecision.model_validate(
        {
            "allowed": False,
            "gate": "rejected",
            "reason": "R07 policy objects could not be resolved",
            "requires_evidence": False,
            "installed_mode": "unresolved",
            "target_mode": "unresolved",
            "installed_commit_sha": "",
            "installed_tree_sha": "",
            "target_commit_sha": "",
            "target_tree_sha": "",
        }
    )

    assert decision.allowed is False
    assert decision.audit_fields()["r07_target_commit_sha"] == ""


def test_decision_allows_release_a_once_from_a_pre_r07_checkout() -> None:
    decision = r07_deploy_evidence.decide_r07_deployment(
        _installed(None),
        _target("disabled_for_bootstrap"),
    )

    assert decision.allowed is True
    assert decision.gate == "bootstrap_disabled"
    assert decision.requires_evidence is False
    assert decision.target_commit_sha == COMMIT_B
    assert decision.target_tree_sha == TREE_B


def test_decision_allows_release_b_naming_the_installed_bootstrap_pair() -> None:
    decision = r07_deploy_evidence.decide_r07_deployment(
        _installed("disabled_for_bootstrap"),
        _target("enforced", predecessor=_installed_pair()),
    )

    assert decision.allowed is True
    assert decision.gate == "enforced"
    assert decision.requires_evidence is True


def test_decision_allows_enforced_forward_and_rollback_targets() -> None:
    decision = r07_deploy_evidence.decide_r07_deployment(
        _installed("enforced", predecessor=_installed_pair()),
        _target("enforced", predecessor=_installed_pair()),
    )

    assert decision.allowed is True
    assert decision.gate == "enforced"
    assert decision.requires_evidence is True


@pytest.mark.parametrize(
    ("installed_mode", "target_mode", "predecessor", "expected"),
    [
        pytest.param(None, "enforced", "installed", "predecessor", id="absent-to-enforced"),
        pytest.param(None, None, None, "pre-R07", id="absent-to-absent"),
        pytest.param(
            "disabled_for_bootstrap",
            "disabled_for_bootstrap",
            None,
            "bootstrap",
            id="disabled-to-disabled",
        ),
        pytest.param("disabled_for_bootstrap", None, None, "pre-R07", id="disabled-to-absent"),
        pytest.param(
            "disabled_for_bootstrap",
            "enforced",
            "other",
            "predecessor",
            id="disabled-to-enforced-wrong-predecessor",
        ),
        pytest.param(
            "enforced",
            "disabled_for_bootstrap",
            None,
            "enforced",
            id="enforced-to-disabled",
        ),
        pytest.param("enforced", None, None, "pre-R07", id="enforced-to-absent"),
    ],
)
def test_decision_table_rejects_every_weakening_transition(
    installed_mode: str | None,
    target_mode: str | None,
    predecessor: str | None,
    expected: str,
) -> None:
    predecessor_pair = {
        None: None,
        "installed": _installed_pair(),
        "other": BootstrapPredecessorV1(commit_sha="9" * 40, tree_sha="8" * 40),
    }[predecessor]
    installed_predecessor = _installed_pair() if installed_mode == "enforced" else None

    decision = r07_deploy_evidence.decide_r07_deployment(
        _installed(installed_mode, predecessor=installed_predecessor),
        _target(target_mode, predecessor=predecessor_pair),
    )

    assert decision.allowed is False
    assert decision.gate == "rejected"
    assert decision.requires_evidence is False
    assert expected in decision.reason


def test_decision_rejects_an_already_current_bootstrap_target() -> None:
    installed = _state(
        "installed",
        "disabled_for_bootstrap",
        commit_sha=COMMIT_B,
        tree_sha=TREE_B,
    )

    decision = r07_deploy_evidence.decide_r07_deployment(
        installed,
        _target("disabled_for_bootstrap"),
    )

    assert decision.allowed is False
    assert "bootstrap" in decision.reason


def _cache_dir(tmp_path: Path) -> Path:
    directory = tmp_path / "r07-dr-evidence"
    directory.mkdir()
    return directory


def _lab_trust(
    root: Path,
    *,
    allow_sticky_ancestors: bool = False,
) -> r07_deploy_evidence.EvidenceCacheTrustV1:
    """Root the ancestor walk at the test directory: ``/tmp`` itself is sticky on Linux."""

    return r07_deploy_evidence.EvidenceCacheTrustV1.for_deployment_process(
        trusted_root=root,
        allow_sticky_ancestors=allow_sticky_ancestors,
    )


def _identity(
    *,
    workflow_run_id: int = RUN_ID,
    run_attempt: int = 2,
) -> r07_deploy_evidence.ResolvedRunIdentityV1:
    return r07_deploy_evidence.ResolvedRunIdentityV1(
        workflow_run_id=workflow_run_id,
        run_attempt=run_attempt,
    )


def test_cache_write_is_atomic_and_world_readable(tmp_path: Path) -> None:
    directory = tmp_path / "nested" / "r07-dr-evidence"
    payload = _valid_wire_bytes(COMMIT_B, TREE_B, policy=_policy())

    written = r07_deploy_evidence.write_cached_evidence(
        cache_dir=directory,
        commit_sha=COMMIT_B,
        payload=payload,
    )

    assert written == directory / f"{COMMIT_B}.json"
    assert written.read_bytes() == payload
    assert stat.S_IMODE(written.lstat().st_mode) == 0o644
    assert stat.S_IMODE(directory.lstat().st_mode) == 0o755
    assert [path.name for path in sorted(directory.iterdir())] == [f"{COMMIT_B}.json"]


def test_cache_read_returns_none_for_a_missing_entry(tmp_path: Path) -> None:
    assert (
        r07_deploy_evidence.read_cached_evidence(
            cache_dir=_cache_dir(tmp_path),
            commit_sha=COMMIT_B,
            tree_sha=TREE_B,
            policy=_policy(),
            run_identity=_identity(),
            trust=_lab_trust(tmp_path),
        )
        is None
    )


def test_cache_read_accepts_the_exact_bound_entry(tmp_path: Path) -> None:
    directory = _cache_dir(tmp_path)
    policy = _policy()
    r07_deploy_evidence.write_cached_evidence(
        cache_dir=directory,
        commit_sha=COMMIT_B,
        payload=_valid_wire_bytes(COMMIT_B, TREE_B, policy=policy),
    )

    wire = r07_deploy_evidence.read_cached_evidence(
        cache_dir=directory,
        commit_sha=COMMIT_B,
        tree_sha=TREE_B,
        policy=policy,
        run_identity=_identity(),
        trust=_lab_trust(tmp_path),
    )

    assert wire is not None
    assert wire.candidate_commit_sha == COMMIT_B
    assert type(wire) is R07DrGateEvidenceWireV1


def test_cache_read_rejects_a_symlinked_entry(tmp_path: Path) -> None:
    directory = _cache_dir(tmp_path)
    real = tmp_path / "evidence.json"
    real.write_bytes(_valid_wire_bytes(COMMIT_B, TREE_B, policy=_policy()))
    (directory / f"{COMMIT_B}.json").symlink_to(real)

    with pytest.raises(r07_deploy_evidence.R07EvidenceError, match="symlink|regular"):
        r07_deploy_evidence.read_cached_evidence(
            cache_dir=directory,
            commit_sha=COMMIT_B,
            tree_sha=TREE_B,
            policy=_policy(),
            run_identity=_identity(),
            trust=_lab_trust(tmp_path),
        )


def test_cache_read_rejects_a_directory_entry(tmp_path: Path) -> None:
    directory = _cache_dir(tmp_path)
    (directory / f"{COMMIT_B}.json").mkdir()

    with pytest.raises(r07_deploy_evidence.R07EvidenceError, match="regular"):
        r07_deploy_evidence.read_cached_evidence(
            cache_dir=directory,
            commit_sha=COMMIT_B,
            tree_sha=TREE_B,
            policy=_policy(),
            run_identity=_identity(),
            trust=_lab_trust(tmp_path),
        )


def test_cache_read_rejects_a_fifo_entry(tmp_path: Path) -> None:
    directory = _cache_dir(tmp_path)
    os.mkfifo(directory / f"{COMMIT_B}.json")

    with pytest.raises(r07_deploy_evidence.R07EvidenceError, match="regular"):
        r07_deploy_evidence.read_cached_evidence(
            cache_dir=directory,
            commit_sha=COMMIT_B,
            tree_sha=TREE_B,
            policy=_policy(),
            run_identity=_identity(),
            trust=_lab_trust(tmp_path),
        )


def test_cache_read_rejects_an_entry_named_for_another_commit(tmp_path: Path) -> None:
    directory = _cache_dir(tmp_path)
    policy = _policy()
    (directory / f"{COMMIT_B}.json").write_bytes(
        _valid_wire_bytes(COMMIT_A, TREE_A, policy=policy)
    )

    with pytest.raises(r07_deploy_evidence.R07EvidenceError, match="commit"):
        r07_deploy_evidence.read_cached_evidence(
            cache_dir=directory,
            commit_sha=COMMIT_B,
            tree_sha=TREE_B,
            policy=policy,
            run_identity=_identity(),
            trust=_lab_trust(tmp_path),
        )


def test_cache_read_rejects_a_tree_that_is_not_the_target_tree(tmp_path: Path) -> None:
    directory = _cache_dir(tmp_path)
    policy = _policy()
    (directory / f"{COMMIT_B}.json").write_bytes(
        _valid_wire_bytes(COMMIT_B, TREE_A, policy=policy)
    )

    with pytest.raises(r07_deploy_evidence.R07EvidenceError, match="tree"):
        r07_deploy_evidence.read_cached_evidence(
            cache_dir=directory,
            commit_sha=COMMIT_B,
            tree_sha=TREE_B,
            policy=policy,
            run_identity=_identity(),
            trust=_lab_trust(tmp_path),
        )


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(lambda raw: raw + b"\n", id="trailing-newline"),
        pytest.param(lambda raw: b" " + raw, id="leading-space"),
        pytest.param(lambda raw: raw.replace(b'{"', b'{ "', 1), id="pretty-printed"),
        pytest.param(lambda raw: raw[:-1], id="truncated"),
    ],
)
def test_cache_read_rejects_non_canonical_bytes(tmp_path: Path, mutate) -> None:
    directory = _cache_dir(tmp_path)
    policy = _policy()
    raw = _valid_wire_bytes(COMMIT_B, TREE_B, policy=policy)
    (directory / f"{COMMIT_B}.json").write_bytes(mutate(raw))

    with pytest.raises(r07_deploy_evidence.R07EvidenceError):
        r07_deploy_evidence.read_cached_evidence(
            cache_dir=directory,
            commit_sha=COMMIT_B,
            tree_sha=TREE_B,
            policy=policy,
            run_identity=_identity(),
            trust=_lab_trust(tmp_path),
        )


def test_cache_read_rejects_duplicate_json_keys(tmp_path: Path) -> None:
    directory = _cache_dir(tmp_path)
    policy = _policy()
    raw = _valid_wire_bytes(COMMIT_B, TREE_B, policy=policy)
    duplicated = raw.replace(
        b'{"artifact_json_path"',
        b'{"artifact_json_path":"r07-dr-gate/evidence-v1.json","artifact_json_path"',
        1,
    )
    (directory / f"{COMMIT_B}.json").write_bytes(duplicated)

    with pytest.raises(r07_deploy_evidence.R07EvidenceError):
        r07_deploy_evidence.read_cached_evidence(
            cache_dir=directory,
            commit_sha=COMMIT_B,
            tree_sha=TREE_B,
            policy=policy,
            run_identity=_identity(),
            trust=_lab_trust(tmp_path),
        )


def test_cache_read_rejects_a_tampered_evidence_digest(tmp_path: Path) -> None:
    directory = _cache_dir(tmp_path)
    policy = _policy()
    raw = _valid_wire_bytes(COMMIT_B, TREE_B, policy=policy)
    payload = json.loads(raw)
    payload["evidence_digest"] = "f" * 64
    (directory / f"{COMMIT_B}.json").write_bytes(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    )

    with pytest.raises(r07_deploy_evidence.R07EvidenceError):
        r07_deploy_evidence.read_cached_evidence(
            cache_dir=directory,
            commit_sha=COMMIT_B,
            tree_sha=TREE_B,
            policy=policy,
            run_identity=_identity(),
            trust=_lab_trust(tmp_path),
        )


def test_cache_read_rejects_a_policy_digest_from_another_policy(tmp_path: Path) -> None:
    directory = _cache_dir(tmp_path)
    policy = _policy()
    other = policy.model_copy(update={"policy_digest": "e" * 64})
    (directory / f"{COMMIT_B}.json").write_bytes(
        _valid_wire_bytes(COMMIT_B, TREE_B, policy=other)
    )

    with pytest.raises(r07_deploy_evidence.R07EvidenceError, match="policy"):
        r07_deploy_evidence.read_cached_evidence(
            cache_dir=directory,
            commit_sha=COMMIT_B,
            tree_sha=TREE_B,
            policy=policy,
            run_identity=_identity(),
            trust=_lab_trust(tmp_path),
        )


def test_cache_read_rejects_channel_metadata_drift(tmp_path: Path) -> None:
    directory = _cache_dir(tmp_path)
    policy = _policy()
    tampered = policy.model_copy(
        update={
            "evidence_channel": EvidenceChannelV1.model_construct(
                **_channel_values(
                    jobs=(EXACT_JOBS[1], EXACT_JOBS[0], EXACT_JOBS[2]),
                )
            )
        }
    )
    (directory / f"{COMMIT_B}.json").write_bytes(
        _valid_wire_bytes(COMMIT_B, TREE_B, policy=tampered)
    )

    with pytest.raises(r07_deploy_evidence.R07EvidenceError, match="channel|policy"):
        r07_deploy_evidence.read_cached_evidence(
            cache_dir=directory,
            commit_sha=COMMIT_B,
            tree_sha=TREE_B,
            policy=tampered,
            run_identity=_identity(),
            trust=_lab_trust(tmp_path),
        )


class _FakeTokenProvider:
    def __init__(self, value: str = "ghs-test-token") -> None:
        self.value = value

    def token(self) -> str:
        if not self.value:
            raise r07_deploy_evidence.R07EvidenceError(
                "RQUANT_GITHUB_EVIDENCE_TOKEN is not configured"
            )
        return self.value


def _download(
    responses: dict[str, bytes],
    *,
    token_provider: object | None = None,
    clock: object | None = None,
) -> bytes:
    transport = _FakeTransport(responses)
    raw, _identity = r07_deploy_evidence.download_evidence_bytes(
        commit_sha=COMMIT_B,
        transport=transport,
        token_provider=token_provider or _FakeTokenProvider(),
        clock=clock or (lambda: 0.0),
    )
    return raw


def test_downloader_reads_only_the_fixed_repository_workflow_and_artifact() -> None:
    responses = _channel_responses()
    transport = _FakeTransport(responses)

    raw, identity = r07_deploy_evidence.download_evidence_bytes(
        commit_sha=COMMIT_B,
        transport=transport,
        token_provider=_FakeTokenProvider(),
        clock=lambda: 0.0,
    )

    assert raw == _valid_wire_bytes(COMMIT_B, TREE_B, policy=_policy())
    assert identity == r07_deploy_evidence.ResolvedRunIdentityV1(
        workflow_run_id=RUN_ID,
        run_attempt=2,
    )
    assert transport.requests[0].startswith(
        "https://api.github.com/repos/roxorlt/rquant/actions/workflows/ci.yml/runs"
    )
    assert f"head_sha={COMMIT_B}" in transport.requests[0]
    assert "event=push" in transport.requests[0]
    assert "branch=main" in transport.requests[0]
    assert transport.tokens == ["ghs-test-token"] * len(transport.requests)


def test_downloader_requires_a_configured_token() -> None:
    with pytest.raises(r07_deploy_evidence.R07EvidenceError, match="RQUANT_GITHUB_EVIDENCE_TOKEN"):
        _download(_channel_responses(), token_provider=_FakeTokenProvider(""))


def test_downloader_never_echoes_the_token_in_its_rejection() -> None:
    responses = _channel_responses(runs=[])

    with pytest.raises(r07_deploy_evidence.R07EvidenceError) as excinfo:
        _download(responses)

    assert "ghs-test-token" not in str(excinfo.value)


def test_downloader_selects_the_highest_successful_attempt_of_one_run() -> None:
    highest = _valid_wire_bytes(COMMIT_B, TREE_B, policy=_policy(), run_attempt=9)
    responses = _channel_responses(
        runs=[
            _run_payload(run_attempt=1),
            _run_payload(run_attempt=3),
            _run_payload(run_attempt=9),
            _run_payload(run_attempt=10, conclusion="failure"),
        ],
        archive=_artifact_zip({"r07-dr-gate/evidence-v1.json": highest}),
    )

    assert _download(responses) == highest


@pytest.mark.parametrize(
    ("claimed_attempt", "claimed_run_id"),
    [
        pytest.param(2, RUN_ID, id="lower-attempt"),
        pytest.param(10, RUN_ID, id="higher-attempt"),
        pytest.param(9, RUN_ID + 1, id="other-run-id"),
        pytest.param(9, 999999, id="unrelated-run-id"),
    ],
)
def test_downloader_rejects_evidence_that_claims_another_run_identity(
    claimed_attempt: int,
    claimed_run_id: int,
) -> None:
    claimed = _valid_wire_bytes(
        COMMIT_B,
        TREE_B,
        policy=_policy(),
        workflow_run_id=claimed_run_id,
        run_attempt=claimed_attempt,
    )
    responses = _channel_responses(
        runs=[_run_payload(run_attempt=1), _run_payload(run_attempt=9)],
        archive=_artifact_zip({"r07-dr-gate/evidence-v1.json": claimed}),
    )

    with pytest.raises(r07_deploy_evidence.R07EvidenceError, match="run"):
        _download(responses)


def test_bind_evidence_wire_requires_the_resolved_run_identity() -> None:
    policy = _policy()
    raw = _valid_wire_bytes(COMMIT_B, TREE_B, policy=policy, run_attempt=9)
    identity = r07_deploy_evidence.ResolvedRunIdentityV1(
        workflow_run_id=RUN_ID,
        run_attempt=9,
    )

    bound = r07_deploy_evidence.bind_evidence_wire(
        raw,
        commit_sha=COMMIT_B,
        tree_sha=TREE_B,
        policy=policy,
        run_identity=identity,
    )
    assert bound.run_attempt == 9

    with pytest.raises(r07_deploy_evidence.R07EvidenceError, match="run"):
        r07_deploy_evidence.bind_evidence_wire(
            raw,
            commit_sha=COMMIT_B,
            tree_sha=TREE_B,
            policy=policy,
            run_identity=identity.model_copy(update={"run_attempt": 3}),
        )


@pytest.mark.parametrize(
    ("runs", "expected"),
    [
        pytest.param([], "exactly one", id="no-run"),
        pytest.param(
            [_run_payload(), _run_payload(id=RUN_ID + 1)],
            "exactly one",
            id="ambiguous-second-run-id",
        ),
        pytest.param([_run_payload(event="pull_request")], "exactly one", id="pull-request"),
        pytest.param(
            [_run_payload(event="workflow_dispatch")],
            "exactly one",
            id="workflow-dispatch",
        ),
        pytest.param([_run_payload(head_branch="topic")], "exactly one", id="non-main-branch"),
        pytest.param(
            [_run_payload(path=".github/workflows/other.yml")],
            "exactly one",
            id="other-workflow",
        ),
        pytest.param([_run_payload(head_sha=COMMIT_A)], "exactly one", id="other-sha"),
        pytest.param([_run_payload(conclusion="failure")], "successful", id="failed-conclusion"),
        pytest.param([_run_payload(conclusion=None)], "successful", id="null-conclusion"),
        pytest.param(
            [_run_payload(status="in_progress", conclusion=None)],
            "successful",
            id="incomplete-run",
        ),
    ],
)
def test_downloader_blocks_ambiguous_or_unsuccessful_run_identity(
    runs: list[dict[str, object]],
    expected: str,
) -> None:
    with pytest.raises(r07_deploy_evidence.R07EvidenceError, match=expected):
        _download(_channel_responses(runs=runs))


@pytest.mark.parametrize(
    "artifacts",
    [
        pytest.param([], id="no-artifact"),
        pytest.param([_artifact_payload(name="r07-dr-gate")], id="unbound-name"),
        pytest.param(
            [_artifact_payload(name=f"r07-dr-gate-{COMMIT_A}")],
            id="other-commit-name",
        ),
        pytest.param(
            [_artifact_payload(), _artifact_payload(id=92)],
            id="duplicate-artifact",
        ),
        pytest.param([_artifact_payload(expired=True)], id="expired-artifact"),
    ],
)
def test_downloader_accepts_only_the_exact_bound_artifact(
    artifacts: list[dict[str, object]],
) -> None:
    with pytest.raises(r07_deploy_evidence.R07EvidenceError, match="artifact"):
        _download(_channel_responses(artifacts=artifacts))


@pytest.mark.parametrize(
    "entries",
    [
        pytest.param({"evidence-v1.json": b"{}"}, id="missing-fixed-path"),
        pytest.param({"r07-dr-gate/other.json": b"{}"}, id="wrong-internal-name"),
        pytest.param(
            {"r07-dr-gate/evidence-v1.json": b"{}", "extra.txt": b"x"},
            id="extra-entry",
        ),
    ],
)
def test_downloader_accepts_only_the_fixed_internal_artifact_layout(
    entries: dict[str, bytes],
) -> None:
    with pytest.raises(r07_deploy_evidence.R07EvidenceError, match="artifact"):
        _download(_channel_responses(archive=_artifact_zip(entries)))


def test_downloader_rejects_a_non_zip_archive() -> None:
    with pytest.raises(r07_deploy_evidence.R07EvidenceError, match="artifact"):
        _download(_channel_responses(archive=b"not-a-zip"))


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(lambda raw: raw + b"\n", id="non-canonical"),
        pytest.param(
            lambda raw: raw.replace(b'"retention_days":90', b'"retention_days":30'),
            id="channel-drift",
        ),
        pytest.param(
            lambda raw: raw.replace(b'"outcome":"passed"', b'"outcome":"failed"'),
            id="failed-outcome",
        ),
    ],
)
def test_downloader_rejects_artifact_bytes_that_are_not_bound_evidence(mutate) -> None:
    raw = mutate(_valid_wire_bytes(COMMIT_B, TREE_B, policy=_policy()))
    archive = _artifact_zip({"r07-dr-gate/evidence-v1.json": raw})

    with pytest.raises(r07_deploy_evidence.R07EvidenceError):
        _download(_channel_responses(archive=archive))


def test_downloader_rejects_duplicate_json_keys() -> None:
    raw = _valid_wire_bytes(COMMIT_B, TREE_B, policy=_policy())
    duplicated = raw.replace(
        b'{"artifact_json_path"',
        b'{"artifact_json_path":"r07-dr-gate/evidence-v1.json","artifact_json_path"',
        1,
    )
    archive = _artifact_zip({"r07-dr-gate/evidence-v1.json": duplicated})

    with pytest.raises(r07_deploy_evidence.R07EvidenceError):
        _download(_channel_responses(archive=archive))


def test_downloader_rejects_evidence_bound_to_another_commit() -> None:
    archive = _artifact_zip(
        {"r07-dr-gate/evidence-v1.json": _valid_wire_bytes(COMMIT_A, TREE_A, policy=_policy())}
    )

    with pytest.raises(r07_deploy_evidence.R07EvidenceError, match="commit"):
        _download(_channel_responses(archive=archive))


def test_downloader_stops_when_the_injected_clock_exceeds_the_budget() -> None:
    ticks = iter([0.0, 1.0, 10_000.0, 10_001.0, 10_002.0])

    with pytest.raises(r07_deploy_evidence.R07EvidenceError, match="deadline"):
        _download(_channel_responses(), clock=lambda: next(ticks))


class _RealGitRunner:
    """Minimal runner that actually executes the trusted Git binary."""

    def __init__(self, cwd: Path) -> None:
        self.cwd = cwd

    def run(
        self,
        args: list[str],
        *,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            args,
            cwd=self.cwd,
            check=check,
            capture_output=True,
            text=True,
        )


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _policy_repo(tmp_path: Path, *, enforced: bool = False) -> tuple[Path, str]:
    repo = tmp_path / "checkout"
    (repo / "tests" / "fixtures" / "r07_differential_gate").mkdir(parents=True)
    _git_init(repo)
    payload = POLICY_PATH.read_bytes()
    if enforced:
        payload = _enforced_policy_bytes()
    (repo / "tests" / "fixtures" / "r07_differential_gate" / "policy-v1.json").write_bytes(payload)
    _git(repo, "add", "-A")
    _git(
        repo,
        "-c",
        "user.email=test@example.invalid",
        "-c",
        "user.name=test",
        "commit",
        "--quiet",
        "-m",
        "policy",
    )
    return repo, _git(repo, "rev-parse", "HEAD")


def _git_init(repo: Path) -> None:
    subprocess.run(["git", "init", "--quiet", str(repo)], check=True, capture_output=True)


def _enforced_policy_bytes(commit_sha: str = COMMIT_A, tree_sha: str = TREE_A) -> bytes:
    raw = json.loads(POLICY_PATH.read_bytes())
    raw["evidence_channel"]["deployment_mode"] = "enforced"
    raw["evidence_channel"]["bootstrap_predecessor"] = {
        "commit_sha": commit_sha,
        "tree_sha": tree_sha,
    }
    del raw["policy_digest"]
    canonical = json.dumps(raw, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    raw["policy_digest"] = hashlib.sha256(canonical.encode()).hexdigest()
    return json.dumps(raw, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def test_policy_blob_is_read_from_the_git_object_not_the_dirty_worktree(
    tmp_path: Path,
) -> None:
    repo, commit = _policy_repo(tmp_path)
    worktree_policy = repo / "tests" / "fixtures" / "r07_differential_gate" / "policy-v1.json"
    worktree_policy.write_bytes(b'{"schema_version":1}')

    raw = r07_deploy_evidence.read_policy_blob(
        runner=_RealGitRunner(repo),
        git_path=Path("/usr/bin/git"),
        commit_sha=commit,
    )

    assert raw == POLICY_PATH.read_bytes()
    assert raw != worktree_policy.read_bytes()


def test_policy_blob_is_absent_for_a_pre_r07_commit(tmp_path: Path) -> None:
    repo = tmp_path / "checkout"
    repo.mkdir()
    _git_init(repo)
    (repo / "README.md").write_text("pre-r07\n")
    _git(repo, "add", "-A")
    _git(
        repo,
        "-c",
        "user.email=test@example.invalid",
        "-c",
        "user.name=test",
        "commit",
        "--quiet",
        "-m",
        "pre-r07",
    )
    commit = _git(repo, "rev-parse", "HEAD")

    assert (
        r07_deploy_evidence.read_policy_blob(
            runner=_RealGitRunner(repo),
            git_path=Path("/usr/bin/git"),
            commit_sha=commit,
        )
        is None
    )


def test_policy_blob_rejects_a_symlinked_policy_object(tmp_path: Path) -> None:
    repo = tmp_path / "checkout"
    fixture_dir = repo / "tests" / "fixtures" / "r07_differential_gate"
    fixture_dir.mkdir(parents=True)
    _git_init(repo)
    (repo / "elsewhere.json").write_bytes(POLICY_PATH.read_bytes())
    (fixture_dir / "policy-v1.json").symlink_to(repo / "elsewhere.json")
    _git(repo, "add", "-A")
    _git(
        repo,
        "-c",
        "user.email=test@example.invalid",
        "-c",
        "user.name=test",
        "commit",
        "--quiet",
        "-m",
        "symlinked policy",
    )
    commit = _git(repo, "rev-parse", "HEAD")

    with pytest.raises(r07_deploy_evidence.R07EvidenceError, match="regular blob"):
        r07_deploy_evidence.read_policy_blob(
            runner=_RealGitRunner(repo),
            git_path=Path("/usr/bin/git"),
            commit_sha=commit,
        )


def test_target_policy_state_reports_the_committed_mode_and_predecessor(
    tmp_path: Path,
) -> None:
    repo, commit = _policy_repo(tmp_path, enforced=True)

    state, policy = r07_deploy_evidence.read_policy_state(
        runner=_RealGitRunner(repo),
        git_path=Path("/usr/bin/git"),
        commit_sha=commit,
        role="target",
    )

    assert type(state) is r07_deploy_evidence.TargetPolicyState
    assert state.deployment_mode == "enforced"
    assert state.bootstrap_predecessor == BootstrapPredecessorV1(
        commit_sha=COMMIT_A,
        tree_sha=TREE_A,
    )
    assert policy is not None
    assert state.policy_digest == policy.policy_digest
    assert state.tree_sha == _git(repo, "rev-parse", "--verify", f"{commit}^{{tree}}")


def test_installed_policy_state_reports_absent_for_a_pre_r07_commit(tmp_path: Path) -> None:
    repo = tmp_path / "checkout"
    repo.mkdir()
    _git_init(repo)
    (repo / "README.md").write_text("pre-r07\n")
    _git(repo, "add", "-A")
    _git(
        repo,
        "-c",
        "user.email=test@example.invalid",
        "-c",
        "user.name=test",
        "commit",
        "--quiet",
        "-m",
        "pre-r07",
    )
    commit = _git(repo, "rev-parse", "HEAD")

    state, policy = r07_deploy_evidence.read_policy_state(
        runner=_RealGitRunner(repo),
        git_path=Path("/usr/bin/git"),
        commit_sha=commit,
        role="installed",
    )

    assert type(state) is r07_deploy_evidence.InstalledPolicyState
    assert state.deployment_mode is None
    assert state.policy_digest is None
    assert policy is None


def _commit_all(repo: Path, message: str) -> str:
    _git(repo, "add", "-A")
    _git(
        repo,
        "-c",
        "user.email=test@example.invalid",
        "-c",
        "user.name=test",
        "commit",
        "--quiet",
        "-m",
        message,
    )
    return _git(repo, "rev-parse", "HEAD")


def _release_repo(tmp_path: Path) -> tuple[Path, str, str, str]:
    """Build pre-R07, Release A (disabled), and Release B (enforced) commits."""

    repo = tmp_path / "production"
    fixture_dir = repo / "tests" / "fixtures" / "r07_differential_gate"
    fixture_dir.mkdir(parents=True)
    _git_init(repo)
    (repo / "README.md").write_text("pre-r07\n")
    pre_r07 = _commit_all(repo, "pre-r07")
    policy_path = fixture_dir / "policy-v1.json"
    policy_path.write_bytes(POLICY_PATH.read_bytes())
    release_a = _commit_all(repo, "release a")
    release_a_tree = _git(repo, "rev-parse", "--verify", f"{release_a}^{{tree}}")
    policy_path.write_bytes(_enforced_policy_bytes(release_a, release_a_tree))
    release_b = _commit_all(repo, "release b")
    return repo, pre_r07, release_a, release_b


def _target_policy_of(repo: Path, commit_sha: str) -> R07PolicyV1:
    raw = r07_deploy_evidence.read_policy_blob(
        runner=_RealGitRunner(repo),
        git_path=Path("/usr/bin/git"),
        commit_sha=commit_sha,
    )
    assert raw is not None
    return r07_deploy_evidence.parse_policy_blob(raw)


def _gate_responses(repo: Path, commit_sha: str) -> dict[str, bytes]:
    tree_sha = _git(repo, "rev-parse", "--verify", f"{commit_sha}^{{tree}}")
    payload = _valid_wire_bytes(commit_sha, tree_sha, policy=_target_policy_of(repo, commit_sha))
    archive_url = "https://api.github.com/artifact/91/zip"
    return {
        r07_deploy_evidence.workflow_runs_url(commit_sha): json.dumps(
            {"workflow_runs": [_run_payload(head_sha=commit_sha)]}
        ).encode(),
        r07_deploy_evidence.run_artifacts_url(RUN_ID): json.dumps(
            {"artifacts": [_artifact_payload(name=f"r07-dr-gate-{commit_sha}")]}
        ).encode(),
        archive_url: _artifact_zip({"r07-dr-gate/evidence-v1.json": payload}),
    }


def _gate(
    tmp_path: Path,
    *,
    responses: dict[str, bytes] | None = None,
    verifier: object | None = None,
    cache_dir: Path | None = None,
    require_declared_cache_path: bool = False,
    cache_trust: object | None = None,
) -> tuple[object, _FakeTransport, object]:
    transport = _FakeTransport(responses or {})
    resolved_verifier = verifier or _FakeVerifier()
    gate = r07_deploy_evidence.R07DeployEvidenceGate(
        cache_dir=cache_dir or tmp_path / "var" / "r07-dr-evidence",
        transport=transport,
        token_provider=_FakeTokenProvider(),
        clock=lambda: 0.0,
        verifier=resolved_verifier,
        require_declared_cache_path=require_declared_cache_path,
        cache_trust=cache_trust or _lab_trust(tmp_path),
    )
    return gate, transport, resolved_verifier


class _OldGitRunner(_RealGitRunner):
    """A deployment host whose Git predates ``merge-tree --write-tree``."""

    def run(
        self,
        args: list[str],
        *,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        if args[1:] == ["--version"]:
            return subprocess.CompletedProcess(args, 0, stdout="git version 2.34.1\n", stderr="")
        return super().run(args, check=check)


def test_enforced_target_is_refused_when_git_cannot_replay_merge_provenance(
    tmp_path: Path,
) -> None:
    repo, _pre_r07, release_a, release_b = _release_repo(tmp_path)
    gate, transport, verifier = _gate(tmp_path, responses=_gate_responses(repo, release_b))

    decision = gate.evaluate(
        repo=repo,
        runner=_OldGitRunner(repo),
        git_path=Path("/usr/bin/git"),
        installed_commit_sha=release_a,
        target_commit_sha=release_b,
    )

    assert not decision.allowed
    assert decision.gate == "rejected"
    assert "merge provenance" in decision.reason
    assert transport.requests == []
    assert verifier.calls == []


def test_git_version_preflight_accepts_only_a_merge_tree_capable_git(tmp_path: Path) -> None:
    repo, _pre_r07, _release_a, _release_b = _release_repo(tmp_path)

    assert r07_deploy_evidence.require_merge_tree_capable_git(
        runner=_RealGitRunner(repo),
        git_path=Path("/usr/bin/git"),
    )[:2] >= (2, 38)

    with pytest.raises(r07_deploy_evidence.R07EvidenceError, match="merge provenance"):
        r07_deploy_evidence.require_merge_tree_capable_git(
            runner=_OldGitRunner(repo),
            git_path=Path("/usr/bin/git"),
        )


def test_pinned_gate_rejects_a_cache_directory_the_policy_never_declared(
    tmp_path: Path,
) -> None:
    repo, _pre_r07, release_a, release_b = _release_repo(tmp_path)
    gate, transport, verifier = _gate(
        tmp_path,
        responses=_gate_responses(repo, release_b),
        require_declared_cache_path=True,
    )

    decision = gate.evaluate(
        repo=repo,
        runner=_RealGitRunner(repo),
        git_path=Path("/usr/bin/git"),
        installed_commit_sha=release_a,
        target_commit_sha=release_b,
    )

    assert decision.allowed is False
    assert decision.gate == "rejected"
    assert r07_deploy_evidence.LINUX_PRODUCTION_EVIDENCE_CACHE_DIR.name in decision.reason
    assert transport.requests == []
    assert verifier.calls == []
    assert not (tmp_path / "var" / "r07-dr-evidence").exists()


def test_pinned_gate_rejects_an_undeclared_cache_directory_on_release_a_too(
    tmp_path: Path,
) -> None:
    """Release A never reads the cache, but a drifted path is a defect before it matters."""

    repo, pre_r07, release_a, _release_b = _release_repo(tmp_path)
    gate, _transport, _verifier = _gate(tmp_path, require_declared_cache_path=True)

    decision = gate.evaluate(
        repo=repo,
        runner=_RealGitRunner(repo),
        git_path=Path("/usr/bin/git"),
        installed_commit_sha=pre_r07,
        target_commit_sha=release_a,
    )

    assert decision.allowed is False
    assert decision.gate == "rejected"


def test_pinned_gate_accepts_exactly_the_declared_cache_directory(tmp_path: Path) -> None:
    repo, pre_r07, release_a, _release_b = _release_repo(tmp_path)
    declared = Path(_target_policy_of(repo, release_a).evidence_channel.cache_path)
    gate, transport, verifier = _gate(
        tmp_path,
        cache_dir=declared,
        require_declared_cache_path=True,
    )

    decision = gate.evaluate(
        repo=repo,
        runner=_RealGitRunner(repo),
        git_path=Path("/usr/bin/git"),
        installed_commit_sha=pre_r07,
        target_commit_sha=release_a,
    )

    assert decision.allowed is True
    assert decision.gate == "bootstrap_disabled"
    assert transport.requests == []
    assert verifier.calls == []
    assert not declared.exists()


def test_an_unpinned_gate_still_uses_the_lab_cache_directory(tmp_path: Path) -> None:
    repo, _pre_r07, release_a, release_b = _release_repo(tmp_path)
    gate, _transport, verifier = _gate(tmp_path, responses=_gate_responses(repo, release_b))

    decision = gate.evaluate(
        repo=repo,
        runner=_RealGitRunner(repo),
        git_path=Path("/usr/bin/git"),
        installed_commit_sha=release_a,
        target_commit_sha=release_b,
    )

    assert decision.allowed is True
    assert verifier.calls
    assert (tmp_path / "var" / "r07-dr-evidence").exists()


def test_gate_installs_release_a_once_without_touching_the_evidence_channel(
    tmp_path: Path,
) -> None:
    repo, pre_r07, release_a, _release_b = _release_repo(tmp_path)
    gate, transport, verifier = _gate(tmp_path)

    decision = gate.evaluate(
        repo=repo,
        runner=_RealGitRunner(repo),
        git_path=Path("/usr/bin/git"),
        installed_commit_sha=pre_r07,
        target_commit_sha=release_a,
    )

    assert decision.allowed is True
    assert decision.gate == "bootstrap_disabled"
    assert transport.requests == []
    assert verifier.calls == []
    assert not (tmp_path / "var" / "r07-dr-evidence").exists()


def test_gate_verifies_release_b_evidence_before_and_after_the_cache_write(
    tmp_path: Path,
) -> None:
    repo, _pre_r07, release_a, release_b = _release_repo(tmp_path)
    gate, transport, verifier = _gate(tmp_path, responses=_gate_responses(repo, release_b))

    decision = gate.evaluate(
        repo=repo,
        runner=_RealGitRunner(repo),
        git_path=Path("/usr/bin/git"),
        installed_commit_sha=release_a,
        target_commit_sha=release_b,
    )

    assert decision.allowed is True
    assert decision.gate == "enforced"
    assert verifier.calls == [release_b, release_b]
    assert (tmp_path / "var" / "r07-dr-evidence" / f"{release_b}.json").is_file()
    assert len(transport.requests) == 3


def test_gate_reuses_a_retained_cache_entry_but_never_its_run_identity_claim(
    tmp_path: Path,
) -> None:
    """The retained entry saves the artifact download; the identity question is asked again."""

    repo, _pre_r07, release_a, release_b = _release_repo(tmp_path)
    gate, transport, verifier = _gate(tmp_path, responses=_gate_responses(repo, release_b))
    gate.evaluate(
        repo=repo,
        runner=_RealGitRunner(repo),
        git_path=Path("/usr/bin/git"),
        installed_commit_sha=release_a,
        target_commit_sha=release_b,
    )
    transport.requests.clear()
    verifier.calls.clear()

    decision = gate.evaluate(
        repo=repo,
        runner=_RealGitRunner(repo),
        git_path=Path("/usr/bin/git"),
        installed_commit_sha=release_a,
        target_commit_sha=release_b,
    )

    assert decision.allowed is True
    assert transport.requests == [r07_deploy_evidence.workflow_runs_url(release_b)]
    assert verifier.calls == [release_b]


def test_gate_never_caches_evidence_the_private_verifier_rejects(tmp_path: Path) -> None:
    repo, _pre_r07, release_a, release_b = _release_repo(tmp_path)
    gate, _transport, _verifier = _gate(
        tmp_path,
        responses=_gate_responses(repo, release_b),
        verifier=_RejectingVerifier(),
    )

    decision = gate.evaluate(
        repo=repo,
        runner=_RealGitRunner(repo),
        git_path=Path("/usr/bin/git"),
        installed_commit_sha=release_a,
        target_commit_sha=release_b,
    )

    assert decision.allowed is False
    assert decision.gate == "rejected"
    assert not (tmp_path / "var" / "r07-dr-evidence" / f"{release_b}.json").exists()


def test_gate_rejects_a_second_bootstrap_disabled_target(tmp_path: Path) -> None:
    repo, _pre_r07, release_a, _release_b = _release_repo(tmp_path)
    gate, transport, verifier = _gate(tmp_path)

    decision = gate.evaluate(
        repo=repo,
        runner=_RealGitRunner(repo),
        git_path=Path("/usr/bin/git"),
        installed_commit_sha=release_a,
        target_commit_sha=release_a,
    )

    assert decision.allowed is False
    assert "bootstrap" in decision.reason
    assert transport.requests == []
    assert verifier.calls == []


def test_gate_rejects_an_unavailable_evidence_channel(tmp_path: Path) -> None:
    repo, _pre_r07, release_a, release_b = _release_repo(tmp_path)
    gate, _transport, verifier = _gate(tmp_path)

    decision = gate.evaluate(
        repo=repo,
        runner=_RealGitRunner(repo),
        git_path=Path("/usr/bin/git"),
        installed_commit_sha=release_a,
        target_commit_sha=release_b,
    )

    assert decision.allowed is False
    assert decision.gate == "rejected"
    assert verifier.calls == []


def test_gate_default_verifier_is_the_single_private_verifier() -> None:
    assert r07_deploy_evidence.DEFAULT_EVIDENCE_VERIFIER is differential_gate.verify_wire


# ---------------------------------------------------------------------------------------
# Cache trust boundary (Codex round-2 order 2026-08-25, item P1-2)
# ---------------------------------------------------------------------------------------


def _run_identity_responses(
    commit_sha: str,
    *,
    runs: list[dict[str, object]] | None = None,
) -> dict[str, bytes]:
    """Only the workflow-runs query: a cache hit never downloads the artifact again."""

    payload = [_run_payload(head_sha=commit_sha)] if runs is None else runs
    return {
        r07_deploy_evidence.workflow_runs_url(commit_sha): json.dumps(
            {"total_count": len(payload), "workflow_runs": payload}
        ).encode()
    }


def _plant_cache_entry(
    repo: Path,
    tmp_path: Path,
    commit_sha: str,
    *,
    workflow_run_id: int = RUN_ID,
    run_attempt: int = 2,
    cache_dir: Path | None = None,
) -> Path:
    """Write a byte-legal, digest-consistent entry the way a deploy-user attacker could."""

    directory = cache_dir or tmp_path / "var" / "r07-dr-evidence"
    tree_sha = _git(repo, "rev-parse", "--verify", f"{commit_sha}^{{tree}}")
    return r07_deploy_evidence.write_cached_evidence(
        cache_dir=directory,
        commit_sha=commit_sha,
        payload=_valid_wire_bytes(
            commit_sha,
            tree_sha,
            policy=_target_policy_of(repo, commit_sha),
            workflow_run_id=workflow_run_id,
            run_attempt=run_attempt,
        ),
    )


def _evaluate_release_b(
    gate: object,
    repo: Path,
    release_a: str,
    release_b: str,
) -> object:
    return gate.evaluate(
        repo=repo,
        runner=_RealGitRunner(repo),
        git_path=Path("/usr/bin/git"),
        installed_commit_sha=release_a,
        target_commit_sha=release_b,
    )


def test_planted_cache_entry_is_refused_when_github_cannot_confirm_the_run(
    tmp_path: Path,
) -> None:
    """The headline P1-2 red test: a plausible planted entry is not evidence of a CI run."""

    repo, _pre_r07, release_a, release_b = _release_repo(tmp_path)
    _plant_cache_entry(repo, tmp_path, release_b)
    gate, transport, verifier = _gate(tmp_path, responses={})

    decision = _evaluate_release_b(gate, repo, release_a, release_b)

    assert decision.allowed is False
    assert decision.gate == "rejected"
    assert transport.requests == [r07_deploy_evidence.workflow_runs_url(release_b)]
    assert verifier.calls == []


def test_planted_cache_entry_naming_an_unknown_run_identity_is_overwritten(
    tmp_path: Path,
) -> None:
    """A disagreeing entry is stale bytes, not a verdict: the channel simply re-supplies them."""

    repo, _pre_r07, release_a, release_b = _release_repo(tmp_path)
    entry = _plant_cache_entry(repo, tmp_path, release_b, workflow_run_id=777_001)
    planted = entry.read_bytes()
    gate, _transport, verifier = _gate(tmp_path, responses=_gate_responses(repo, release_b))

    decision = _evaluate_release_b(gate, repo, release_a, release_b)

    assert decision.allowed is True
    assert decision.gate == "enforced"
    retained = entry.read_bytes()
    assert retained != planted
    assert json.loads(retained)["workflow_run_id"] == RUN_ID
    assert verifier.calls == [release_b, release_b]


def test_planted_cache_entry_naming_an_unknown_run_identity_blocks_without_an_artifact(
    tmp_path: Path,
) -> None:
    """Falling back to the download path is not a way through when the channel cannot supply."""

    repo, _pre_r07, release_a, release_b = _release_repo(tmp_path)
    _plant_cache_entry(repo, tmp_path, release_b, workflow_run_id=777_001)
    gate, _transport, verifier = _gate(
        tmp_path,
        responses=_run_identity_responses(release_b),
    )

    decision = _evaluate_release_b(gate, repo, release_a, release_b)

    assert decision.allowed is False
    assert decision.gate == "rejected"
    assert verifier.calls == []


def test_a_superseded_cache_attempt_is_replaced_by_a_fresh_download(tmp_path: Path) -> None:
    """`Re-run all jobs` supersedes attempt 1; the entry written for it must not brick rollback.

    GitHub's run list reports only the current attempt, so attempt 1 becomes invisible and the
    retained entry can never match again. Treating that as a miss re-downloads the evidence the
    channel resolves now and atomically replaces the entry, leaving no stale bytes behind.
    """

    repo, _pre_r07, release_a, release_b = _release_repo(tmp_path)
    entry = _plant_cache_entry(repo, tmp_path, release_b, run_attempt=1)
    superseded = entry.read_bytes()
    assert json.loads(superseded)["run_attempt"] == 1
    gate, transport, verifier = _gate(tmp_path, responses=_gate_responses(repo, release_b))

    decision = _evaluate_release_b(gate, repo, release_a, release_b)

    assert decision.allowed is True
    assert decision.gate == "enforced"
    retained = entry.read_bytes()
    assert retained != superseded
    assert json.loads(retained)["run_attempt"] == 2
    assert [path.name for path in sorted(entry.parent.iterdir())] == [f"{release_b}.json"]
    assert verifier.calls == [release_b, release_b]
    assert transport.requests == [
        r07_deploy_evidence.workflow_runs_url(release_b),
        r07_deploy_evidence.workflow_runs_url(release_b),
        r07_deploy_evidence.run_artifacts_url(RUN_ID),
        "https://api.github.com/artifact/91/zip",
    ]


def test_a_superseded_cache_attempt_still_blocks_when_the_artifact_has_expired(
    tmp_path: Path,
) -> None:
    repo, _pre_r07, release_a, release_b = _release_repo(tmp_path)
    entry = _plant_cache_entry(repo, tmp_path, release_b, run_attempt=1)
    superseded = entry.read_bytes()
    gate, _transport, verifier = _gate(
        tmp_path,
        responses=_run_identity_responses(release_b),
    )

    decision = _evaluate_release_b(gate, repo, release_a, release_b)

    assert decision.allowed is False
    assert decision.gate == "rejected"
    assert entry.read_bytes() == superseded
    assert verifier.calls == []


def test_cache_hit_is_blocked_when_the_target_has_no_single_push_main_run(
    tmp_path: Path,
) -> None:
    repo, _pre_r07, release_a, release_b = _release_repo(tmp_path)
    _plant_cache_entry(repo, tmp_path, release_b)
    gate, _transport, verifier = _gate(
        tmp_path,
        responses=_run_identity_responses(
            release_b,
            runs=[
                _run_payload(head_sha=release_b, id=RUN_ID),
                _run_payload(head_sha=release_b, id=RUN_ID + 1),
            ],
        ),
    )

    decision = _evaluate_release_b(gate, repo, release_a, release_b)

    assert decision.allowed is False
    assert decision.gate == "rejected"
    assert verifier.calls == []


def test_cache_hit_reverifies_the_run_identity_and_skips_only_the_artifact_download(
    tmp_path: Path,
) -> None:
    """The control: a retained entry whose run identity still confirms is accepted."""

    repo, _pre_r07, release_a, release_b = _release_repo(tmp_path)
    _plant_cache_entry(repo, tmp_path, release_b)
    gate, transport, verifier = _gate(
        tmp_path,
        responses=_run_identity_responses(release_b),
    )

    decision = _evaluate_release_b(gate, repo, release_a, release_b)

    assert decision.allowed is True
    assert decision.gate == "enforced"
    assert transport.requests == [r07_deploy_evidence.workflow_runs_url(release_b)]
    assert verifier.calls == [release_b]


def test_cache_entry_reached_through_a_symlinked_ancestor_is_refused(tmp_path: Path) -> None:
    """``O_NOFOLLOW`` on the final component never covered a swapped parent directory."""

    repo, _pre_r07, release_a, release_b = _release_repo(tmp_path)
    real = tmp_path / "real"
    real.mkdir()
    (tmp_path / "var").symlink_to(real, target_is_directory=True)
    _plant_cache_entry(repo, tmp_path, release_b)
    gate, _transport, verifier = _gate(
        tmp_path,
        responses=_run_identity_responses(release_b),
    )

    decision = _evaluate_release_b(gate, repo, release_a, release_b)

    assert decision.allowed is False
    assert decision.gate == "rejected"
    assert verifier.calls == []


def test_group_writable_cache_entry_is_refused(tmp_path: Path) -> None:
    repo, _pre_r07, release_a, release_b = _release_repo(tmp_path)
    entry = _plant_cache_entry(repo, tmp_path, release_b)
    entry.chmod(0o664)
    gate, _transport, verifier = _gate(
        tmp_path,
        responses=_run_identity_responses(release_b),
    )

    decision = _evaluate_release_b(gate, repo, release_a, release_b)

    assert decision.allowed is False
    assert decision.gate == "rejected"
    assert verifier.calls == []


def test_group_writable_cache_directory_is_refused(tmp_path: Path) -> None:
    repo, _pre_r07, release_a, release_b = _release_repo(tmp_path)
    entry = _plant_cache_entry(repo, tmp_path, release_b)
    entry.parent.chmod(0o775)
    gate, _transport, verifier = _gate(
        tmp_path,
        responses=_run_identity_responses(release_b),
    )

    decision = _evaluate_release_b(gate, repo, release_a, release_b)

    assert decision.allowed is False
    assert decision.gate == "rejected"
    assert verifier.calls == []


def test_hard_linked_cache_entry_is_refused(tmp_path: Path) -> None:
    repo, _pre_r07, release_a, release_b = _release_repo(tmp_path)
    entry = _plant_cache_entry(repo, tmp_path, release_b)
    os.link(entry, tmp_path / "second-name.json")
    gate, _transport, verifier = _gate(
        tmp_path,
        responses=_run_identity_responses(release_b),
    )

    decision = _evaluate_release_b(gate, repo, release_a, release_b)

    assert decision.allowed is False
    assert decision.gate == "rejected"
    assert verifier.calls == []


def test_cache_entry_owned_by_another_identity_is_refused(tmp_path: Path) -> None:
    """The expected owner is configurable so a root-owned cache is a value change, not a fork.

    What refuses here is the *ancestor* owner predicate: `expected_uid` also seeds
    `allowed_ancestor_uids`, and the cache directory's real owner is not in that set once the
    expected identity moves. The final file's own `allowed_final_uids` is not what this case
    exercises and has no direct coverage - reaching it needs an entry owned by someone else
    under an ancestor chain that is still trusted, which cannot be built without root.
    """

    directory = _cache_dir(tmp_path)
    policy = _policy()
    r07_deploy_evidence.write_cached_evidence(
        cache_dir=directory,
        commit_sha=COMMIT_B,
        payload=_valid_wire_bytes(COMMIT_B, TREE_B, policy=policy),
    )
    foreign = r07_deploy_evidence.EvidenceCacheTrustV1(
        trusted_root=tmp_path,
        expected_uid=os.geteuid() + 1,
        expected_gids=(os.getegid(), 0),
        allow_sticky_ancestors=False,
    )

    with pytest.raises(r07_deploy_evidence.R07EvidenceError, match="trusted regular file"):
        r07_deploy_evidence.read_cached_evidence(
            cache_dir=directory,
            commit_sha=COMMIT_B,
            tree_sha=TREE_B,
            policy=policy,
            run_identity=_identity(),
            trust=foreign,
        )


def test_cache_entry_larger_than_one_plausible_wire_is_refused(tmp_path: Path) -> None:
    directory = _cache_dir(tmp_path)
    policy = _policy()
    entry = directory / f"{COMMIT_B}.json"
    entry.write_bytes(_valid_wire_bytes(COMMIT_B, TREE_B, policy=policy) + b" " * (64 * 1024))
    entry.chmod(0o644)

    assert entry.stat().st_size > 64 * 1024
    with pytest.raises(r07_deploy_evidence.R07EvidenceError, match="trusted regular file"):
        r07_deploy_evidence.read_cached_evidence(
            cache_dir=directory,
            commit_sha=COMMIT_B,
            tree_sha=TREE_B,
            policy=policy,
            run_identity=_identity(),
            trust=_lab_trust(tmp_path),
        )


def test_a_sticky_ancestor_is_tolerated_only_under_an_explicit_lab_root(tmp_path: Path) -> None:
    sticky = tmp_path / "sticky"
    sticky.mkdir()
    directory = sticky / "r07-dr-evidence"
    policy = _policy()
    r07_deploy_evidence.write_cached_evidence(
        cache_dir=directory,
        commit_sha=COMMIT_B,
        payload=_valid_wire_bytes(COMMIT_B, TREE_B, policy=policy),
    )
    sticky.chmod(0o1777)

    with pytest.raises(r07_deploy_evidence.R07EvidenceError, match="trusted regular file"):
        r07_deploy_evidence.read_cached_evidence(
            cache_dir=directory,
            commit_sha=COMMIT_B,
            tree_sha=TREE_B,
            policy=policy,
            run_identity=_identity(),
            trust=_lab_trust(tmp_path),
        )

    wire = r07_deploy_evidence.read_cached_evidence(
        cache_dir=directory,
        commit_sha=COMMIT_B,
        tree_sha=TREE_B,
        policy=policy,
        run_identity=_identity(),
        trust=_lab_trust(tmp_path, allow_sticky_ancestors=True),
    )

    assert wire is not None
    assert wire.candidate_commit_sha == COMMIT_B


def test_a_world_writable_ancestor_without_the_sticky_bit_is_never_tolerated(
    tmp_path: Path,
) -> None:
    loose = tmp_path / "loose"
    loose.mkdir()
    directory = loose / "r07-dr-evidence"
    policy = _policy()
    r07_deploy_evidence.write_cached_evidence(
        cache_dir=directory,
        commit_sha=COMMIT_B,
        payload=_valid_wire_bytes(COMMIT_B, TREE_B, policy=policy),
    )
    loose.chmod(0o777)

    for trust in (_lab_trust(tmp_path), _lab_trust(tmp_path, allow_sticky_ancestors=True)):
        with pytest.raises(r07_deploy_evidence.R07EvidenceError, match="trusted regular file"):
            r07_deploy_evidence.read_cached_evidence(
                cache_dir=directory,
                commit_sha=COMMIT_B,
                tree_sha=TREE_B,
                policy=policy,
                run_identity=_identity(),
                trust=trust,
            )


def test_linux_production_cache_trust_never_tolerates_a_sticky_ancestor() -> None:
    with pytest.raises(ValidationError, match="sticky"):
        r07_deploy_evidence.EvidenceCacheTrustV1(
            trusted_root=Path("/"),
            expected_uid=os.geteuid(),
            expected_gids=(os.getegid(),),
            allow_sticky_ancestors=True,
        )


def test_the_default_deployment_cache_trust_is_the_process_identity_rooted_at_slash() -> None:
    trust = r07_deploy_evidence.EvidenceCacheTrustV1.for_deployment_process()

    assert trust.trusted_root == Path("/")
    assert trust.expected_uid == os.geteuid()
    assert trust.allow_sticky_ancestors is False


def test_the_cache_trusted_root_must_be_absolute_and_canonical(tmp_path: Path) -> None:
    for root in (Path("relative/root"), tmp_path / ".." / tmp_path.name):
        with pytest.raises(ValidationError, match="canonical"):
            r07_deploy_evidence.EvidenceCacheTrustV1(
                trusted_root=root,
                expected_uid=os.geteuid(),
                expected_gids=(os.getegid(),),
                allow_sticky_ancestors=False,
            )


def test_the_gate_defaults_to_the_production_cache_trust(tmp_path: Path) -> None:
    gate = r07_deploy_evidence.R07DeployEvidenceGate(
        cache_dir=tmp_path / "var" / "r07-dr-evidence",
        transport=_FakeTransport({}),
        token_provider=_FakeTokenProvider(),
    )

    assert gate.cache_trust == r07_deploy_evidence.EvidenceCacheTrustV1.for_deployment_process()


def test_the_cache_write_leaves_no_group_writable_ancestor_behind(tmp_path: Path) -> None:
    """The read walk refuses a group-writable parent, so the writer may not create one."""

    directory = tmp_path / "nested" / "deeper" / "r07-dr-evidence"
    previous = os.umask(0o000)
    try:
        r07_deploy_evidence.write_cached_evidence(
            cache_dir=directory,
            commit_sha=COMMIT_B,
            payload=_valid_wire_bytes(COMMIT_B, TREE_B, policy=_policy()),
        )
    finally:
        os.umask(previous)

    for created in (tmp_path / "nested", tmp_path / "nested" / "deeper", directory):
        assert stat.S_IMODE(created.lstat().st_mode) == 0o755


# ---------------------------------------------------------------------------------------
# The other half of the degradation boundary: only staleness may become a cache miss
# ---------------------------------------------------------------------------------------


def _plant_cache_bytes(tmp_path: Path, commit_sha: str, payload: bytes) -> Path:
    """Write arbitrary bytes through the real writer, so modes and atomicity are the real ones."""

    return r07_deploy_evidence.write_cached_evidence(
        cache_dir=tmp_path / "var" / "r07-dr-evidence",
        commit_sha=commit_sha,
        payload=payload,
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        pytest.param("candidate_tree_sha", "0" * 40, id="tree"),
        pytest.param("policy_digest", "1" * 64, id="policy-digest"),
    ],
)
def test_a_wrongly_bound_cache_entry_blocks_and_is_never_redownloaded(
    tmp_path: Path,
    field: str,
    value: str,
) -> None:
    """Everything the channel *can* answer is available here, and it still must not be asked.

    A retained entry that is not bound to this target says the deployment host is not what it
    should be. Unlike a superseded attempt, that is not staleness, so it blocks where it stands
    instead of being quietly repaired by a download.

    Rewriting either field in place leaves `candidate_binding_digest` and `evidence_digest`
    disagreeing with it, so the refusal comes from the wire model's own validator, one layer
    ahead of the gate's binding comparison. Both layers are fail-closed and the observable
    outcome under test - blocked, entry untouched, no artifact fetched - is the same either
    way; the gate-layer comparison itself is pinned by the run-identity cases.
    """

    repo, _pre_r07, release_a, release_b = _release_repo(tmp_path)
    tree_sha = _git(repo, "rev-parse", "--verify", f"{release_b}^{{tree}}")
    payload = json.loads(
        _valid_wire_bytes(release_b, tree_sha, policy=_target_policy_of(repo, release_b))
    )
    payload[field] = value
    entry = _plant_cache_bytes(
        tmp_path,
        release_b,
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(),
    )
    planted = entry.read_bytes()
    gate, transport, verifier = _gate(tmp_path, responses=_gate_responses(repo, release_b))

    decision = _evaluate_release_b(gate, repo, release_a, release_b)

    assert decision.allowed is False
    assert decision.gate == "rejected"
    assert entry.read_bytes() == planted
    assert verifier.calls == []
    assert transport.requests == [r07_deploy_evidence.workflow_runs_url(release_b)]


def test_a_filesystem_noncompliant_cache_entry_blocks_even_when_the_channel_could_supply(
    tmp_path: Path,
) -> None:
    """A group-writable entry is refused before the channel is asked anything at all."""

    repo, _pre_r07, release_a, release_b = _release_repo(tmp_path)
    entry = _plant_cache_entry(repo, tmp_path, release_b)
    entry.chmod(0o664)
    planted = entry.read_bytes()
    gate, transport, verifier = _gate(tmp_path, responses=_gate_responses(repo, release_b))

    decision = _evaluate_release_b(gate, repo, release_a, release_b)

    assert decision.allowed is False
    assert decision.gate == "rejected"
    assert entry.read_bytes() == planted
    assert stat.S_IMODE(entry.lstat().st_mode) == 0o664
    assert verifier.calls == []
    assert transport.requests == []
