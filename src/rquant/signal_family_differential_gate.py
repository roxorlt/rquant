"""Pure, bounded R07 differential-gate contracts.

This module deliberately operates on checked-in source text and declared fixtures. It never
imports production builder modules, constructs a registry, or follows object references.
"""

from __future__ import annotations

import ast
import base64
import copy
import hashlib
import importlib.util
import io
import json
import os
import stat
import subprocess
import tarfile
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from typing import Any, Literal, Self, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    StrictStr,
    ValidationError,
    model_validator,
)

from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads

# The frozen baseline is the merge base of the two commits an R07 run states, so
# baseline..candidate is the complete pull request merge face rather than a branch-local
# subset. It is refrozen by each pull request to the merge commit its predecessor left on
# main; the deployer argv topic moves it from PR #180's merge commit to PR #183's,
# which is what origin/main is when this candidate is cut. Which two commits
# meet here is decided by resolve_baseline_context below, from explicit endpoints; it used to
# be rediscovered as merge_base(origin/main, candidate), and that stopped having an answer the
# moment the merge that froze it landed, because origin/main then *was* the candidate.
# Refreezing this pair and regenerating the policy is the last commit of every pull request.
BASELINE_COMMIT_SHA = "444728a5613cb7514c858151504677ef5cececd9"
BASELINE_TREE_SHA = "38f56b93aebdf1126e11347b5d98434b1b238978"
# The pre-amendment baseline. It is not an origin/main ancestor, so it can no longer anchor
# the diff, but the Git hard constraint requires it to stay reachable from every candidate.
HISTORICAL_BASELINE_COMMIT_SHA = "45d0b57c4c5cbab1700fa5e3c386c6756892a7d6"
# ``git merge-tree --write-tree`` is the offline merge-provenance replay; it landed in 2.38.
MINIMUM_MERGE_TREE_GIT_VERSION = (2, 38)
POLICY_RELATIVE_PATH = "tests/fixtures/r07_differential_gate/policy-v1.json"
R07_EVIDENCE_CACHE_DIR = "/home/lighthouse/rquant/var/r07-dr-evidence"
BOUNDARY_PROBE_COUNT = 17
FIXED_STATIC_CHECK_NAMES = (
    "policy-completeness",
    "top-level-source-closure",
    "forbidden-definitions",
    "diff-scope-forbidden-definitions",
)
EVIDENCE_CHANNEL_JOBS = (
    "r07-differential-gate-py311",
    "r07-differential-gate-py312",
    "r07-differential-gate-evidence",
)
_SHA256 = r"^[0-9a-f]{64}$"
_SHA1 = r"^[0-9a-f]{40}$"
R07_CI_EVIDENCE_PRODUCER_IMPLEMENTED = True
_VERIFIED_CONSTRUCTION_TOKEN = object()

_EMPTY_VERSION_VARIANT_AST_FIELDS = {
    "FunctionDef": ("type_params",),
    "AsyncFunctionDef": ("type_params",),
    "ClassDef": ("type_params",),
}


def normalized_ast_dump(node: ast.AST) -> str:
    """Return an AST dump stable across the supported Python 3.11/3.12 parsers."""
    normalized = copy.deepcopy(node)
    for candidate in ast.walk(normalized):
        for field in _EMPTY_VERSION_VARIANT_AST_FIELDS.get(type(candidate).__name__, ()):
            if not hasattr(candidate, field):
                continue
            value = getattr(candidate, field)
            if type(value) is not list or value:
                continue
            candidate._fields = tuple(item for item in candidate._fields if item != field)
            delattr(candidate, field)
    return ast.dump(normalized, annotate_fields=True, include_attributes=False)


def normalized_ast_sha256(node: ast.AST) -> str:
    return hashlib.sha256(normalized_ast_dump(node).encode("utf-8")).hexdigest()


EXPECTED_ROOT_QUALNAMES = (
    "rquant.runtime_service_main.build_builtin_registry",
    "rquant.runtime_service_builtin.build_builtin_registry",
    "rquant.runtime_builder_strategy.strategy_live_builder",
    "rquant.runtime_builder_signal.signal_router_builder",
    "rquant.runtime_builder_signal.notifier_builder",
    "rquant.runtime_builder_shadow.shadow_session_builder",
    "rquant.runtime_builder_paper.paper_consumer_builder",
    "rquant.runtime_builder_paper.paper_broker_builder",
    "rquant.runtime_builder_serving.serving_publisher_builder",
    "rquant.runtime_builder_daily_orchestrator.daily_pipeline_orchestrator_builder",
)
EXPECTED_PRODUCTION_DECLARATIONS = (
    (
        "src/rquant/signal_route_spool.py",
        "CurrentSignalBusRoutedRecord",
        "read_only_v3_model",
    ),
    (
        "src/rquant/signal_route_spool.py",
        "CurrentSignalRouteSpoolRecord",
        "read_only_v3_model",
    ),
    (
        "src/rquant/signal_route_spool.py",
        "decode_current_signal_route_spool_record",
        "read_only_v3_decoder",
    ),
    (
        "src/rquant/signal_route_spool.py",
        "verify_current_signal_route_spool_fixture",
        "read_only_v3_decoder",
    ),
    (
        "src/rquant/signal_bus.py",
        "require_legacy_signal_write",
        "legacy_boundary_reject_guard",
    ),
    (
        "src/rquant/signal_route_spool.py",
        "_require_legacy_spool_publish_input",
        "legacy_boundary_reject_guard",
    ),
    (
        "src/rquant/strategy_runner.py",
        "_legacy_signal_constructor_identity_matches",
        "legacy_boundary_reject_guard",
    ),
)
EXPECTED_FORBIDDEN_SOURCE_FILES = (
    "src/rquant/runtime_builder_daily_orchestrator.py",
    "src/rquant/runtime_builder_paper.py",
    "src/rquant/runtime_builder_serving.py",
    "src/rquant/runtime_builder_shadow.py",
    "src/rquant/runtime_builder_signal.py",
    "src/rquant/runtime_builder_strategy.py",
    "src/rquant/runtime_service_builtin.py",
    "src/rquant/runtime_service_main.py",
    "src/rquant/signal_route_spool.py",
)
EXPECTED_FORBIDDEN_SYMBOLS = (
    "CurrentSignalRouteSpoolWriter",
    "SignalRouteSpoolV3Writer",
    "current_signal_writer",
    "publish_v3",
    "r07_activation",
    "r07_capability",
    "r07_cursor",
    "r07_cutover",
    "r07_drain",
    "r07_environment",
    "r07_flag",
    "r07_migration",
    "r07_overlay",
    "v3_activation",
    "v3_capability",
    "v3_cursor",
    "v3_cutover",
    "v3_drain",
    "v3_environment",
    "v3_flag",
    "v3_migration",
    "v3_overlay",
    "v3_writer",
)
EXPECTED_FORBIDDEN_EXPORTS = (
    "CurrentSignalRouteSpoolWriter",
    "SignalRouteSpoolV3Writer",
    "publish_v3",
)
EXPECTED_FORBIDDEN_REGISTRY_KEYS = (
    "current_signal_route_spool_writer",
    "r07_activation",
    "r07_overlay",
    "signal_route_spool_v3_writer",
    "v3_activation",
    "v3_overlay",
)
EXPECTED_CURRENT_FIXTURE_DECLARATIONS = (
    (
        "current.object",
        "rquant.signal_contracts",
        "CurrentSignalEnvelope",
        "rquant.signal_contracts",
        "parse_signal_envelope",
        "object",
    ),
    (
        "current.stored-bytes",
        "rquant.signal_contracts",
        "CurrentSignalEnvelope",
        "rquant.signal_contracts",
        "parse_signal_envelope",
        "stored_bytes",
    ),
)
_BOUNDARY_BEHAVIOR_FILE = "tests/unit/test_signal_family_no_activation_reset.py"
EXPECTED_BOUNDARY_BEHAVIOR_TESTS = (
    *(
        f"{_BOUNDARY_BEHAVIOR_FILE}::test_r07_dynamic_boundary_probe[R07-B{index:02d}]"
        for index in range(1, 18)
    ),
    "tests/unit/test_signal_family_differential_gate.py::test_root_snapshots_are_static_and_cover_ten_roots",
    "tests/unit/test_signal_family_differential_gate.py::test_forbidden_definition_universe_is_static_source_only",
)


class _StrictModelMixin:
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        str_strip_whitespace=False,
    )


class PythonRunEvidenceV1(_StrictModelMixin, BaseModel):
    python_minor: StrictStr
    job_id: StrictStr
    job_run_id: StrictInt = Field(gt=0)
    workflow_run_id: StrictInt = Field(gt=0)
    run_attempt: StrictInt = Field(gt=0)
    candidate_commit_sha: StrictStr = Field(pattern=_SHA1)
    candidate_tree_sha: StrictStr = Field(pattern=_SHA1)
    collected: StrictInt = Field(gt=0)
    passed: StrictInt = Field(gt=0)
    skipped: StrictInt = Field(ge=0)
    deselected: StrictInt = Field(ge=0)
    # Amended per Codex round-2 order 2026-08-25, ruling 6: each interpreter publishes the
    # exact gate outputs it observed, so a 3.11/3.12 divergence is visible on the wire.
    candidate_gate_digest: StrictStr = Field(pattern=_SHA256)
    static_result_digest: StrictStr = Field(pattern=_SHA256)
    boundary_result_digest: StrictStr = Field(pattern=_SHA256)
    check_inventory_digest: StrictStr = Field(pattern=_SHA256)
    result_digest: StrictStr = Field(pattern=_SHA256)
    outcome: Literal["passed"]

    @model_validator(mode="after")
    def validate_result(self) -> Self:
        if self.collected != self.passed or self.skipped != 0 or self.deselected != 0:
            raise ValueError("R07 Python run must be fully collected and passing")
        return self


def python_run_result_digest(value: PythonRunEvidenceV1) -> str:
    if type(value) is not PythonRunEvidenceV1:
        raise TypeError("R07 Python run requires the exact model")
    return hashlib.sha256(
        canonical_json_bytes(value.model_dump(mode="json", exclude={"result_digest"}))
    ).hexdigest()


PYTHON_RUN_GATE_DIGEST_FIELDS = (
    "candidate_gate_digest",
    "static_result_digest",
    "boundary_result_digest",
    "check_inventory_digest",
)


def parse_git_version(text: str) -> tuple[int, int, int]:
    """Parse ``git --version`` output into an exact comparable triple."""

    if type(text) is not str:
        raise TypeError("Git version output must be text")
    fields = text.strip().split()
    if len(fields) < 3 or fields[0] != "git" or fields[1] != "version":
        raise ValueError("Git version output is not recognizable")
    numbers = fields[2].split(".")[:3]
    if not numbers or any(not part.isdigit() for part in numbers):
        raise ValueError("Git version output is not recognizable")
    triple = tuple(int(part) for part in numbers)
    return (*triple, 0, 0)[:3]  # type: ignore[return-value]


def require_merge_tree_git_version(text: str) -> tuple[int, int, int]:
    """Fail closed unless this Git can write the merge tree the verifier replays."""

    version = parse_git_version(text)
    if version[:2] < MINIMUM_MERGE_TREE_GIT_VERSION:
        raise ValueError(
            "R07 merge provenance requires Git 2.38 or newer for merge-tree --write-tree"
        )
    return version


def _candidate_binding_digest_values(
    *,
    baseline_commit_sha: str,
    baseline_tree_sha: str,
    candidate_commit_sha: str,
    candidate_tree_sha: str,
    complete_diff_digest: str,
) -> str:
    return hashlib.sha256(
        canonical_json_bytes(
            {
                "baseline_commit_sha": baseline_commit_sha,
                "baseline_tree_sha": baseline_tree_sha,
                "candidate_commit_sha": candidate_commit_sha,
                "candidate_tree_sha": candidate_tree_sha,
                "complete_diff_digest": complete_diff_digest,
            }
        )
    ).hexdigest()


class R07DrGateEvidenceWireV1(_StrictModelMixin, BaseModel):
    schema_version: Literal[1]
    repository: Literal["roxorlt/rquant"]
    workflow_path: Literal[".github/workflows/ci.yml"]
    event_name: Literal["push"]
    ref: Literal["refs/heads/main"]
    producer_job_id: Literal["r07-differential-gate-evidence"]
    workflow_run_id: StrictInt = Field(gt=0)
    run_attempt: StrictInt = Field(gt=0)
    candidate_commit_sha: StrictStr = Field(pattern=_SHA1)
    candidate_tree_sha: StrictStr = Field(pattern=_SHA1)
    baseline_commit_sha: Literal[BASELINE_COMMIT_SHA]
    baseline_tree_sha: Literal[BASELINE_TREE_SHA]
    # Amended per Codex round-2 order 2026-08-25, item P1-1 and ruling 9: the wire binds the
    # final merge tree to its exact two parents, so the structure a merge commit must have is
    # replayable offline from Git alone.
    merge_base_commit_sha: StrictStr = Field(pattern=_SHA1)
    merge_base_tree_sha: StrictStr = Field(pattern=_SHA1)
    candidate_parent_commits: tuple[StrictStr, StrictStr]
    merge_tree_sha: StrictStr = Field(pattern=_SHA1)
    policy_digest: StrictStr = Field(pattern=_SHA256)
    complete_diff_digest: StrictStr = Field(pattern=_SHA256)
    candidate_binding_digest: StrictStr = Field(pattern=_SHA256)
    boundary_manifest_digest: StrictStr = Field(pattern=_SHA256)
    boundary_result_digest: StrictStr = Field(pattern=_SHA256)
    root_snapshot_digest: StrictStr = Field(pattern=_SHA256)
    forbidden_definition_digest: StrictStr = Field(pattern=_SHA256)
    python_runs: tuple[PythonRunEvidenceV1, PythonRunEvidenceV1]
    artifact_name: StrictStr
    artifact_json_path: Literal["r07-dr-gate/evidence-v1.json"]
    retention_days: Literal[90]
    outcome: Literal["passed"]
    evidence_digest: StrictStr = Field(pattern=_SHA256)

    @model_validator(mode="after")
    def validate_channel_and_bindings(self) -> Self:
        expected_jobs = ("r07-differential-gate-py311", "r07-differential-gate-py312")
        expected_minors = ("3.11", "3.12")
        for run, minor, job in zip(self.python_runs, expected_minors, expected_jobs, strict=True):
            if (run.python_minor, run.job_id) != (minor, job):
                raise ValueError("R07 Python runs must be the ordered 3.11/3.12 pair")
            if (run.workflow_run_id, run.run_attempt) != (self.workflow_run_id, self.run_attempt):
                raise ValueError("Python run is not bound to the top-level workflow run")
            if (run.candidate_commit_sha, run.candidate_tree_sha) != (
                self.candidate_commit_sha,
                self.candidate_tree_sha,
            ):
                raise ValueError("Python run is not bound to the candidate pair")
        first, second = self.python_runs
        for field in PYTHON_RUN_GATE_DIGEST_FIELDS:
            if getattr(first, field) != getattr(second, field):
                raise ValueError("R07 gate outputs diverge between Python 3.11 and 3.12: " + field)
        if self.boundary_result_digest != first.boundary_result_digest:
            raise ValueError("boundary result digest is not the digest both Python runs observed")
        if self.merge_base_commit_sha != self.baseline_commit_sha:
            raise ValueError("merge base commit is not the frozen baseline commit")
        if self.merge_base_tree_sha != self.baseline_tree_sha:
            raise ValueError("merge base tree is not the frozen baseline tree")
        if any(not _is_lower_hex(parent, length=40) for parent in self.candidate_parent_commits):
            raise ValueError("candidate parents must be lowercase 40-hex commits")
        if self.candidate_parent_commits[0] != self.merge_base_commit_sha:
            raise ValueError("first candidate parent is not the recorded merge base")
        if self.candidate_parent_commits[0] == self.candidate_parent_commits[1]:
            raise ValueError("candidate parents must be two distinct commits")
        if self.merge_tree_sha != self.candidate_tree_sha:
            raise ValueError("merge tree is not the candidate tree")
        if self.artifact_name != f"r07-dr-gate-{self.candidate_commit_sha}":
            raise ValueError("artifact name is not bound to candidate commit")
        expected_binding = _candidate_binding_digest_values(
            baseline_commit_sha=self.baseline_commit_sha,
            baseline_tree_sha=self.baseline_tree_sha,
            candidate_commit_sha=self.candidate_commit_sha,
            candidate_tree_sha=self.candidate_tree_sha,
            complete_diff_digest=self.complete_diff_digest,
        )
        if self.candidate_binding_digest != expected_binding:
            raise ValueError("candidate_binding_digest does not match candidate pair and diff")
        expected = _digest_without_field(self, "evidence_digest")
        if self.evidence_digest != expected:
            raise ValueError("evidence_digest does not match canonical evidence")
        return self

    @classmethod
    def from_canonical_json(cls, payload: bytes | str) -> Self:
        raw = payload.encode() if isinstance(payload, str) else bytes(payload)
        decoded = strict_canonical_json_loads(raw)
        if not isinstance(decoded, dict):
            raise ValueError("R07 evidence must be a JSON object")
        if type(decoded.get("python_runs")) is not list:
            raise ValueError("R07 evidence python_runs must be a JSON array")
        if type(decoded.get("candidate_parent_commits")) is not list:
            raise ValueError("R07 evidence candidate_parent_commits must be a JSON array")
        decoded = {
            **decoded,
            "python_runs": tuple(decoded["python_runs"]),
            "candidate_parent_commits": tuple(decoded["candidate_parent_commits"]),
        }
        return cls.model_validate(decoded)


def _digest_without_field(model: Any, field: str) -> str:
    payload = model.model_dump(mode="json", exclude={field})
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


class VerifiedR07DrGateEvidenceV1:
    """Non-serializable verified typestate created only by the private verifier."""

    __slots__ = ("_wire",)

    def __new__(
        cls,
        *,
        _construction_token: object | None = None,
        _wire: R07DrGateEvidenceWireV1 | None = None,
    ) -> Self:
        if _construction_token is not _VERIFIED_CONSTRUCTION_TOKEN or type(_wire) is not (
            R07DrGateEvidenceWireV1
        ):
            raise TypeError("verified R07 evidence is private verifier output")
        instance = super().__new__(cls)
        object.__setattr__(instance, "_wire", _wire)
        return instance

    def __init__(
        self,
        *,
        _construction_token: object | None = None,
        _wire: R07DrGateEvidenceWireV1 | None = None,
    ) -> None:
        if _construction_token is not _VERIFIED_CONSTRUCTION_TOKEN or type(_wire) is not (
            R07DrGateEvidenceWireV1
        ):
            raise TypeError("verified R07 evidence is private verifier output")

    def __setattr__(self, _name: str, _value: object) -> None:
        raise AttributeError("verified R07 evidence is immutable")

    @property
    def wire(self) -> R07DrGateEvidenceWireV1:
        return self._wire

    def __getattr__(self, name: str) -> object:
        return getattr(self._wire, name)

    def model_dump(self, *, mode: str = "python", **kwargs: object) -> dict[str, object]:
        return self._wire.model_dump(mode=mode, **kwargs)  # type: ignore[arg-type]

    def model_dump_json(self, **kwargs: object) -> str:
        return self._wire.model_dump_json(**kwargs)

    @classmethod
    def from_gate_results(
        cls,
        *,
        repo: Path,
        policy: R07PolicyV1,
        candidate_gate: CandidateGateResult,
        boundary_results: tuple[BoundaryProbeResultV1, ...],
        static_result: R07StaticGateResult,
        python_runs: tuple[PythonRunEvidenceV1, PythonRunEvidenceV1],
        workflow_run_id: int,
        run_attempt: int,
    ) -> Self:
        merge_provenance = resolve_merge_provenance(
            repo,
            candidate_commit=candidate_gate.candidate_commit_sha,
            merge_base_commit=candidate_gate.baseline_commit_sha,
        )
        wire = _wire_from_gate_results(
            policy=policy,
            candidate_gate=candidate_gate,
            boundary_results=boundary_results,
            static_result=static_result,
            python_runs=python_runs,
            merge_provenance=merge_provenance,
            workflow_run_id=workflow_run_id,
            run_attempt=run_attempt,
        )
        return _verify_wire(repo, policy, wire)


def canonical_evidence_json_bytes(
    value: R07DrGateEvidenceWireV1 | VerifiedR07DrGateEvidenceV1,
) -> bytes:
    wire = value.wire if type(value) is VerifiedR07DrGateEvidenceV1 else value
    if type(wire) is not R07DrGateEvidenceWireV1:
        raise TypeError("R07 evidence requires wire or verified evidence")
    if wire.evidence_digest != _digest_without_field(wire, "evidence_digest"):
        raise ValueError("evidence_digest does not match canonical evidence")
    raw = canonical_json_bytes(wire.model_dump(mode="json"))
    if R07DrGateEvidenceWireV1.from_canonical_json(raw) != wire:
        raise ValueError("R07 evidence does not round-trip")
    return raw


class FixtureValueV1(_StrictModelMixin, BaseModel):
    fixture_id: StrictStr = Field(min_length=1)
    kind: Literal["scalar", "bytes", "tuple", "list"]
    value: Any
    sha256: StrictStr = Field(pattern=_SHA256)

    @model_validator(mode="after")
    def validate_shape(self) -> Self:
        if self.kind == "bytes" and type(self.value) is not str:
            raise ValueError("bytes fixture value must be base64 text")
        if self.kind in ("tuple", "list") and (
            type(self.value) is not list or any(type(item) is not str for item in self.value)
        ):
            raise ValueError("composite fixture value must be an ordered list of fixture IDs")
        if self.kind == "scalar" and type(self.value) not in (type(None), str, bool, int, float):
            raise ValueError("scalar fixture value must be an exact JSON scalar")
        return self

    def validate_references(self, *, seen_ids: set[str]) -> None:
        if self.kind not in ("tuple", "list"):
            return
        if self.fixture_id in self.value:
            raise ValueError("composite fixture cycle")
        missing = [item for item in self.value if item not in seen_ids]
        if missing:
            raise ValueError(f"composite fixture must reference prior fixture: {missing[0]}")


def _fixture_value_bytes(value: object) -> bytes:
    if isinstance(value, bytes):
        return value
    return canonical_json_bytes(value)


def _fixture_digest(fixture: FixtureValueV1, resolved: object) -> str:
    if fixture.kind == "bytes":
        decoded = base64.b64decode(fixture.value, validate=True)
        return hashlib.sha256(decoded).hexdigest()
    return hashlib.sha256(_fixture_value_bytes(resolved)).hexdigest()


def strict_fixture_value(fixture: FixtureValueV1) -> FixtureValueV1:
    if fixture.kind in ("tuple", "list"):
        raise ValueError("composite fixture digest requires the complete ordered fixture set")
    if fixture.kind == "bytes":
        try:
            base64.b64decode(fixture.value, validate=True)
        except ValueError as exc:
            raise ValueError("invalid fixture base64") from exc
    if fixture.sha256 != _fixture_digest(fixture, fixture.value):
        raise ValueError("fixture sha256 does not match exact value")
    return fixture


def resolve_fixture_values(
    fixtures: tuple[FixtureValueV1, ...] | list[FixtureValueV1],
) -> dict[str, object]:
    resolved: dict[str, object] = {}
    for fixture in fixtures:
        if fixture.fixture_id in resolved:
            raise ValueError("duplicate fixture_id")
        fixture.validate_references(seen_ids=set(resolved))
        if fixture.kind == "scalar":
            value = fixture.value
        elif fixture.kind == "bytes":
            value = base64.b64decode(fixture.value, validate=True)
        else:
            children = [resolved[item] for item in fixture.value]
            value = tuple(children) if fixture.kind == "tuple" else list(children)
        if fixture.sha256 != _fixture_digest(fixture, value):
            raise ValueError(f"fixture sha256 does not match {fixture.fixture_id}")
        resolved[fixture.fixture_id] = value
    return resolved


class CurrentFixtureV1(_StrictModelMixin, BaseModel):
    fixture_id: StrictStr
    current_model_module: StrictStr
    current_model_qualname: StrictStr
    canonical_model_bytes: StrictStr
    parser_module: StrictStr
    parser_qualname: StrictStr
    parsed_model_digest: StrictStr = Field(pattern=_SHA256)
    allowed_form: Literal["object", "stored_bytes"]

    @model_validator(mode="after")
    def validate_model_bytes(self) -> Self:
        try:
            decoded = base64.b64decode(self.canonical_model_bytes, validate=True)
        except ValueError as exc:
            raise ValueError("current fixture model bytes must be base64") from exc
        if hashlib.sha256(decoded).hexdigest() != self.parsed_model_digest:
            raise ValueError("current fixture parsed_model_digest mismatch")
        return self


class CallShapeV1(_StrictModelMixin, BaseModel):
    receiver_fixture_id: StrictStr | None
    positional_fixture_ids: tuple[StrictStr, ...]
    keyword_fixture_ids: dict[StrictStr, StrictStr]
    call_result_action: Literal["none", "consume_tuple"]

    @model_validator(mode="after")
    def validate_contract(self) -> Self:
        if self.call_result_action not in {"none", "consume_tuple"}:
            raise ValueError("call_result_action must be none or consume_tuple")
        object.__setattr__(
            self, "keyword_fixture_ids", dict(sorted(self.keyword_fixture_ids.items()))
        )
        return self


class ProbeSetupStepV1(_StrictModelMixin, BaseModel):
    kind: Literal[
        "receiver",
        "constructor_replacement",
        "preloaded_store_row",
        "composite_batch",
        "source_result",
    ]
    target: StrictStr
    fixture_ids: tuple[StrictStr, ...] = ()
    expected_binding: StrictStr


class ProbeSetupV1(_StrictModelMixin, BaseModel):
    setup_id: StrictStr
    steps: tuple[ProbeSetupStepV1, ...]
    setup_result_digest: StrictStr = Field(pattern=_SHA256)


class BoundaryReachedSentinelV1(_StrictModelMixin, BaseModel):
    sentinel_id: StrictStr
    inventory_id: StrictStr
    source_span: StrictStr
    ast_digest: StrictStr = Field(pattern=_SHA256)
    reached_count: StrictInt = Field(ge=0)
    mutation_reached_count: StrictInt = Field(ge=0)

    @property
    def passed(self) -> bool:
        return self.reached_count == 1 and self.mutation_reached_count == 0


class ConstructorIdentityFenceSentinelV1(_StrictModelMixin, BaseModel):
    sentinel_id: StrictStr
    inventory_id: StrictStr
    replaced_global: StrictStr
    expected_replacement_identity: StrictStr
    observed_identity: StrictStr
    reached_count: StrictInt = Field(ge=0)

    @property
    def passed(self) -> bool:
        return (
            self.reached_count == 1 and self.expected_replacement_identity == self.observed_identity
        )


class MutationExpectationV1(_StrictModelMixin, BaseModel):
    guard_ids: tuple[StrictStr, ...]
    expected_total_count: Literal[0]
    before_after_equal: Literal[True]

    @model_validator(mode="after")
    def validate_guards(self) -> Self:
        if len(self.guard_ids) != len(set(self.guard_ids)):
            raise ValueError("mutation guard IDs must be unique")
        return self


class BoundaryProbeV1(_StrictModelMixin, BaseModel):
    probe_id: StrictStr
    inventory_id: StrictStr
    variant: Literal[
        "constructor_identity",
        "constructed_current",
        "direct_current",
        "stored_byte_current",
        "batch_current",
        "source_result_current",
        "static_only",
    ]
    setup_id: StrictStr
    entrypoint: StrictStr
    behavior_test: StrictStr
    positional_fixture_ids: tuple[StrictStr, ...]
    keyword_fixture_ids: dict[StrictStr, StrictStr]
    current_member_fixture_ids: tuple[StrictStr, ...]
    call_shape: CallShapeV1
    call_result_action: Literal["none", "consume_tuple"]
    expected_exception: StrictStr
    expected_exception_phase: Literal["invocation", "consumption"]
    sentinel_id: StrictStr
    sentinel_kind: Literal["constructor_identity_fence", "boundary_reached", "static_snapshot"]
    mutation_expectation: MutationExpectationV1
    before_snapshot_digest: StrictStr = Field(pattern=_SHA256)
    after_snapshot_digest: StrictStr = Field(pattern=_SHA256)
    source_span: StrictStr
    boundary_ast_sha256: StrictStr = Field(pattern=_SHA256)
    expected_yielded_count: StrictInt = Field(ge=0)

    @model_validator(mode="after")
    def validate_call_shape(self) -> Self:
        if self.positional_fixture_ids != self.call_shape.positional_fixture_ids:
            raise ValueError("probe positional fixtures do not match call shape")
        if self.keyword_fixture_ids != self.call_shape.keyword_fixture_ids:
            raise ValueError("probe keyword fixtures do not match call shape")
        if self.call_result_action != self.call_shape.call_result_action:
            raise ValueError("probe result action does not match call shape")
        if self.variant == "static_only":
            if self.current_member_fixture_ids or self.sentinel_kind != "static_snapshot":
                raise ValueError("static probe has dynamic fixture or sentinel")
        elif self.variant != "constructor_identity" and not self.current_member_fixture_ids:
            raise ValueError("dynamic probe must name its current-family members")
        if self.expected_exception_phase == "consumption" and self.call_result_action != (
            "consume_tuple"
        ):
            raise ValueError("consumption exception requires consume_tuple")
        object.__setattr__(
            self, "keyword_fixture_ids", dict(sorted(self.keyword_fixture_ids.items()))
        )
        return self


class BoundaryProbeResultV1(_StrictModelMixin, BaseModel):
    probe_id: StrictStr
    inventory_id: StrictStr
    setup_id: StrictStr
    setup_result_digest: StrictStr = Field(pattern=_SHA256)
    call_shape_digest: StrictStr = Field(pattern=_SHA256)
    exception_type: StrictStr
    exception_phase: Literal["invocation", "consumption"]
    sentinel_id: StrictStr
    sentinel_kind: Literal["constructor_identity_fence", "boundary_reached"]
    sentinel_after_invocation: StrictInt = Field(ge=0)
    sentinel_after_consumption: StrictInt = Field(ge=0)
    reached_count: StrictInt = Field(ge=0)
    mutation_guard_counts: dict[StrictStr, StrictInt]
    setup_call_counts: dict[StrictStr, StrictInt]
    yielded_count: StrictInt = Field(ge=0)
    before_snapshot_digest: StrictStr = Field(pattern=_SHA256)
    after_snapshot_digest: StrictStr = Field(pattern=_SHA256)
    passed: bool
    result_digest: StrictStr = Field(pattern=_SHA256)

    @model_validator(mode="after")
    def validate_result_digest(self) -> Self:
        if self.result_digest != _digest_without_field(self, "result_digest"):
            raise ValueError("boundary result digest mismatch")
        return self

    @classmethod
    def with_digest(cls, **values: Any) -> Self:
        values["result_digest"] = "0" * 64
        provisional = cls.model_construct(**values)
        values["result_digest"] = _digest_without_field(provisional, "result_digest")
        return cls.model_validate(values)


class RootSnapshotV1(_StrictModelMixin, BaseModel):
    module_path: StrictStr
    qualname: StrictStr
    signature: StrictStr
    source_sha256: StrictStr = Field(pattern=_SHA256)
    ast_sha256: StrictStr = Field(pattern=_SHA256)
    exports: tuple[StrictStr, ...]

    @model_validator(mode="after")
    def validate_exports(self) -> Self:
        if not self.exports:
            raise ValueError("root snapshot exports must not be empty")
        return self


class ProductionDeclarationV1(_StrictModelMixin, BaseModel):
    module_path: StrictStr
    symbol: StrictStr
    source_span: StrictStr = Field(pattern=r"^[1-9][0-9]*:[1-9][0-9]*$")
    normalized_ast_sha256: StrictStr = Field(pattern=_SHA256)
    role: Literal[
        "read_only_v3_model",
        "read_only_v3_decoder",
        "legacy_boundary_reject_guard",
    ]


class ForbiddenDefinitionUniverseV1(_StrictModelMixin, BaseModel):
    source_files: tuple[StrictStr, ...]
    symbols: tuple[StrictStr, ...]
    exports: tuple[StrictStr, ...]
    registry_keys: tuple[StrictStr, ...]


class TopLevelDeclarationV1(_StrictModelMixin, BaseModel):
    ordinal: StrictInt = Field(ge=0)
    node_kind: StrictStr
    names: tuple[StrictStr, ...]
    source_span: StrictStr = Field(pattern=r"^[1-9][0-9]*:[1-9][0-9]*$")
    normalized_ast_sha256: StrictStr = Field(pattern=_SHA256)
    role: Literal[
        "module_docstring",
        "import",
        "assignment",
        "function",
        "class",
        "registration",
        "export",
        "conditional",
        "statement",
    ]


class SourceFileSnapshotV1(_StrictModelMixin, BaseModel):
    module_path: StrictStr
    source_sha256: StrictStr = Field(pattern=_SHA256)
    declarations: tuple[TopLevelDeclarationV1, ...]

    @model_validator(mode="after")
    def validate_declarations(self) -> Self:
        if not self.declarations:
            raise ValueError("source file declaration closure must not be empty")
        if tuple(item.ordinal for item in self.declarations) != tuple(
            range(len(self.declarations))
        ):
            raise ValueError("top-level declaration ordinals must be complete and ordered")
        return self


class AllowedDiffEntryV1(_StrictModelMixin, BaseModel):
    path: StrictStr = Field(min_length=1)
    status: Literal["A", "M", "D", "T"]
    old_mode: StrictStr = Field(pattern=r"^[0-7]{6}$")
    new_mode: StrictStr = Field(pattern=r"^[0-7]{6}$")
    category: Literal["architecture", "production", "fixture", "test"]

    @property
    def policy_key(self) -> tuple[str, str, str, str]:
        return (self.path, self.status, self.old_mode, self.new_mode)


class BootstrapPredecessorV1(_StrictModelMixin, BaseModel):
    """The exact installed Release A pair an enforced target must name."""

    commit_sha: StrictStr = Field(pattern=_SHA1)
    tree_sha: StrictStr = Field(pattern=_SHA1)


class EvidenceChannelV1(_StrictModelMixin, BaseModel):
    repository: Literal["roxorlt/rquant"]
    workflow_path: Literal[".github/workflows/ci.yml"]
    jobs: tuple[
        Literal["r07-differential-gate-py311"],
        Literal["r07-differential-gate-py312"],
        Literal["r07-differential-gate-evidence"],
    ]
    artifact_json_path: Literal["r07-dr-gate/evidence-v1.json"]
    retention_days: Literal[90]
    cache_path: Literal["/home/lighthouse/rquant/var/r07-dr-evidence"]
    deployment_mode: Literal["disabled_for_bootstrap", "enforced"]
    bootstrap_predecessor: BootstrapPredecessorV1 | None

    @model_validator(mode="after")
    def validate_deployment_mode(self) -> Self:
        if self.deployment_mode == "enforced" and self.bootstrap_predecessor is None:
            raise ValueError("enforced R07 policy must name an exact bootstrap predecessor")
        if self.deployment_mode == "disabled_for_bootstrap" and (
            self.bootstrap_predecessor is not None
        ):
            raise ValueError("bootstrap-disabled R07 policy must not name a predecessor")
        return self


def boundary_manifest_digest(
    probe_setups: tuple[ProbeSetupV1, ...],
    boundary_probes: tuple[BoundaryProbeV1, ...],
) -> str:
    payload = {
        "probe_setups": [setup.model_dump(mode="json") for setup in probe_setups],
        "boundary_probes": [probe.model_dump(mode="json") for probe in boundary_probes],
    }
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


class R07PolicyV1(_StrictModelMixin, BaseModel):
    schema_version: Literal[1]
    baseline_commit_sha: Literal[BASELINE_COMMIT_SHA]
    baseline_tree_sha: Literal[BASELINE_TREE_SHA]
    allowed_diff: tuple[AllowedDiffEntryV1, ...]
    production_declarations: tuple[ProductionDeclarationV1, ...]
    fixtures: tuple[FixtureValueV1, ...]
    current_fixtures: tuple[CurrentFixtureV1, ...]
    fixtures_digest: StrictStr = Field(pattern=_SHA256)
    root_snapshots: tuple[RootSnapshotV1, ...]
    forbidden_definition_universe: ForbiddenDefinitionUniverseV1
    source_file_snapshots: tuple[SourceFileSnapshotV1, ...]
    probe_setups: tuple[ProbeSetupV1, ...]
    boundary_probes: tuple[BoundaryProbeV1, ...]
    boundary_manifest_digest: StrictStr = Field(pattern=_SHA256)
    evidence_channel: EvidenceChannelV1
    policy_digest: StrictStr = Field(pattern=_SHA256)

    @model_validator(mode="after")
    def validate_digest(self) -> Self:
        if self.boundary_manifest_digest != boundary_manifest_digest(
            self.probe_setups,
            self.boundary_probes,
        ):
            raise ValueError("boundary_manifest_digest does not match canonical probes")
        if self.policy_digest != _digest_without_field(self, "policy_digest"):
            raise ValueError("policy_digest does not match canonical policy")
        return self

    def validate_boundary_manifest(self) -> StaticCheckResult:
        observed = boundary_manifest_digest(self.probe_setups, self.boundary_probes)
        if observed != self.boundary_manifest_digest:
            return StaticCheckResult(False, ("boundary manifest digest mismatch",))
        return StaticCheckResult(True)

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json"))


def expected_gate_check_total(policy: R07PolicyV1) -> int:
    """Ordered checks one exact gate run must complete for a frozen policy."""

    if type(policy) is not R07PolicyV1:
        raise TypeError("exact R07PolicyV1 is required")
    return (
        1  # policy load from the candidate Git tree
        + 1  # complete raw diff and allowlist gate
        + len(FIXED_STATIC_CHECK_NAMES)
        + len(policy.root_snapshots)
        + len(policy.production_declarations)
        + len(policy.boundary_probes)
        + BOUNDARY_PROBE_COUNT
        + 1  # boundary probe results digest
    )


def verify_channel_binding(policy: R07PolicyV1, wire: R07DrGateEvidenceWireV1) -> None:
    """Compare the policy evidence channel field-for-field with the wire observation."""

    if type(policy) is not R07PolicyV1 or type(wire) is not R07DrGateEvidenceWireV1:
        raise TypeError("R07 channel binding requires exact policy and wire types")
    channel = policy.evidence_channel
    if channel.jobs != EVIDENCE_CHANNEL_JOBS:
        raise ValueError("R07 policy channel jobs are not the exact ordered triple")
    if channel.cache_path != R07_EVIDENCE_CACHE_DIR:
        raise ValueError("R07 policy channel cache path is not the fixed server directory")
    observed = (
        wire.repository,
        wire.workflow_path,
        wire.artifact_json_path,
        wire.retention_days,
        wire.python_runs[0].job_id,
        wire.python_runs[1].job_id,
        wire.producer_job_id,
    )
    declared = (
        channel.repository,
        channel.workflow_path,
        channel.artifact_json_path,
        channel.retention_days,
        channel.jobs[0],
        channel.jobs[1],
        channel.jobs[2],
    )
    if observed != declared:
        raise ValueError("R07 wire channel metadata does not match the policy channel")
    expected_total = expected_gate_check_total(policy)
    for run in wire.python_runs:
        if (run.collected, run.passed) != (expected_total, expected_total):
            raise ValueError("R07 Python run check count does not match the frozen policy")
        if (run.skipped, run.deselected) != (0, 0):
            raise ValueError("R07 Python run check count must exclude skips and deselects")


def load_policy(path: Path) -> R07PolicyV1:
    raw = path.read_bytes()
    strict_canonical_json_loads(raw)
    policy = R07PolicyV1.model_validate_json(raw)
    completeness = verify_policy_completeness(policy)
    if not completeness.passed:
        raise ValueError("incomplete R07 policy: " + "; ".join(completeness.reasons))
    return policy


@dataclass(frozen=True)
class GitDiffEntry:
    status: str
    path: str
    old_mode: str
    new_mode: str
    old_object: str
    new_object: str

    @property
    def policy_key(self) -> tuple[str, str, str, str]:
        return (self.path, self.status, self.old_mode, self.new_mode)


@dataclass(frozen=True)
class CompleteDiffResult:
    baseline_commit_sha: str
    candidate_commit_sha: str
    entries: tuple[GitDiffEntry, ...]

    @property
    def digest(self) -> str:
        payload = tuple(
            {
                "status": entry.status,
                "path": entry.path,
                "old_mode": entry.old_mode,
                "new_mode": entry.new_mode,
                "old_object": entry.old_object,
                "new_object": entry.new_object,
            }
            for entry in self.entries
        )
        return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


@dataclass(frozen=True)
class CandidateGateResult:
    passed: bool
    baseline_commit_sha: str
    baseline_tree_sha: str
    candidate_commit_sha: str
    candidate_tree_sha: str
    diff_digest: str
    diff_entries: tuple[GitDiffEntry, ...]
    candidate_binding_digest: str
    blocked_entries: tuple[GitDiffEntry, ...] = ()
    missing_entries: tuple[AllowedDiffEntryV1, ...] = ()


def _git_output(repo: Path, *arguments: str) -> bytes:
    return subprocess.run(
        ["git", "-C", str(repo), *arguments],
        check=True,
        capture_output=True,
    ).stdout


def _resolved_commit(repo: Path, commit: str) -> str:
    resolved = _git_output(repo, "rev-parse", "--verify", f"{commit}^{{commit}}").decode().strip()
    if resolved != commit or not _is_lower_hex(resolved, length=40):
        raise ValueError("candidate and baseline commits must be explicit lowercase commit SHAs")
    return resolved


def _resolved_tree(repo: Path, commit: str) -> str:
    tree = _git_output(repo, "rev-parse", "--verify", f"{commit}^{{tree}}").decode().strip()
    if not _is_lower_hex(tree, length=40):
        raise ValueError("Git tree must be a lowercase SHA")
    return tree


def _is_lower_hex(value: str, *, length: int) -> bool:
    return len(value) == length and all(character in "0123456789abcdef" for character in value)


def _validated_git_path(path: str) -> str:
    pure = PurePosixPath(path)
    if (
        not path
        or path.startswith("-")
        or pure.is_absolute()
        or pure.as_posix() != path
        or "\\" in path
        or any(part in {"", ".", ".."} for part in pure.parts)
    ):
        raise ValueError(f"invalid repository path: {path!r}")
    return path


def _tree_objects(repo: Path, commit: str) -> dict[str, tuple[str, str, str]]:
    resolved = _resolved_commit(repo, commit)
    raw = _git_output(repo, "ls-tree", "-r", "-z", "--full-tree", resolved)
    objects: dict[str, tuple[str, str, str]] = {}
    for record in raw.split(b"\0"):
        if not record:
            continue
        metadata, separator, raw_path = record.partition(b"\t")
        if not separator:
            raise ValueError("invalid Git tree record")
        mode, object_type, object_id = metadata.decode("ascii").split(" ")
        path = _validated_git_path(raw_path.decode("utf-8"))
        if path in objects:
            raise ValueError(f"duplicate Git tree path: {path}")
        if not _is_lower_hex(object_id, length=40):
            raise ValueError("Git tree object is not a lowercase SHA")
        objects[path] = (mode, object_type, object_id)
    return objects


def _git_mode_kind(mode: str) -> str:
    if mode == "000000":
        return "absent"
    if mode in {"100644", "100755"}:
        return "regular"
    if mode == "120000":
        return "symlink"
    if mode == "160000":
        return "gitlink"
    raise ValueError(f"unsupported Git mode: {mode}")


def _expected_diff_status(
    old: tuple[str, str, str] | None,
    new: tuple[str, str, str] | None,
) -> str:
    if old is None:
        return "A"
    if new is None:
        return "D"
    if _git_mode_kind(old[0]) != _git_mode_kind(new[0]):
        return "T"
    if old != new:
        return "M"
    raise ValueError("raw diff contains an unchanged path")


def validate_complete_diff_objects(repo: Path, result: CompleteDiffResult) -> None:
    baseline = _resolved_commit(repo, result.baseline_commit_sha)
    candidate = _resolved_commit(repo, result.candidate_commit_sha)
    baseline_objects = _tree_objects(repo, baseline)
    candidate_objects = _tree_objects(repo, candidate)
    seen: set[str] = set()
    for entry in result.entries:
        path = _validated_git_path(entry.path)
        if path in seen:
            raise ValueError(f"duplicate raw diff entry: {path}")
        seen.add(path)
        if entry.status == "?":
            if entry.old_object != "0" * 40 or entry.new_object != "0" * 40:
                raise ValueError("untracked diff entry must not claim a Git object")
            continue
        old = baseline_objects.get(path)
        new = candidate_objects.get(path)
        expected_old = ("000000", "0" * 40) if old is None else (old[0], old[2])
        expected_new = ("000000", "0" * 40) if new is None else (new[0], new[2])
        if (entry.old_mode, entry.old_object) != expected_old:
            raise ValueError(f"raw diff old object or mode mismatch: {path}")
        if (entry.new_mode, entry.new_object) != expected_new:
            raise ValueError(f"raw diff new object or mode mismatch: {path}")
        if old is not None and old[1] not in {"blob", "commit"}:
            raise ValueError(f"unsupported old Git object type: {path}")
        if new is not None and new[1] not in {"blob", "commit"}:
            raise ValueError(f"unsupported new Git object type: {path}")
        if entry.status != _expected_diff_status(old, new):
            raise ValueError(f"raw diff status mismatch: {path}")


def _candidate_binding_digest(
    complete: CompleteDiffResult,
    *,
    baseline_tree: str,
    candidate_tree: str,
) -> str:
    return _candidate_binding_digest_values(
        baseline_commit_sha=complete.baseline_commit_sha,
        baseline_tree_sha=baseline_tree,
        candidate_commit_sha=complete.candidate_commit_sha,
        candidate_tree_sha=candidate_tree,
        complete_diff_digest=complete.digest,
    )


def _untracked_mode(path: Path) -> str:
    observed = path.lstat().st_mode
    if stat.S_ISLNK(observed):
        return "120000"
    if stat.S_ISREG(observed):
        return "100755" if observed & stat.S_IXUSR else "100644"
    raise ValueError(f"unsupported untracked Git object type: {path}")


def collect_complete_git_diff(
    repo: Path,
    *,
    baseline_commit: str,
    candidate_commit: str,
    include_untracked: bool,
) -> CompleteDiffResult:
    baseline = _resolved_commit(repo, baseline_commit)
    candidate = _resolved_commit(repo, candidate_commit)
    raw = _git_output(
        repo,
        "diff",
        "--raw",
        "-z",
        "--no-renames",
        "--abbrev=40",
        baseline,
        candidate,
    )
    parts = raw.split(b"\0")
    if parts[-1] != b"":
        raise ValueError("raw Git diff is not NUL terminated")
    entries: list[GitDiffEntry] = []
    for offset in range(0, len(parts) - 1, 2):
        header = parts[offset].decode("ascii")
        path = parts[offset + 1].decode("utf-8")
        if not header.startswith(":"):
            raise ValueError("invalid raw Git diff header")
        old_mode, new_mode, old_object, new_object, status_value = header[1:].split(" ")
        status = status_value[0]
        if status not in {"A", "M", "D", "T"} or len(status_value) != 1:
            raise ValueError(f"unsupported raw Git diff status: {status_value}")
        entries.append(
            GitDiffEntry(
                status=status,
                path=path,
                old_mode=old_mode,
                new_mode=new_mode,
                old_object=old_object,
                new_object=new_object,
            )
        )
    if include_untracked:
        raw_untracked = _git_output(repo, "ls-files", "--others", "--exclude-standard", "-z")
        for raw_path in raw_untracked.split(b"\0"):
            if not raw_path:
                continue
            path = raw_path.decode("utf-8")
            entries.append(
                GitDiffEntry(
                    status="?",
                    path=path,
                    old_mode="000000",
                    new_mode=_untracked_mode(repo / path),
                    old_object="0" * 40,
                    new_object="0" * 40,
                )
            )
    ordered = tuple(sorted(entries, key=lambda item: (item.path, item.status)))
    result = CompleteDiffResult(
        baseline_commit_sha=baseline,
        candidate_commit_sha=candidate,
        entries=ordered,
    )
    validate_complete_diff_objects(repo, result)
    return result


@dataclass(frozen=True)
class MergeProvenanceResult:
    """The exact two-parent structure the final merge tree must have."""

    merge_base_commit_sha: str
    merge_base_tree_sha: str
    candidate_parent_commits: tuple[str, str]
    merge_tree_sha: str


def _repo_git_version(repo: Path) -> tuple[int, int, int]:
    return require_merge_tree_git_version(_git_output(repo, "--version").decode())


def _is_ancestor(repo: Path, ancestor: str, descendant: str) -> bool:
    return (
        subprocess.run(
            ["git", "-C", str(repo), "merge-base", "--is-ancestor", ancestor, descendant],
            check=False,
            capture_output=True,
        ).returncode
        == 0
    )


def resolve_merge_provenance(
    repo: Path,
    *,
    candidate_commit: str,
    merge_base_commit: str,
) -> MergeProvenanceResult:
    """Resolve the candidate's merge structure from Git objects alone.

    Amended per Codex round-2 order 2026-08-25, item P1-1 and ruling 9. A deployable
    candidate must have the shape a pull request merge produces: exactly two parents, the
    first being the pre-merge ``main`` tip that is also the frozen merge base, and a tree that
    is exactly what merging those two parents produces. A squash, a rebase, or a
    fast-forward direct push has one parent and can never satisfy this. What this checks is
    the commit's structure; it does not prove the merge was reviewed, since a commit of the
    same shape can be built locally. It is the structural stand-in for the branch protection
    this repository does not have, and unlike a GitHub API answer it replays offline.
    """

    _repo_git_version(repo)
    resolved = _resolved_commit(repo, candidate_commit)
    base = _resolved_commit(repo, merge_base_commit)
    listed = _git_output(repo, "rev-list", "--parents", "-n", "1", resolved).decode().split()
    if not listed or listed[0] != resolved:
        raise ValueError("candidate commit does not resolve to itself in the Git object store")
    parents = tuple(listed[1:])
    if len(parents) != 2:
        raise ValueError(
            f"R07 candidate must be a two-parent merge commit; observed {len(parents)} parent(s)"
        )
    if parents[0] == parents[1]:
        raise ValueError("R07 candidate parents must be two distinct commits")
    first, second = (_resolved_commit(repo, parent) for parent in parents)
    if first != base:
        raise ValueError("R07 candidate first parent is not the recorded merge base")
    if not _is_ancestor(repo, first, second):
        raise ValueError("R07 candidate merge base is not an ancestor of the pull request head")
    written = subprocess.run(
        ["git", "-C", str(repo), "merge-tree", "--write-tree", first, second],
        check=False,
        capture_output=True,
    )
    merge_tree = written.stdout.decode().strip().splitlines()[0] if written.stdout else ""
    if written.returncode != 0 or not _is_lower_hex(merge_tree, length=40):
        raise ValueError("R07 candidate parents do not merge to one exact tree")
    return MergeProvenanceResult(
        merge_base_commit_sha=first,
        merge_base_tree_sha=_resolved_tree(repo, first),
        candidate_parent_commits=(first, second),
        merge_tree_sha=merge_tree,
    )


def verify_merge_provenance(
    repo: Path,
    *,
    candidate_commit: str,
    candidate_tree: str,
    merge_base_commit: str,
    merge_base_tree: str,
    declared_parents: tuple[str, str],
    declared_merge_tree: str,
) -> MergeProvenanceResult:
    """Compare a declared merge structure field-for-field with the one Git produces."""

    resolved = resolve_merge_provenance(
        repo,
        candidate_commit=candidate_commit,
        merge_base_commit=merge_base_commit,
    )
    if tuple(declared_parents) != resolved.candidate_parent_commits:
        raise ValueError("declared candidate parent commits are not the Git parents")
    if declared_merge_tree != resolved.merge_tree_sha:
        raise ValueError("declared merge tree is not the tree those parents merge to")
    if merge_base_tree != resolved.merge_base_tree_sha:
        raise ValueError("declared merge base tree is not the merge base commit tree")
    if candidate_tree != resolved.merge_tree_sha:
        raise ValueError("candidate tree is not the final merge tree")
    return resolved


# ---------------------------------------------------------------------------
# Baseline context resolution
# ---------------------------------------------------------------------------
# Amended for Release B. The frozen baseline used to be re-discovered inside the checkout as
# ``merge_base(origin/main, HEAD)``. Once the pull request that froze it is merged,
# ``origin/main`` *is* the candidate, that expression collapses to HEAD, and no constant can
# ever match it again: the old semantics denies itself the moment it lands. Every R07 run now
# states its two endpoints explicitly instead, and this module is the only place that decides
# what they are, so the generator, the CI producer, and the tests cannot drift apart.

NULL_COMMIT_SHA = "0" * 40


class R07BaselineContextV1(_StrictModelMixin, BaseModel):
    """The exact pair of commits one R07 run compares, plus where the base came from.

    ``pull_request`` states the target branch tip and the pull request head, and the frozen
    baseline must be their real two-sided merge base. ``push`` states the release interval:
    the base is the pushed merge commit's first parent, which is the pre-merge ``main`` tip,
    and ``event_before_sha`` is GitHub's own claim about that same commit, kept only so the
    two can be cross-checked.
    """

    event: Literal["pull_request", "push"]
    base_sha: StrictStr = Field(pattern=_SHA1)
    candidate_sha: StrictStr = Field(pattern=_SHA1)
    base_source: Literal[
        "explicit_cli",
        "github_event_payload",
        "git_first_parent",
        "frozen_baseline_fallback",
    ]
    event_before_sha: StrictStr | None = None

    @model_validator(mode="after")
    def validate_endpoints(self) -> Self:
        if self.base_sha == self.candidate_sha:
            raise ValueError("R07 base and candidate must be two distinct commits")
        if self.event_before_sha is not None:
            if self.event != "push":
                raise ValueError("only a push context carries a GitHub before SHA")
            if not _is_lower_hex(self.event_before_sha, length=40):
                raise ValueError("R07 push before SHA must be a lowercase 40-hex commit")
            if self.event_before_sha == NULL_COMMIT_SHA:
                raise ValueError(
                    "R07 push before SHA is the null commit, so this push created or reset the "
                    "branch and has no release interval start"
                )
        return self


@dataclass(frozen=True)
class DeclaredBaselineArgumentsV1:
    """What the caller said, before Git is asked anything."""

    event: str
    base_sha: str | None
    candidate_sha: str | None
    event_before_sha: str | None


@dataclass(frozen=True)
class R07BaselineResolutionV1:
    """The verified answer: which commit anchors this run's reviewed diff."""

    context: R07BaselineContextV1
    baseline_commit_sha: str
    baseline_tree_sha: str


def baseline_resolution_summary(resolution: R07BaselineResolutionV1) -> str:
    """One line naming which of the four sources decided this run's baseline.

    The four sources are not equally strong - ``frozen_baseline_fallback`` degenerates the
    merge-base equality into ancestry - and they all produce identical policy bytes, so
    without this line a log cannot tell which one ran. Only SHAs and a fixed enum, so it is
    safe in a public CI log.
    """

    if type(resolution) is not R07BaselineResolutionV1:
        raise TypeError("exact R07BaselineResolutionV1 is required")
    context = resolution.context
    return (
        f"R07 baseline: event={context.event} base={resolution.baseline_commit_sha} "
        f"candidate={context.candidate_sha} base_source={context.base_source}"
    )


def _optional_argument_sha(value: str | None, *, field: str) -> str | None:
    """A workflow expression that resolves to nothing substitutes an empty string."""

    if value is None or value == "":
        return None
    if not _is_lower_hex(value, length=40):
        raise ValueError(f"R07 {field} is not a lowercase 40-hex commit SHA: {value!r}")
    return value


def parse_baseline_cli_arguments(
    *,
    event: str,
    base_sha: str | None = None,
    candidate_sha: str | None = None,
    event_before_sha: str | None = None,
) -> DeclaredBaselineArgumentsV1:
    """Validate exactly the strings a GitHub workflow expression can substitute.

    This is deliberately pure: the workflow can only be exercised for real by pushing it, so
    the part that can be wrong locally — an expression that yields nothing, a null commit on a
    branch's first push, a symbolic ref smuggled in where a SHA belongs — is decided here and
    unit tested against the shapes GitHub actually produces.
    """

    if event not in ("pull_request", "push"):
        raise ValueError(f"R07 baseline resolution has no semantics for the {event!r} event")
    base = _optional_argument_sha(base_sha, field="base SHA")
    candidate = _optional_argument_sha(candidate_sha, field="candidate SHA")
    before = _optional_argument_sha(event_before_sha, field="push before SHA")
    if event == "pull_request":
        if base is None:
            raise ValueError(
                "a pull request R07 context must state its base SHA; there is no ref to read it "
                "back from"
            )
        if before is not None:
            raise ValueError("only a push context carries a GitHub before SHA")
    elif before == NULL_COMMIT_SHA:
        raise ValueError(
            "R07 push before SHA is the null commit, so this push created or reset the branch "
            "and has no release interval start"
        )
    return DeclaredBaselineArgumentsV1(
        event=event,
        base_sha=base,
        candidate_sha=candidate,
        event_before_sha=before,
    )


def _head_commit(repo: Path) -> str:
    """Resolve the checkout's own HEAD, the only ref this resolver ever reads."""

    try:
        resolved = _git_output(repo, "rev-parse", "--verify", "HEAD^{commit}").decode().strip()
    except subprocess.CalledProcessError as exc:
        raise ValueError("the checkout has no resolvable HEAD commit") from exc
    if not _is_lower_hex(resolved, length=40):
        raise ValueError("checkout HEAD is not a lowercase 40-hex commit")
    return resolved


def _context_commit(repo: Path, sha: str, *, field: str) -> str:
    """Resolve one stated endpoint, turning "no such object" into an R07 refusal.

    ``git rev-parse --verify`` exits nonzero for a well-formed SHA that names nothing, and
    letting that surface as a CalledProcessError reports a bare git command line with no hint
    of which endpoint was wrong. Failing closed is not enough on its own here: a gate that
    refuses without saying what it refused is a gate people route around.
    """

    if not _is_lower_hex(sha, length=40):
        raise ValueError(f"R07 {field} is not a lowercase 40-hex commit SHA: {sha!r}")
    try:
        return _resolved_commit(repo, sha)
    except subprocess.CalledProcessError as exc:
        raise ValueError(f"R07 {field} does not name a commit in this repository: {sha}") from exc


def _commit_parents(repo: Path, commit: str) -> tuple[str, ...]:
    listed = _git_output(repo, "rev-list", "--parents", "-n", "1", commit).decode().split()
    if not listed or listed[0] != commit:
        raise ValueError("commit does not resolve to itself in the Git object store")
    return tuple(listed[1:])


def _two_sided_merge_base(repo: Path, left: str, right: str) -> str:
    """The real merge base of two commits, or a refusal. Never a one-sided ancestry answer."""

    completed = subprocess.run(
        ["git", "-C", str(repo), "merge-base", left, right],
        check=False,
        capture_output=True,
    )
    resolved = completed.stdout.decode().strip()
    if completed.returncode != 0 or not _is_lower_hex(resolved, length=40):
        raise ValueError(
            "R07 endpoints have no computable merge base; the gate does not degrade to an "
            "ancestry test"
        )
    return resolved


def _payload_sha(section: object, *, field: str) -> str:
    if not isinstance(section, dict):
        raise ValueError(f"R07 GitHub event payload has no {field}")
    value = section.get("sha")
    if not isinstance(value, str) or not _is_lower_hex(value, length=40):
        raise ValueError(f"R07 GitHub event payload {field} is not a lowercase 40-hex commit")
    return value


def _declared_from_github_event(values: Mapping[str, str]) -> DeclaredBaselineArgumentsV1:
    event_name = values.get("GITHUB_EVENT_NAME", "")
    event_path = values.get("GITHUB_EVENT_PATH", "")
    if not event_path:
        raise ValueError("R07 baseline resolution needs GITHUB_EVENT_PATH beside GITHUB_EVENT_NAME")
    try:
        payload = json.loads(Path(event_path).read_bytes())
    except (OSError, ValueError) as exc:
        raise ValueError("R07 GitHub event payload is unreadable") from exc
    if not isinstance(payload, dict):
        raise ValueError("R07 GitHub event payload is not a JSON object")
    if event_name == "pull_request":
        section = payload.get("pull_request")
        if not isinstance(section, dict):
            raise ValueError("R07 GitHub event payload has no pull_request section")
        return parse_baseline_cli_arguments(
            event="pull_request",
            base_sha=_payload_sha(section.get("base"), field="pull_request.base.sha"),
            candidate_sha=_payload_sha(section.get("head"), field="pull_request.head.sha"),
        )
    if event_name == "push":
        after = payload.get("after")
        before = payload.get("before")
        if not isinstance(after, str) or not _is_lower_hex(after, length=40):
            raise ValueError("R07 GitHub event payload after is not a lowercase 40-hex commit")
        if not isinstance(before, str) or not _is_lower_hex(before, length=40):
            raise ValueError("R07 GitHub event payload before is not a lowercase 40-hex commit")
        return parse_baseline_cli_arguments(
            event="push",
            candidate_sha=after,
            event_before_sha=before,
        )
    raise ValueError(f"R07 baseline resolution has no semantics for the {event_name!r} event")


def _declared_from_local_head(
    repo: Path,
    values: Mapping[str, str],
    expected_baseline: str,
) -> tuple[DeclaredBaselineArgumentsV1, str]:
    """Derive the context from HEAD's own parent structure, asking no ref anything.

    A two-parent HEAD is exactly the shape a merge into ``main`` leaves behind, so it replays
    the push semantics offline: the base is the first parent. A single-parent HEAD is a branch
    tip with nothing to merge-base against, so it falls back to the frozen baseline; that mode
    is labelled, and it is refused inside GitHub Actions, where the event always states the
    endpoints.
    """

    candidate = _head_commit(repo)
    parents = _commit_parents(repo, candidate)
    if len(parents) == 2:
        return (
            DeclaredBaselineArgumentsV1(
                event="push",
                base_sha=parents[0],
                candidate_sha=candidate,
                event_before_sha=None,
            ),
            "git_first_parent",
        )
    if len(parents) > 2:
        raise ValueError(
            f"R07 has no semantics for a {len(parents)}-parent octopus merge as the candidate"
        )
    if "GITHUB_ACTIONS" in values:
        raise ValueError(
            "the R07 branch-tip baseline fallback is refused inside GitHub Actions; CI must "
            "state the pull request or push endpoints explicitly"
        )
    return (
        DeclaredBaselineArgumentsV1(
            event="pull_request",
            base_sha=expected_baseline,
            candidate_sha=candidate,
            event_before_sha=None,
        ),
        "frozen_baseline_fallback",
    )


def resolve_baseline_context(
    repo: Path,
    *,
    event: str | None = None,
    base_sha: str | None = None,
    candidate_sha: str | None = None,
    event_before_sha: str | None = None,
    environ: Mapping[str, str] | None = None,
    expected_baseline: str = BASELINE_COMMIT_SHA,
) -> R07BaselineResolutionV1:
    """Decide which commit anchors this run's reviewed diff, and fail closed otherwise.

    The sources are tried in one fixed order, and no later source can rescue an earlier one
    that was stated badly: explicit arguments, then the GitHub event payload, then HEAD's own
    parent structure. Nothing here reads ``origin/main``, ``origin/HEAD``, or any other
    remote-tracking ref, which is the whole point: those move, and a gate anchored to
    something that moves is not a gate.

    In the ``frozen_baseline_fallback`` mode the base *is* the frozen baseline, so the
    merge-base equality degenerates into "the baseline is an ancestor of HEAD". That mode is
    for a local branch tip only. What actually holds the line there is the allowlist equality
    in ``verify_candidate_gate`` plus the two ancestry constraints and the policy digest, not
    the merge-base test.
    """

    values = os.environ if environ is None else environ
    if not _is_lower_hex(expected_baseline, length=40):
        raise ValueError("the expected R07 baseline must be a lowercase 40-hex commit SHA")
    if event is None:
        if any(value for value in (base_sha, candidate_sha, event_before_sha)):
            raise ValueError("R07 baseline endpoints were given without an event; state it")
        if values.get("GITHUB_EVENT_NAME"):
            declared = _declared_from_github_event(values)
            source = "github_event_payload" if declared.base_sha is not None else "git_first_parent"
        else:
            declared, source = _declared_from_local_head(repo, values, expected_baseline)
    else:
        declared = parse_baseline_cli_arguments(
            event=event,
            base_sha=base_sha,
            candidate_sha=candidate_sha,
            event_before_sha=event_before_sha,
        )
        source = "explicit_cli" if declared.base_sha is not None else "git_first_parent"

    candidate = (
        _head_commit(repo)
        if declared.candidate_sha is None
        else _context_commit(repo, declared.candidate_sha, field="candidate SHA")
    )
    base = declared.base_sha
    if base is None:
        parents = _commit_parents(repo, candidate)
        if len(parents) != 2:
            raise ValueError(
                "R07 push candidate must be a two-parent merge commit to have a first parent; "
                f"observed {len(parents)} parent(s)"
            )
        base = parents[0]
    context = R07BaselineContextV1(
        event=declared.event,
        base_sha=_context_commit(repo, base, field="base SHA"),
        candidate_sha=candidate,
        base_source=source,
        event_before_sha=declared.event_before_sha,
    )
    return verify_baseline_context(repo, context, expected_baseline=expected_baseline)


def verify_baseline_context(
    repo: Path,
    context: R07BaselineContextV1,
    *,
    expected_baseline: str = BASELINE_COMMIT_SHA,
) -> R07BaselineResolutionV1:
    """Prove against Git that the stated endpoints really do meet at the frozen baseline."""

    if type(context) is not R07BaselineContextV1:
        raise TypeError("exact R07BaselineContextV1 is required")
    base = _context_commit(repo, context.base_sha, field="base SHA")
    candidate = _context_commit(repo, context.candidate_sha, field="candidate SHA")
    if base == candidate:
        raise ValueError("R07 base and candidate are one commit, so the reviewed diff is empty")
    if context.event == "push":
        provenance = resolve_merge_provenance(
            repo,
            candidate_commit=candidate,
            merge_base_commit=base,
        )
        first, second = provenance.candidate_parent_commits
        if _two_sided_merge_base(repo, first, second) != first:
            raise ValueError(
                "the pushed merge commit's first parent is not the merge base of its two parents"
            )
        if context.event_before_sha is not None and context.event_before_sha != first:
            raise ValueError(
                "the GitHub push before SHA is not the first parent of the pushed merge commit"
            )
        baseline = first
    else:
        baseline = _two_sided_merge_base(repo, base, candidate)
    if baseline != expected_baseline:
        if context.event == "push":
            # For a push the merge base *is* the first parent, so name it that way: the
            # reader has to know which of the two commits to go look at.
            raise ValueError(
                f"the pushed merge commit's first parent is not the frozen R07 baseline: {baseline}"
            )
        raise ValueError(
            "the frozen R07 baseline is not the merge base of this run's stated endpoints: "
            f"{baseline}"
        )
    return R07BaselineResolutionV1(
        context=context,
        baseline_commit_sha=baseline,
        baseline_tree_sha=_resolved_tree(repo, baseline),
    )


def verify_candidate_gate(
    repo: Path,
    *,
    policy: R07PolicyV1,
    candidate_commit: str,
    candidate_tree: str,
) -> CandidateGateResult:
    baseline = _resolved_commit(repo, policy.baseline_commit_sha)
    candidate = _resolved_commit(repo, candidate_commit)
    baseline_tree = _resolved_tree(repo, baseline)
    observed_candidate_tree = _resolved_tree(repo, candidate)
    if not _is_lower_hex(candidate_tree, length=40):
        raise ValueError("candidate tree must be an explicit lowercase tree SHA")
    if observed_candidate_tree != candidate_tree:
        raise ValueError("candidate tree does not match candidate commit")
    if baseline_tree != policy.baseline_tree_sha:
        raise ValueError("policy baseline tree does not match its commit")
    # Amended for Release B. This used to be justified as "the same as requiring
    # merge_base(origin/main, candidate) to stay this commit", and that equivalence stopped
    # holding the moment the freezing pull request merged: origin/main became the candidate.
    # Which two commits meet at the baseline is now decided by resolve_baseline_context from
    # explicit endpoints. What is left here is the descent constraint the allowlist needs to
    # mean anything, and the allowlist equality below is what actually holds the line.
    if not _is_ancestor(repo, baseline, candidate):
        raise ValueError("candidate commit does not descend from the policy baseline merge base")
    # Codex Git hard constraint 3: the pre-amendment baseline must stay reachable.
    if not _is_ancestor(repo, HISTORICAL_BASELINE_COMMIT_SHA, candidate):
        raise ValueError("historical baseline is no longer an ancestor of the candidate")
    complete = collect_complete_git_diff(
        repo,
        baseline_commit=baseline,
        candidate_commit=candidate,
        include_untracked=False,
    )
    allowed = {
        (entry.path, entry.status, entry.old_mode, entry.new_mode): entry
        for entry in policy.allowed_diff
    }
    observed = {entry.policy_key: entry for entry in complete.entries}
    blocked = tuple(observed[key] for key in sorted(observed.keys() - allowed.keys()))
    missing = tuple(allowed[key] for key in sorted(allowed.keys() - observed.keys()))
    return CandidateGateResult(
        passed=not blocked and not missing,
        baseline_commit_sha=baseline,
        baseline_tree_sha=baseline_tree,
        candidate_commit_sha=candidate,
        candidate_tree_sha=observed_candidate_tree,
        diff_digest=complete.digest,
        diff_entries=complete.entries,
        candidate_binding_digest=_candidate_binding_digest(
            complete,
            baseline_tree=baseline_tree,
            candidate_tree=observed_candidate_tree,
        ),
        blocked_entries=blocked,
        missing_entries=missing,
    )


@dataclass(frozen=True)
class StaticCheckResult:
    passed: bool
    reasons: tuple[str, ...] = ()


@dataclass(frozen=True)
class R07StaticGateResult:
    passed: bool
    candidate_commit_sha: str
    candidate_tree_sha: str
    root_snapshot_digest: str
    forbidden_definition_digest: str
    checks: tuple[tuple[str, StaticCheckResult], ...]


def candidate_gate_digest(result: CandidateGateResult) -> str:
    """Digest the complete candidate diff gate outcome, not only its diff digest."""

    if type(result) is not CandidateGateResult:
        raise TypeError("exact CandidateGateResult is required")
    payload = {
        "passed": result.passed,
        "baseline_commit_sha": result.baseline_commit_sha,
        "baseline_tree_sha": result.baseline_tree_sha,
        "candidate_commit_sha": result.candidate_commit_sha,
        "candidate_tree_sha": result.candidate_tree_sha,
        "complete_diff_digest": result.diff_digest,
        "candidate_binding_digest": result.candidate_binding_digest,
        "diff_entries": [
            {
                "status": entry.status,
                "path": entry.path,
                "old_mode": entry.old_mode,
                "new_mode": entry.new_mode,
                "old_object": entry.old_object,
                "new_object": entry.new_object,
            }
            for entry in result.diff_entries
        ],
        "blocked_entries": [entry.path for entry in result.blocked_entries],
        "missing_entries": [entry.path for entry in result.missing_entries],
    }
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def static_gate_result_digest(result: R07StaticGateResult) -> str:
    """Digest every named static check outcome, so a version split cannot hide in a count."""

    if type(result) is not R07StaticGateResult:
        raise TypeError("exact R07StaticGateResult is required")
    payload = {
        "passed": result.passed,
        "candidate_commit_sha": result.candidate_commit_sha,
        "candidate_tree_sha": result.candidate_tree_sha,
        "root_snapshot_digest": result.root_snapshot_digest,
        "forbidden_definition_digest": result.forbidden_definition_digest,
        "checks": [
            {"name": name, "passed": check.passed, "reasons": list(check.reasons)}
            for name, check in result.checks
        ],
    }
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def gate_check_inventory(
    policy: R07PolicyV1,
    static_result: R07StaticGateResult,
    boundary_results: tuple[BoundaryProbeResultV1, ...],
) -> tuple[str, ...]:
    """The exact ordered check inventory one gate run must complete for a frozen policy."""

    if type(policy) is not R07PolicyV1 or type(static_result) is not R07StaticGateResult:
        raise TypeError("exact policy and static gate result are required")
    inventory = (
        f"policy-load:{POLICY_RELATIVE_PATH}",
        "candidate-diff-gate",
        *(f"static:{name}" for name, _result in static_result.checks),
        *(f"boundary-probe:{result.inventory_id}" for result in boundary_results),
        "boundary-probe-results-digest",
    )
    if len(set(inventory)) != len(inventory):
        raise ValueError("R07 gate check inventory is not unique")
    if len(inventory) != expected_gate_check_total(policy):
        raise ValueError("R07 gate check inventory does not match the frozen policy")
    return inventory


def check_inventory_digest(inventory: tuple[str, ...]) -> str:
    if type(inventory) is not tuple or any(type(name) is not str for name in inventory):
        raise TypeError("check inventory must be an exact tuple of names")
    return hashlib.sha256(canonical_json_bytes(list(inventory))).hexdigest()


def _wire_from_gate_results(
    *,
    policy: R07PolicyV1,
    candidate_gate: CandidateGateResult,
    boundary_results: tuple[BoundaryProbeResultV1, ...],
    static_result: R07StaticGateResult,
    python_runs: tuple[PythonRunEvidenceV1, PythonRunEvidenceV1],
    merge_provenance: MergeProvenanceResult,
    workflow_run_id: int,
    run_attempt: int,
) -> R07DrGateEvidenceWireV1:
    if type(policy) is not R07PolicyV1:
        raise ValueError("exact R07PolicyV1 is required")
    if type(candidate_gate) is not CandidateGateResult or not candidate_gate.passed:
        raise ValueError("passed CandidateGateResult is required")
    if type(static_result) is not R07StaticGateResult or not static_result.passed:
        raise ValueError("passed R07StaticGateResult is required")
    if (static_result.candidate_commit_sha, static_result.candidate_tree_sha) != (
        candidate_gate.candidate_commit_sha,
        candidate_gate.candidate_tree_sha,
    ):
        raise ValueError("static gate candidate tree binding mismatch")
    if (
        type(python_runs) is not tuple
        or len(python_runs) != 2
        or any(type(run) is not PythonRunEvidenceV1 for run in python_runs)
    ):
        raise ValueError("exact ordered Python run pair is required")
    if any(
        (run.candidate_commit_sha, run.candidate_tree_sha)
        != (candidate_gate.candidate_commit_sha, candidate_gate.candidate_tree_sha)
        for run in python_runs
    ):
        raise ValueError("candidate tree binding mismatch in Python run evidence")
    boundary_result_digest = boundary_probe_results_digest(policy, boundary_results)
    values: dict[str, Any] = {
        "schema_version": 1,
        "repository": "roxorlt/rquant",
        "workflow_path": ".github/workflows/ci.yml",
        "event_name": "push",
        "ref": "refs/heads/main",
        "producer_job_id": "r07-differential-gate-evidence",
        "workflow_run_id": workflow_run_id,
        "run_attempt": run_attempt,
        "candidate_commit_sha": candidate_gate.candidate_commit_sha,
        "candidate_tree_sha": candidate_gate.candidate_tree_sha,
        "baseline_commit_sha": candidate_gate.baseline_commit_sha,
        "baseline_tree_sha": candidate_gate.baseline_tree_sha,
        "merge_base_commit_sha": merge_provenance.merge_base_commit_sha,
        "merge_base_tree_sha": merge_provenance.merge_base_tree_sha,
        "candidate_parent_commits": merge_provenance.candidate_parent_commits,
        "merge_tree_sha": merge_provenance.merge_tree_sha,
        "policy_digest": policy.policy_digest,
        "complete_diff_digest": candidate_gate.diff_digest,
        "candidate_binding_digest": candidate_gate.candidate_binding_digest,
        "boundary_manifest_digest": policy.boundary_manifest_digest,
        "boundary_result_digest": boundary_result_digest,
        "root_snapshot_digest": static_result.root_snapshot_digest,
        "forbidden_definition_digest": static_result.forbidden_definition_digest,
        "python_runs": python_runs,
        "artifact_name": f"r07-dr-gate-{candidate_gate.candidate_commit_sha}",
        "artifact_json_path": "r07-dr-gate/evidence-v1.json",
        "retention_days": 90,
        "outcome": "passed",
        "evidence_digest": "0" * 64,
    }
    provisional = R07DrGateEvidenceWireV1.model_construct(**values)
    values["evidence_digest"] = _digest_without_field(provisional, "evidence_digest")
    try:
        return R07DrGateEvidenceWireV1.model_validate(values)
    except ValidationError as exc:
        raise ValueError("wire candidate binding or field validation failed") from exc


@contextmanager
def _materialize_candidate_tree(repo: Path, candidate_commit: str) -> Iterator[Path]:
    with TemporaryDirectory(prefix="rquant-r07-candidate-") as directory:
        root = Path(directory).resolve(strict=True)
        archive = subprocess.run(
            ["git", "-C", str(repo), "archive", "--format=tar", candidate_commit],
            check=True,
            capture_output=True,
        ).stdout
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as bundle:
            for member in bundle.getmembers():
                relative = PurePosixPath(member.name)
                if (
                    relative.is_absolute()
                    or any(part in {"", ".", ".."} for part in relative.parts)
                    or member.issym()
                    or member.islnk()
                    or not (member.isdir() or member.isreg())
                ):
                    raise ValueError("candidate archive contains an unsafe path")
                try:
                    bundle.extract(member, root, filter="data")
                except TypeError as exc:
                    if "filter" not in str(exc):
                        raise
                    bundle.extract(member, root)
        for path in sorted(root.rglob("*"), key=lambda item: len(item.parts), reverse=True):
            os.chmod(path, 0o555 if path.is_dir() else 0o444)
        os.chmod(root, 0o555)
        yield root


def _load_candidate_probe_runner(candidate_root: Path) -> Callable[..., dict[str, object]]:
    """Load the probe facade from the candidate Git tree, never from the caller's worktree."""

    module_path = Path(candidate_root) / "tests" / "r07_differential_probe_runner.py"
    if not stat.S_ISREG(module_path.lstat().st_mode):
        raise ValueError("candidate probe runner facade is not a regular file")
    spec = importlib.util.spec_from_file_location(
        "rquant._r07_candidate_probe_runner",
        module_path.resolve(strict=True),
    )
    if spec is None or spec.loader is None:
        raise ValueError("candidate probe runner facade has no importable spec")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    runner = getattr(module, "run_boundary_probe_subprocess", None)
    if not callable(runner):
        raise ValueError("candidate probe runner facade is missing its fixed entrypoint")
    return cast("Callable[..., dict[str, object]]", runner)


def _wire_with_recomputed_digests(
    wire: R07DrGateEvidenceWireV1,
    *,
    policy: R07PolicyV1,
    candidate_gate: CandidateGateResult,
    static_result: R07StaticGateResult,
    boundary_result_digest: str,
    check_inventory: tuple[str, ...],
    merge_provenance: MergeProvenanceResult,
) -> R07DrGateEvidenceWireV1:
    recomputed_run_digests = {
        "candidate_gate_digest": candidate_gate_digest(candidate_gate),
        "static_result_digest": static_gate_result_digest(static_result),
        "boundary_result_digest": boundary_result_digest,
        "check_inventory_digest": check_inventory_digest(check_inventory),
    }
    python_runs = tuple(run.model_copy(update=recomputed_run_digests) for run in wire.python_runs)
    python_runs = tuple(
        run.model_copy(update={"result_digest": python_run_result_digest(run)})
        for run in python_runs
    )
    values = wire.model_dump(mode="python")
    values.update(
        {
            "merge_base_commit_sha": merge_provenance.merge_base_commit_sha,
            "merge_base_tree_sha": merge_provenance.merge_base_tree_sha,
            "candidate_parent_commits": merge_provenance.candidate_parent_commits,
            "merge_tree_sha": merge_provenance.merge_tree_sha,
            "policy_digest": policy.policy_digest,
            "complete_diff_digest": candidate_gate.diff_digest,
            "candidate_binding_digest": candidate_gate.candidate_binding_digest,
            "boundary_manifest_digest": policy.boundary_manifest_digest,
            "boundary_result_digest": boundary_result_digest,
            "root_snapshot_digest": static_result.root_snapshot_digest,
            "forbidden_definition_digest": static_result.forbidden_definition_digest,
            "python_runs": python_runs,
            "evidence_digest": "0" * 64,
        }
    )
    provisional = R07DrGateEvidenceWireV1.model_construct(**values)
    values["evidence_digest"] = _digest_without_field(provisional, "evidence_digest")
    return R07DrGateEvidenceWireV1.model_validate(values)


def _verify_wire(
    repo: Path,
    policy: R07PolicyV1,
    wire: R07DrGateEvidenceWireV1,
) -> VerifiedR07DrGateEvidenceV1:
    if type(policy) is not R07PolicyV1 or type(wire) is not R07DrGateEvidenceWireV1:
        raise TypeError("R07 verifier requires exact policy and wire types")
    try:
        validated_policy = R07PolicyV1.model_validate(policy.model_dump(mode="python"))
        validated_wire = R07DrGateEvidenceWireV1.model_validate(wire.model_dump(mode="python"))
    except ValidationError as exc:
        raise ValueError("R07 wire or policy failed strict validation") from exc
    if validated_policy != policy or validated_wire != wire:
        raise ValueError("R07 wire or policy is not canonically self-consistent")
    verify_channel_binding(validated_policy, validated_wire)

    try:
        candidate_gate = verify_candidate_gate(
            repo,
            policy=validated_policy,
            candidate_commit=wire.candidate_commit_sha,
            candidate_tree=wire.candidate_tree_sha,
        )
        if not candidate_gate.passed:
            raise ValueError("candidate gate did not pass")
        static_result = verify_r07_static_gate(
            repo,
            policy=validated_policy,
            candidate_commit=wire.candidate_commit_sha,
            candidate_tree=wire.candidate_tree_sha,
        )
        if not static_result.passed:
            raise ValueError("static gate did not pass")
        merge_provenance = verify_merge_provenance(
            repo,
            candidate_commit=wire.candidate_commit_sha,
            candidate_tree=wire.candidate_tree_sha,
            merge_base_commit=wire.merge_base_commit_sha,
            merge_base_tree=wire.merge_base_tree_sha,
            declared_parents=wire.candidate_parent_commits,
            declared_merge_tree=wire.merge_tree_sha,
        )
    except (ImportError, OSError, subprocess.CalledProcessError) as exc:
        raise ValueError("wire Git binding does not resolve to exact Git objects") from exc

    if (
        candidate_gate.candidate_commit_sha != wire.candidate_commit_sha
        or candidate_gate.candidate_tree_sha != wire.candidate_tree_sha
        or candidate_gate.baseline_commit_sha != wire.baseline_commit_sha
        or candidate_gate.baseline_tree_sha != wire.baseline_tree_sha
    ):
        raise ValueError("wire Git binding does not match candidate gate")
    if (
        static_result.candidate_commit_sha != wire.candidate_commit_sha
        or static_result.candidate_tree_sha != wire.candidate_tree_sha
    ):
        raise ValueError("wire Git binding does not match static gate")

    policy_path = "tests/fixtures/r07_differential_gate/policy-v1.json"
    with _materialize_candidate_tree(repo, wire.candidate_commit_sha) as candidate_root:
        candidate_policy_path = candidate_root / policy_path
        try:
            candidate_policy_bytes = candidate_policy_path.read_bytes()
        except OSError as exc:
            raise ValueError("candidate policy object is unavailable") from exc
        if candidate_policy_bytes != validated_policy.canonical_bytes:
            raise ValueError("wire policy is not the exact candidate Git policy object")
        try:
            candidate_policy = load_policy(candidate_policy_path)
        except (OSError, ValidationError, ValueError) as exc:
            raise ValueError("candidate policy object failed strict validation") from exc
        if candidate_policy != validated_policy:
            raise ValueError("candidate policy object does not match supplied policy")

        try:
            run_boundary_probe_subprocess = _load_candidate_probe_runner(candidate_root)
        except (ImportError, OSError, SyntaxError, ValueError) as exc:
            raise ValueError("candidate probe runner facade is unavailable") from exc

        with TemporaryDirectory(prefix="rquant-r07-probe-") as probe_directory:
            probe_root = Path(probe_directory).resolve(strict=True)
            boundary_results = tuple(
                BoundaryProbeResultV1.model_validate(
                    run_boundary_probe_subprocess(
                        policy_path=candidate_policy_path,
                        candidate_root=candidate_root,
                        inventory_id=f"R07-B{index:02d}",
                        tmp_path=probe_root / f"b{index:02d}",
                    )
                )
                for index in range(1, 18)
            )
    if not all(result.passed for result in boundary_results):
        raise ValueError("one or more R07 boundary probes failed")
    boundary_digest = boundary_probe_results_digest(validated_policy, boundary_results)
    expected = _wire_with_recomputed_digests(
        wire,
        policy=validated_policy,
        candidate_gate=candidate_gate,
        static_result=static_result,
        boundary_result_digest=boundary_digest,
        check_inventory=gate_check_inventory(validated_policy, static_result, boundary_results),
        merge_provenance=merge_provenance,
    )
    if expected != wire:
        raise ValueError("R07 wire does not match recomputed gate evidence")
    return VerifiedR07DrGateEvidenceV1(
        _construction_token=_VERIFIED_CONSTRUCTION_TOKEN,
        _wire=wire,
    )


def verify_wire(
    repo: Path,
    policy: R07PolicyV1,
    wire: R07DrGateEvidenceWireV1,
) -> VerifiedR07DrGateEvidenceV1:
    """Verify an observation wire through the sole private R07 verifier."""

    return _verify_wire(repo, policy, wire)


def fixture_manifest_digest(
    fixtures: tuple[FixtureValueV1, ...],
    current_fixtures: tuple[CurrentFixtureV1, ...],
) -> str:
    resolve_fixture_values(fixtures)
    payload = {
        "fixtures": [fixture.model_dump(mode="json") for fixture in fixtures],
        "current_fixtures": [fixture.model_dump(mode="json") for fixture in current_fixtures],
    }
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def verify_policy_completeness(policy: R07PolicyV1) -> StaticCheckResult:
    reasons: list[str] = []
    allowed_keys = tuple(entry.policy_key for entry in policy.allowed_diff)
    if not allowed_keys or len(allowed_keys) != len(set(allowed_keys)):
        reasons.append("allowed diff must be nonempty and unique")
    if allowed_keys != tuple(sorted(allowed_keys)):
        reasons.append("allowed diff must use canonical path/status/mode order")
    declaration_keys = tuple(
        (declaration.module_path, declaration.symbol, declaration.role)
        for declaration in policy.production_declarations
    )
    if declaration_keys != EXPECTED_PRODUCTION_DECLARATIONS:
        reasons.append("production declaration universe mismatch")
    if tuple(snapshot.qualname for snapshot in policy.root_snapshots) != EXPECTED_ROOT_QUALNAMES:
        reasons.append("root snapshot universe mismatch")
    if any(not snapshot.exports for snapshot in policy.root_snapshots):
        reasons.append("root snapshot exports must not be empty")
    universe = policy.forbidden_definition_universe
    if universe.source_files != EXPECTED_FORBIDDEN_SOURCE_FILES:
        reasons.append("forbidden source-file universe mismatch")
    if universe.symbols != EXPECTED_FORBIDDEN_SYMBOLS:
        reasons.append("forbidden symbol universe mismatch")
    if universe.exports != EXPECTED_FORBIDDEN_EXPORTS:
        reasons.append("forbidden export universe mismatch")
    if universe.registry_keys != EXPECTED_FORBIDDEN_REGISTRY_KEYS:
        reasons.append("forbidden registry-key universe mismatch")
    if tuple(snapshot.module_path for snapshot in policy.source_file_snapshots) != (
        EXPECTED_FORBIDDEN_SOURCE_FILES
    ):
        reasons.append("source-file snapshot universe mismatch")
    if any(not snapshot.declarations for snapshot in policy.source_file_snapshots):
        reasons.append("source-file declaration closure must not be empty")
    if not policy.fixtures or not policy.current_fixtures:
        reasons.append("fixture manifests must not be empty")
    else:
        current_fixture_declarations = tuple(
            (
                fixture.fixture_id,
                fixture.current_model_module,
                fixture.current_model_qualname,
                fixture.parser_module,
                fixture.parser_qualname,
                fixture.allowed_form,
            )
            for fixture in policy.current_fixtures
        )
        if current_fixture_declarations != EXPECTED_CURRENT_FIXTURE_DECLARATIONS:
            reasons.append("current fixture declaration universe mismatch")
        try:
            observed_fixture_digest = fixture_manifest_digest(
                policy.fixtures,
                policy.current_fixtures,
            )
        except ValueError as exc:
            reasons.append(f"fixture manifest invalid: {exc}")
        else:
            if observed_fixture_digest != policy.fixtures_digest:
                reasons.append("fixture manifest digest mismatch")
    expected_ids = tuple(f"R07-B{index:02d}" for index in range(1, 20))
    if tuple(probe.inventory_id for probe in policy.boundary_probes) != expected_ids:
        reasons.append("boundary inventory mismatch")
    expected_setup_ids = tuple(f"setup-r07-b{index:02d}" for index in range(1, 20))
    if tuple(setup.setup_id for setup in policy.probe_setups) != expected_setup_ids:
        reasons.append("probe setup inventory mismatch")
    if tuple(probe.setup_id for probe in policy.boundary_probes) != expected_setup_ids:
        reasons.append("probe-to-setup binding mismatch")
    if tuple(probe.behavior_test for probe in policy.boundary_probes) != (
        EXPECTED_BOUNDARY_BEHAVIOR_TESTS
    ):
        reasons.append("boundary behavior-test universe mismatch")
    fixture_ids = {fixture.fixture_id for fixture in policy.fixtures} | {
        fixture.fixture_id for fixture in policy.current_fixtures
    }
    for setup in policy.probe_setups:
        for step in setup.steps:
            missing = set(step.fixture_ids) - fixture_ids
            if missing:
                reasons.append(f"{setup.setup_id}: missing setup fixture {sorted(missing)[0]}")
    for probe in policy.boundary_probes:
        referenced = (
            set(probe.positional_fixture_ids)
            | set(probe.keyword_fixture_ids.values())
            | set(probe.current_member_fixture_ids)
        )
        if probe.call_shape.receiver_fixture_id is not None:
            referenced.add(probe.call_shape.receiver_fixture_id)
        missing = referenced - fixture_ids
        if missing:
            reasons.append(f"{probe.inventory_id}: missing call fixture {sorted(missing)[0]}")
    if not policy.validate_boundary_manifest().passed:
        reasons.append("boundary manifest digest mismatch")
    return StaticCheckResult(not reasons, tuple(reasons))


def _source_bytes_for(repo: Path, candidate_commit: str, module_path: str) -> bytes:
    candidate = _resolved_commit(repo, candidate_commit)
    path = _validated_git_path(module_path)
    try:
        return _git_output(repo, "cat-file", "blob", f"{candidate}:{path}")
    except subprocess.CalledProcessError as exc:
        raise OSError(f"candidate source is unavailable: {path}") from exc


def _source_for(repo: Path, candidate_commit: str, module_path: str) -> str:
    return _source_bytes_for(repo, candidate_commit, module_path).decode("utf-8")


def _function_node(tree: ast.Module, qualname: str) -> ast.AST | None:
    name = qualname.rsplit(".", 1)[-1]
    return next(
        (
            node
            for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name
        ),
        None,
    )


def verify_root_snapshot(
    repo: Path,
    candidate_commit: str,
    snapshot: RootSnapshotV1,
) -> StaticCheckResult:
    try:
        source = _source_for(repo, candidate_commit, snapshot.module_path)
        tree = ast.parse(source, filename=snapshot.module_path)
    except (OSError, SyntaxError) as exc:
        return StaticCheckResult(False, (f"source parse failed: {type(exc).__name__}",))
    node = _function_node(tree, snapshot.qualname)
    if node is None:
        return StaticCheckResult(False, ("declared function is missing",))
    segment = ast.get_source_segment(source, node)
    if segment is None:
        return StaticCheckResult(False, ("declared function source is missing",))
    source_digest = hashlib.sha256(segment.encode()).hexdigest()
    ast_digest = normalized_ast_sha256(node)
    if source_digest != snapshot.source_sha256 or ast_digest != snapshot.ast_sha256:
        return StaticCheckResult(False, ("source or AST snapshot drift",))
    names = {
        item.name
        for item in tree.body
        if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
    }
    if not set(snapshot.exports) <= names:
        return StaticCheckResult(False, ("export snapshot drift",))
    signature = _signature_text(node)
    if signature != snapshot.signature:
        return StaticCheckResult(False, ("signature snapshot drift",))
    return StaticCheckResult(True)


def verify_production_declaration(
    repo: Path,
    candidate_commit: str,
    declaration: ProductionDeclarationV1,
) -> StaticCheckResult:
    try:
        source = _source_for(repo, candidate_commit, declaration.module_path)
        tree = ast.parse(source, filename=declaration.module_path)
    except (OSError, SyntaxError) as exc:
        return StaticCheckResult(False, (f"source parse failed: {type(exc).__name__}",))
    node = next(
        (
            item
            for item in tree.body
            if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
            and item.name == declaration.symbol
        ),
        None,
    )
    if node is None:
        return StaticCheckResult(False, ("declared production symbol is missing",))
    observed_span = f"{node.lineno}:{node.end_lineno}"
    observed_digest = normalized_ast_sha256(node)
    reasons: list[str] = []
    if observed_span != declaration.source_span:
        reasons.append("production declaration source span drift")
    if observed_digest != declaration.normalized_ast_sha256:
        reasons.append("production declaration AST drift")
    return StaticCheckResult(not reasons, tuple(reasons))


def verify_boundary_probe_source(
    repo: Path,
    candidate_commit: str,
    probe: BoundaryProbeV1,
) -> StaticCheckResult:
    try:
        filename, line_text = probe.source_span.rsplit(":", 1)
        line = int(line_text)
        module_path = f"src/rquant/{filename}"
        source = _source_for(repo, candidate_commit, module_path)
        tree = ast.parse(source, filename=module_path)
    except (OSError, SyntaxError, ValueError) as exc:
        return StaticCheckResult(False, (f"boundary source parse failed: {type(exc).__name__}",))
    candidates = [
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
        and node.lineno <= line <= (node.end_lineno or node.lineno)
    ]
    if not candidates:
        return StaticCheckResult(False, ("boundary source anchor is missing",))
    node = min(candidates, key=lambda item: (item.end_lineno or item.lineno) - item.lineno)
    observed = normalized_ast_sha256(node)
    if observed != probe.boundary_ast_sha256:
        return StaticCheckResult(False, ("boundary AST snapshot drift",))
    return StaticCheckResult(True)


def _signature_text(node: ast.AST) -> str:
    if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return ""
    return ast.unparse(node.args)


def _target_names(node: ast.AST) -> tuple[str, ...]:
    if isinstance(node, ast.Name):
        return (node.id,)
    if isinstance(node, (ast.Tuple, ast.List)):
        return tuple(name for item in node.elts for name in _target_names(item))
    if isinstance(node, ast.Subscript):
        return (_subscript_root_name(node),)
    return ()


def _subscript_root_name(node: ast.Subscript) -> str:
    value: ast.AST = node.value
    while isinstance(value, (ast.Attribute, ast.Subscript)):
        value = value.value
    return value.id if isinstance(value, ast.Name) else ""


def _declaration_names(node: ast.AST) -> tuple[str, ...]:
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        return (node.name,)
    if isinstance(node, (ast.Import, ast.ImportFrom)):
        return tuple(alias.asname or alias.name.split(".", 1)[0] for alias in node.names)
    if isinstance(node, ast.Assign):
        return tuple(name for target in node.targets for name in _target_names(target))
    if isinstance(node, (ast.AnnAssign, ast.AugAssign)):
        return _target_names(node.target)
    return ()


def _is_registry_statement(node: ast.AST) -> bool:
    targets: tuple[ast.AST, ...] = ()
    if isinstance(node, ast.Assign):
        targets = tuple(node.targets)
    elif isinstance(node, (ast.AnnAssign, ast.AugAssign)):
        targets = (node.target,)
    if any(
        isinstance(target, ast.Subscript) and "registry" in _subscript_root_name(target).lower()
        for target in targets
    ):
        return True
    if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
        called = node.value.func
        name = called.attr if isinstance(called, ast.Attribute) else getattr(called, "id", "")
        return "register" in name.lower()
    return False


def _is_export_statement(node: ast.AST) -> bool:
    if isinstance(node, ast.Assign):
        return any("__all__" in _target_names(target) for target in node.targets)
    if isinstance(node, (ast.AnnAssign, ast.AugAssign)):
        return "__all__" in _target_names(node.target)
    return False


def _top_level_role(node: ast.AST, *, ordinal: int) -> str:
    if (
        ordinal == 0
        and isinstance(node, ast.Expr)
        and isinstance(node.value, ast.Constant)
        and type(node.value.value) is str
    ):
        return "module_docstring"
    if isinstance(node, (ast.Import, ast.ImportFrom)):
        return "import"
    if _is_export_statement(node):
        return "export"
    if _is_registry_statement(node):
        return "registration"
    if isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
        return "assignment"
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return "function"
    if isinstance(node, ast.ClassDef):
        return "class"
    if isinstance(node, (ast.If, ast.Try, ast.With, ast.Match)):
        return "conditional"
    return "statement"


def top_level_declaration_snapshot(node: ast.AST, *, ordinal: int) -> TopLevelDeclarationV1:
    return TopLevelDeclarationV1(
        ordinal=ordinal,
        node_kind=type(node).__name__,
        names=_declaration_names(node),
        source_span=f"{node.lineno}:{node.end_lineno}",
        normalized_ast_sha256=normalized_ast_sha256(node),
        role=_top_level_role(node, ordinal=ordinal),
    )


def source_file_snapshot_from_source(
    module_path: str,
    source_bytes: bytes,
) -> SourceFileSnapshotV1:
    """Snapshot one module's top-level declaration closure from exact source bytes."""

    tree = ast.parse(source_bytes.decode("utf-8"), filename=module_path)
    return SourceFileSnapshotV1(
        module_path=module_path,
        source_sha256=hashlib.sha256(source_bytes).hexdigest(),
        declarations=tuple(
            top_level_declaration_snapshot(node, ordinal=ordinal)
            for ordinal, node in enumerate(tree.body)
        ),
    )


def source_file_snapshot(
    repo: Path,
    candidate_commit: str,
    module_path: str,
) -> SourceFileSnapshotV1:
    return source_file_snapshot_from_source(
        module_path,
        _source_bytes_for(repo, candidate_commit, module_path),
    )


def _dynamic_top_level_reasons(tree: ast.Module, module_path: str) -> tuple[str, ...]:
    reasons: list[str] = []
    declared = {name for node in tree.body for name in _declaration_names(node) if name}
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        for candidate in ast.walk(node):
            if isinstance(candidate, ast.Call):
                called = candidate.func
                if isinstance(called, ast.Name) and called.id == "__import__":
                    reasons.append(f"{module_path}: dynamic import")
                if (
                    isinstance(called, ast.Attribute)
                    and called.attr == "import_module"
                    and isinstance(called.value, ast.Name)
                    and called.value.id == "importlib"
                ):
                    reasons.append(f"{module_path}: dynamic import")
            if isinstance(candidate, ast.Subscript) and isinstance(candidate.ctx, ast.Store):
                root_name = _subscript_root_name(candidate)
                if "registry" not in root_name.lower():
                    continue
                key = candidate.slice
                if not (isinstance(key, ast.Constant) and type(key.value) is str):
                    reasons.append(f"{module_path}: dynamic registration key")
                if root_name not in declared:
                    reasons.append(f"{module_path}: unresolved registry alias {root_name}")
        if _is_export_statement(node) and isinstance(node, (ast.Assign, ast.AnnAssign)):
            value = node.value
            if not (
                isinstance(value, (ast.Tuple, ast.List))
                and all(
                    isinstance(item, ast.Constant) and type(item.value) is str
                    for item in value.elts
                )
            ):
                reasons.append(f"{module_path}: dynamic export")
    return tuple(dict.fromkeys(reasons))


def verify_top_level_source_closure(
    repo: Path,
    candidate_commit: str,
    snapshots: tuple[SourceFileSnapshotV1, ...] | list[SourceFileSnapshotV1],
) -> StaticCheckResult:
    reasons: list[str] = []
    for expected in snapshots:
        try:
            observed = source_file_snapshot(repo, candidate_commit, expected.module_path)
            tree = ast.parse(
                _source_for(repo, candidate_commit, expected.module_path),
                filename=expected.module_path,
            )
        except (OSError, SyntaxError, UnicodeDecodeError, ValueError) as exc:
            reasons.append(f"{expected.module_path}: {type(exc).__name__}")
            continue
        reasons.extend(_dynamic_top_level_reasons(tree, expected.module_path))
        if observed.source_sha256 != expected.source_sha256:
            reasons.append(f"{expected.module_path}: source digest drift")
        if observed.declarations != expected.declarations:
            reasons.append(f"{expected.module_path}: top-level declaration closure drift")
    return StaticCheckResult(not reasons, tuple(dict.fromkeys(reasons)))


def verify_forbidden_definitions(
    repo: Path,
    candidate_commit: str,
    universe: ForbiddenDefinitionUniverseV1,
) -> StaticCheckResult:
    reasons: list[str] = []
    forbidden = set(universe.symbols) | set(universe.exports) | set(universe.registry_keys)
    for module_path in universe.source_files:
        try:
            tree = ast.parse(
                _source_for(repo, candidate_commit, module_path),
                filename=module_path,
            )
        except (OSError, SyntaxError) as exc:
            reasons.append(f"{module_path}: {type(exc).__name__}")
            continue
        identifiers = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)} | {
            node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)
        }
        aliases = {
            alias.name.rsplit(".", 1)[-1]
            for node in ast.walk(tree)
            if isinstance(node, (ast.Import, ast.ImportFrom))
            for alias in node.names
        } | {
            alias.asname
            for node in ast.walk(tree)
            if isinstance(node, (ast.Import, ast.ImportFrom))
            for alias in node.names
            if alias.asname is not None
        }
        string_literals = {
            node.value
            for node in ast.walk(tree)
            if isinstance(node, ast.Constant) and type(node.value) is str
        }
        found = sorted(forbidden & (identifiers | aliases | string_literals))
        if found:
            reasons.append(f"{module_path}: {','.join(found)}")
    return StaticCheckResult(not reasons, tuple(reasons))


def diff_scope_source_paths(policy: R07PolicyV1) -> tuple[str, ...]:
    """Every candidate source file the reviewed diff touches, in canonical order."""

    if type(policy) is not R07PolicyV1:
        raise TypeError("exact R07PolicyV1 is required")
    return tuple(
        sorted(
            {
                entry.path
                for entry in policy.allowed_diff
                if entry.status != "D"
                and entry.path.startswith("src/rquant/")
                and entry.path.endswith(".py")
            }
        )
    )


def _bound_value_names(value: ast.AST | None) -> tuple[str, ...]:
    """Names an assignment's right-hand side rebinds, so an alias cannot launder a symbol."""

    if isinstance(value, ast.Name):
        return (value.id,)
    if isinstance(value, ast.Attribute):
        return (value.attr,)
    if isinstance(value, (ast.Tuple, ast.List)):
        return tuple(name for item in value.elts for name in _bound_value_names(item))
    return ()


def _string_constants(value: ast.AST | None) -> tuple[str, ...]:
    if isinstance(value, (ast.Tuple, ast.List)):
        return tuple(
            item.value
            for item in value.elts
            if isinstance(item, ast.Constant) and type(item.value) is str
        )
    return ()


def _definition_scope(tree: ast.Module) -> tuple[set[str], set[str], set[str], set[str]]:
    definitions: set[str] = set()
    exports: set[str] = set()
    registry_keys: set[str] = set()
    literal_keys: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            # ast.walk reaches nested definitions, so a method or a closure-local class
            # is bound exactly like a module-level one.
            definitions.add(node.name)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                # Both halves bind: renaming on import does not unbind the original name.
                definitions.add(alias.name.rsplit(".", 1)[-1])
                if alias.asname is not None:
                    definitions.add(alias.asname)
        elif isinstance(node, ast.Assign):
            names = tuple(name for target in node.targets for name in _target_names(target))
            definitions.update(names)
            definitions.update(_bound_value_names(node.value))
            if "__all__" in names:
                exports.update(_string_constants(node.value))
        elif isinstance(node, (ast.AnnAssign, ast.AugAssign)):
            names = _target_names(node.target)
            definitions.update(names)
            definitions.update(_bound_value_names(node.value))
            if "__all__" in names:
                exports.update(_string_constants(node.value))
        elif isinstance(node, ast.NamedExpr):
            # A walrus is an assignment expression: it binds its target wherever it appears,
            # and its right-hand side can alias a forbidden name just like a plain assignment.
            definitions.update(_target_names(node.target))
            definitions.update(_bound_value_names(node.value))
        elif isinstance(node, ast.Dict):
            # A string key in a dict literal is a registration position: it is how a dispatch
            # or handler table names the thing it binds.
            literal_keys.update(
                key.value
                for key in node.keys
                if isinstance(key, ast.Constant) and type(key.value) is str
            )
        if (
            isinstance(node, ast.Subscript)
            and isinstance(node.ctx, ast.Store)
            and "registry" in _subscript_root_name(node).lower()
            and isinstance(node.slice, ast.Constant)
            and type(node.slice.value) is str
        ):
            registry_keys.add(node.slice.value)
        if isinstance(node, ast.Call):
            called = node.func
            name = called.attr if isinstance(called, ast.Attribute) else getattr(called, "id", "")
            if "register" in name.lower():
                registry_keys.update(
                    argument.value
                    for argument in node.args
                    if isinstance(argument, ast.Constant) and type(argument.value) is str
                )
                registry_keys.update(
                    keyword.value.value
                    for keyword in node.keywords
                    if isinstance(keyword.value, ast.Constant) and type(keyword.value.value) is str
                )
    return definitions, exports, registry_keys, literal_keys


def verify_diff_scope_forbidden_definitions(
    repo: Path,
    candidate_commit: str,
    policy: R07PolicyV1,
) -> StaticCheckResult:
    """Scan every diffed candidate source file for a forbidden definition, export, or key.

    Amended per Codex round-2 order 2026-08-25, item P1-1. The ten frozen root snapshots and
    the nine-file forbidden universe keep their exact per-file closure; this check adds the
    breadth the merge face needs, asserting that none of the sources the pull request touches
    *defines* a writer/activation symbol, exports one, or registers one under a literal key.
    Mentioning a forbidden name is not a definition, which is why the policy module that
    declares the universe is not a hit.
    """

    if type(policy) is not R07PolicyV1:
        raise TypeError("exact R07PolicyV1 is required")
    universe = policy.forbidden_definition_universe
    reasons: list[str] = []
    for module_path in diff_scope_source_paths(policy):
        try:
            tree = ast.parse(
                _source_for(repo, candidate_commit, module_path),
                filename=module_path,
            )
        except (OSError, SyntaxError, UnicodeDecodeError) as exc:
            reasons.append(f"{module_path}: {type(exc).__name__}")
            continue
        definitions, exports, registry_keys, literal_keys = _definition_scope(tree)
        forbidden = set(universe.symbols) | set(universe.exports) | set(universe.registry_keys)
        found = sorted(
            (set(universe.symbols) & definitions)
            | (set(universe.exports) & exports)
            | (set(universe.registry_keys) & registry_keys)
            | (forbidden & literal_keys)
        )
        if found:
            reasons.append(f"{module_path}: {','.join(found)}")
    return StaticCheckResult(not reasons, tuple(reasons))


def boundary_probe_results_digest(
    policy: R07PolicyV1,
    results: tuple[BoundaryProbeResultV1, ...],
) -> str:
    expected_probes = tuple(
        probe for probe in policy.boundary_probes if probe.variant != "static_only"
    )
    if len(expected_probes) != 17 or len(results) != len(expected_probes):
        raise ValueError("boundary results must be the complete B01..B17 set")
    setups = {setup.setup_id: setup for setup in policy.probe_setups}
    for expected, result in zip(expected_probes, results, strict=True):
        if type(result) is not BoundaryProbeResultV1 or not result.passed:
            raise ValueError(f"{expected.inventory_id}: passed real probe result is required")
        if result.result_digest != _digest_without_field(result, "result_digest"):
            raise ValueError(f"{expected.inventory_id}: probe result digest mismatch")
        setup = setups[expected.setup_id]
        expected_call_shape_digest = hashlib.sha256(
            canonical_json_bytes(expected.call_shape.model_dump(mode="json"))
        ).hexdigest()
        expected_after_invocation = 0 if expected.expected_exception_phase == "consumption" else 1
        bindings_match = (
            result.probe_id == expected.probe_id
            and result.inventory_id == expected.inventory_id
            and result.setup_id == expected.setup_id
            and result.setup_result_digest == setup.setup_result_digest
            and result.call_shape_digest == expected_call_shape_digest
            and result.exception_type == expected.expected_exception
            and result.exception_phase == expected.expected_exception_phase
            and result.sentinel_id == expected.sentinel_id
            and result.sentinel_kind == expected.sentinel_kind
            and result.sentinel_after_invocation == expected_after_invocation
            and result.sentinel_after_consumption == 1
            and result.reached_count == 1
            and set(result.mutation_guard_counts) == set(expected.mutation_expectation.guard_ids)
            and len(result.mutation_guard_counts) == len(expected.mutation_expectation.guard_ids)
            and all(count == 0 for count in result.mutation_guard_counts.values())
            and all(count >= 0 for count in result.setup_call_counts.values())
            and result.yielded_count == expected.expected_yielded_count
            and result.before_snapshot_digest == expected.before_snapshot_digest
            and result.after_snapshot_digest == expected.after_snapshot_digest
            and result.before_snapshot_digest == result.after_snapshot_digest
        )
        if not bindings_match:
            raise ValueError(f"{expected.inventory_id}: probe result does not match policy")
    return hashlib.sha256(
        canonical_json_bytes([result.model_dump(mode="json") for result in results])
    ).hexdigest()


def verify_r07_static_gate(
    repo: Path,
    *,
    policy: R07PolicyV1,
    candidate_commit: str,
    candidate_tree: str,
) -> R07StaticGateResult:
    resolved_commit = _resolved_commit(repo, candidate_commit)
    resolved_tree = _resolved_tree(repo, resolved_commit)
    if resolved_tree != candidate_tree:
        raise ValueError("static gate candidate tree does not match candidate commit")
    checks: list[tuple[str, StaticCheckResult]] = [
        ("policy-completeness", verify_policy_completeness(policy))
    ]
    checks.extend(
        (
            f"root:{snapshot.qualname}",
            verify_root_snapshot(repo, resolved_commit, snapshot),
        )
        for snapshot in policy.root_snapshots
    )
    checks.extend(
        (
            f"production:{declaration.module_path}:{declaration.symbol}",
            verify_production_declaration(repo, resolved_commit, declaration),
        )
        for declaration in policy.production_declarations
    )
    checks.extend(
        (
            f"boundary:{probe.inventory_id}",
            verify_boundary_probe_source(repo, resolved_commit, probe),
        )
        for probe in policy.boundary_probes
    )
    checks.extend(
        (
            (
                "top-level-source-closure",
                verify_top_level_source_closure(
                    repo,
                    resolved_commit,
                    policy.source_file_snapshots,
                ),
            ),
            (
                "forbidden-definitions",
                verify_forbidden_definitions(
                    repo,
                    resolved_commit,
                    policy.forbidden_definition_universe,
                ),
            ),
            (
                "diff-scope-forbidden-definitions",
                verify_diff_scope_forbidden_definitions(repo, resolved_commit, policy),
            ),
        )
    )
    root_snapshot_digest = hashlib.sha256(
        canonical_json_bytes(
            [snapshot.model_dump(mode="json") for snapshot in policy.root_snapshots]
        )
    ).hexdigest()
    forbidden_definition_digest = hashlib.sha256(
        canonical_json_bytes(
            {
                "universe": policy.forbidden_definition_universe.model_dump(mode="json"),
                "source_file_snapshots": [
                    snapshot.model_dump(mode="json") for snapshot in policy.source_file_snapshots
                ],
            }
        )
    ).hexdigest()
    frozen_checks = tuple(checks)
    return R07StaticGateResult(
        passed=all(result.passed for _, result in frozen_checks),
        candidate_commit_sha=resolved_commit,
        candidate_tree_sha=resolved_tree,
        root_snapshot_digest=root_snapshot_digest,
        forbidden_definition_digest=forbidden_definition_digest,
        checks=frozen_checks,
    )


BOUNDARY_PROBES: tuple[BoundaryProbeV1, ...] = ()

FixtureValue = FixtureValueV1
CurrentFixture = CurrentFixtureV1
ProbeSetup = ProbeSetupV1
CallShape = CallShapeV1
BoundaryReachedSentinel = BoundaryReachedSentinelV1
BoundaryProbe = BoundaryProbeV1


ROOT_SNAPSHOTS: tuple[RootSnapshotV1, ...] = ()
FORBIDDEN_DEFINITION_UNIVERSE = ForbiddenDefinitionUniverseV1(
    source_files=EXPECTED_FORBIDDEN_SOURCE_FILES,
    symbols=EXPECTED_FORBIDDEN_SYMBOLS,
    exports=EXPECTED_FORBIDDEN_EXPORTS,
    registry_keys=EXPECTED_FORBIDDEN_REGISTRY_KEYS,
)


def _load_frozen_gate_metadata() -> R07PolicyV1 | None:
    path = (
        Path(__file__).parents[2]
        / "tests"
        / "fixtures"
        / "r07_differential_gate"
        / "policy-v1.json"
    )
    try:
        return load_policy(path)
    except (OSError, ValidationError, ValueError):
        return None


_FROZEN_GATE_METADATA = _load_frozen_gate_metadata()
if _FROZEN_GATE_METADATA is not None:
    BOUNDARY_PROBES = _FROZEN_GATE_METADATA.boundary_probes
    ROOT_SNAPSHOTS = _FROZEN_GATE_METADATA.root_snapshots
    FORBIDDEN_DEFINITION_UNIVERSE = _FROZEN_GATE_METADATA.forbidden_definition_universe
