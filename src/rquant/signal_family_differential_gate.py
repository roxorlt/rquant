"""Pure, bounded R07 differential-gate contracts.

This module deliberately operates on checked-in source text and declared fixtures. It never
imports production builder modules, constructs a registry, or follows object references.
"""

from __future__ import annotations

import ast
import base64
import hashlib
import stat
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Self

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

BASELINE_COMMIT_SHA = "45d0b57c4c5cbab1700fa5e3c386c6756892a7d6"
BASELINE_TREE_SHA = "4f67e67192855874e82baa13dc343a1d6939bd67"
_SHA256 = r"^[0-9a-f]{64}$"
_SHA1 = r"^[0-9a-f]{40}$"

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
    f"{_BOUNDARY_BEHAVIOR_FILE}::test_strategy_runner_rejects_a_substituted_current_constructor_without_row_or_byte_mutation",
    f"{_BOUNDARY_BEHAVIOR_FILE}::test_daily_signal_constructors_reject_current_family_substitution_without_mutation[summary]",
    f"{_BOUNDARY_BEHAVIOR_FILE}::test_r07_b03_sentinel_is_lazy_and_stops_before_first_yield",
    f"{_BOUNDARY_BEHAVIOR_FILE}::test_daily_signal_constructors_reject_current_family_substitution_without_mutation[cli-error]",
    f"{_BOUNDARY_BEHAVIOR_FILE}::test_daily_notification_emit_preflights_before_calling_the_bus",
    f"{_BOUNDARY_BEHAVIOR_FILE}::test_signal_bus_ingest_preflights_before_database_mutation",
    f"{_BOUNDARY_BEHAVIOR_FILE}::test_signal_bus_route_rejects_stored_current_before_outbox_mutation",
    f"{_BOUNDARY_BEHAVIOR_FILE}::test_signal_bus_commit_preflights_before_source_signal_receipt_or_outbox_mutation",
    f"{_BOUNDARY_BEHAVIOR_FILE}::test_route_runner_preflights_full_batch_before_bus_cursor_or_source_binding",
    f"{_BOUNDARY_BEHAVIOR_FILE}::test_signal_route_spool_publish_preflights_before_lock_source_record_or_pointer",
    f"{_BOUNDARY_BEHAVIOR_FILE}::test_signal_route_spool_prefix_preflights_before_initial_empty_publication",
    f"{_BOUNDARY_BEHAVIOR_FILE}::test_notification_replicate_preflights_all_records_before_transaction",
    f"{_BOUNDARY_BEHAVIOR_FILE}::test_paper_queue_ingest_forms_preflight_before_queue_transaction[direct]",
    f"{_BOUNDARY_BEHAVIOR_FILE}::test_paper_queue_ingest_forms_preflight_before_queue_transaction[stored-bytes]",
    f"{_BOUNDARY_BEHAVIOR_FILE}::test_signal_delivery_payload_rejects_current_before_authority_input_exists",
    f"{_BOUNDARY_BEHAVIOR_FILE}::test_publish_signal_authority_rejects_before_reader_or_publisher_callbacks",
    f"{_BOUNDARY_BEHAVIOR_FILE}::test_serving_authority_publisher_rejects_current_before_generation_or_pointer_files",
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
    result_digest: StrictStr = Field(pattern=_SHA256)
    outcome: Literal["passed"]

    @model_validator(mode="after")
    def validate_result(self) -> Self:
        if self.collected != self.passed or self.skipped != 0 or self.deselected != 0:
            raise ValueError("R07 Python run must be fully collected and passing")
        return self


class R07DrGateEvidenceV1(_StrictModelMixin, BaseModel):
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
    policy_digest: StrictStr = Field(pattern=_SHA256)
    complete_diff_digest: StrictStr = Field(pattern=_SHA256)
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
        if self.artifact_name != f"r07-dr-gate-{self.candidate_commit_sha}":
            raise ValueError("artifact name is not bound to candidate commit")
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
        return cls.model_validate_json(raw)

    @classmethod
    def with_digest(cls, **values: Any) -> Self:
        values["evidence_digest"] = "0" * 64
        provisional = cls.model_construct(**values)
        values["evidence_digest"] = _digest_without_field(provisional, "evidence_digest")
        return cls.model_validate(values)


def _digest_without_field(model: Any, field: str) -> str:
    payload = model.model_dump(mode="json", exclude={field})
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def canonical_evidence_json_bytes(value: R07DrGateEvidenceV1) -> bytes:
    if type(value) is not R07DrGateEvidenceV1:
        raise TypeError("R07 evidence requires the exact model")
    if value.evidence_digest != _digest_without_field(value, "evidence_digest"):
        raise ValueError("evidence_digest does not match canonical evidence")
    raw = canonical_json_bytes(value.model_dump(mode="json"))
    if R07DrGateEvidenceV1.from_canonical_json(raw) != value:
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
    entrypoint: StrictStr
    behavior_test: StrictStr
    current_fixture_id: StrictStr | None
    call_result_action: Literal["none", "consume_tuple"]
    expected_exception: StrictStr
    expected_exception_phase: Literal["invocation", "consumption"]
    sentinel_kind: Literal["constructor_identity_fence", "boundary_reached", "static_snapshot"]
    source_span: StrictStr
    boundary_ast_sha256: StrictStr = Field(pattern=_SHA256)
    expected_yielded_count: StrictInt = Field(ge=0)


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


class AllowedDiffEntryV1(_StrictModelMixin, BaseModel):
    path: StrictStr = Field(min_length=1)
    status: Literal["A", "M", "D", "T"]
    old_mode: StrictStr = Field(pattern=r"^[0-7]{6}$")
    new_mode: StrictStr = Field(pattern=r"^[0-7]{6}$")
    category: Literal["architecture", "production", "fixture", "test"]

    @property
    def policy_key(self) -> tuple[str, str, str, str]:
        return (self.path, self.status, self.old_mode, self.new_mode)


class EvidenceChannelV1(_StrictModelMixin, BaseModel):
    repository: Literal["roxorlt/rquant"]
    workflow_path: Literal[".github/workflows/ci.yml"]
    jobs: tuple[
        Literal[
            "r07-differential-gate-py311",
            "r07-differential-gate-py312",
            "r07-differential-gate-evidence",
        ],
        ...,
    ]
    artifact_json_path: Literal["r07-dr-gate/evidence-v1.json"]
    retention_days: Literal[90]


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
    boundary_probes: tuple[BoundaryProbeV1, ...]
    evidence_channel: EvidenceChannelV1
    policy_digest: StrictStr = Field(pattern=_SHA256)

    @model_validator(mode="after")
    def validate_digest(self) -> Self:
        if self.policy_digest != _digest_without_field(self, "policy_digest"):
            raise ValueError("policy_digest does not match canonical policy")
        return self

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json"))


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
    return CompleteDiffResult(
        baseline_commit_sha=baseline,
        candidate_commit_sha=candidate,
        entries=ordered,
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
    ancestry = subprocess.run(
        ["git", "-C", str(repo), "merge-base", "--is-ancestor", baseline, candidate],
        check=False,
        capture_output=True,
    )
    if ancestry.returncode != 0:
        raise ValueError("candidate commit does not descend from the policy baseline")
    complete = collect_complete_git_diff(
        repo,
        baseline_commit=baseline,
        candidate_commit=candidate,
        include_untracked=True,
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
        blocked_entries=blocked,
        missing_entries=missing,
    )


@dataclass(frozen=True)
class StaticCheckResult:
    passed: bool
    reasons: tuple[str, ...] = ()


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
    if tuple(probe.behavior_test for probe in policy.boundary_probes) != (
        EXPECTED_BOUNDARY_BEHAVIOR_TESTS
    ):
        reasons.append("boundary behavior-test universe mismatch")
    fixture_ids = {fixture.fixture_id for fixture in policy.current_fixtures}
    for probe in policy.boundary_probes:
        if probe.variant != "static_only" and probe.current_fixture_id not in fixture_ids:
            reasons.append(f"{probe.inventory_id}: missing current fixture")
    return StaticCheckResult(not reasons, tuple(reasons))


def _source_for(root: Path, module_path: str) -> str:
    return (root / module_path).read_text(encoding="utf-8")


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


def verify_root_snapshot(root: Path, snapshot: RootSnapshotV1) -> StaticCheckResult:
    try:
        source = _source_for(root, snapshot.module_path)
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
    ast_digest = hashlib.sha256(ast.dump(node, include_attributes=False).encode()).hexdigest()
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
    root: Path,
    declaration: ProductionDeclarationV1,
) -> StaticCheckResult:
    try:
        source = _source_for(root, declaration.module_path)
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
    observed_digest = hashlib.sha256(ast.dump(node, include_attributes=False).encode()).hexdigest()
    reasons: list[str] = []
    if observed_span != declaration.source_span:
        reasons.append("production declaration source span drift")
    if observed_digest != declaration.normalized_ast_sha256:
        reasons.append("production declaration AST drift")
    return StaticCheckResult(not reasons, tuple(reasons))


def verify_boundary_probe_source(root: Path, probe: BoundaryProbeV1) -> StaticCheckResult:
    try:
        filename, line_text = probe.source_span.rsplit(":", 1)
        line = int(line_text)
        module_path = f"src/rquant/{filename}"
        source = _source_for(root, module_path)
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
    observed = hashlib.sha256(ast.dump(node, include_attributes=False).encode()).hexdigest()
    if observed != probe.boundary_ast_sha256:
        return StaticCheckResult(False, ("boundary AST snapshot drift",))
    return StaticCheckResult(True)


def _signature_text(node: ast.AST) -> str:
    if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return ""
    return ast.unparse(node.args)


def verify_forbidden_definitions(
    root: Path, universe: ForbiddenDefinitionUniverseV1
) -> StaticCheckResult:
    reasons: list[str] = []
    forbidden = set(universe.symbols) | set(universe.exports) | set(universe.registry_keys)
    for module_path in universe.source_files:
        try:
            tree = ast.parse(_source_for(root, module_path), filename=module_path)
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
