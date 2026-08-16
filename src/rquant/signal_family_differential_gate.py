"""Pure, bounded R07 differential-gate contracts.

This module deliberately operates on checked-in source text and declared fixtures. It never
imports production builder modules, constructs a registry, or follows object references.
"""

from __future__ import annotations

import ast
import base64
import hashlib
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

BASELINE_COMMIT_SHA = "03cbf1d617ff20264cf700a0305f59541700148c"
BASELINE_TREE_SHA = "c5379dd7e450e2b70392b0fd9767a3bcdc6bc9ed"
_SHA256 = r"^[0-9a-f]{64}$"
_SHA1 = r"^[0-9a-f]{40}$"


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
    positional_fixture_ids: tuple[StrictStr, ...]
    keyword_fixture_ids: dict[StrictStr, StrictStr]
    current_member_fixture_ids: tuple[StrictStr, ...]
    call_shape: CallShapeV1
    expected_exception: StrictStr
    expected_exception_phase: Literal["invocation", "consumption"]
    sentinel_id: StrictStr
    sentinel_kind: Literal["constructor_identity_fence", "boundary_reached", "static_snapshot"]
    mutation_guard_expectation: Literal["zero"]
    before_snapshot_digest: StrictStr = Field(pattern=_SHA256)
    after_snapshot_digest: StrictStr = Field(pattern=_SHA256)
    source_span: StrictStr
    expected_yielded_count: StrictInt = Field(ge=0)
    current_fixture: CurrentFixtureV1
    setup: ProbeSetupV1


class RootSnapshotV1(_StrictModelMixin, BaseModel):
    module_path: StrictStr
    qualname: StrictStr
    signature: StrictStr
    source_sha256: StrictStr = Field(pattern=_SHA256)
    ast_sha256: StrictStr = Field(pattern=_SHA256)
    exports: tuple[StrictStr, ...]


class ForbiddenDefinitionUniverseV1(_StrictModelMixin, BaseModel):
    source_files: tuple[StrictStr, ...]
    symbols: tuple[StrictStr, ...]
    exports: tuple[StrictStr, ...]
    registry_keys: tuple[StrictStr, ...]


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
    allowed_diff_paths: tuple[StrictStr, ...]
    production_declarations: tuple[StrictStr, ...]
    fixtures: tuple[FixtureValueV1, ...]
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
    return R07PolicyV1.model_validate_json(raw)


@dataclass(frozen=True)
class CompleteDiffResult:
    entries: tuple[tuple[str, str], ...]
    blocked_paths: tuple[str, ...]

    @property
    def digest(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.entries)).hexdigest()


def collect_complete_git_diff(
    repo: Path,
    *,
    allowed_paths: set[str],
    baseline_ref: str = "HEAD",
) -> CompleteDiffResult:
    command = [
        "git",
        "-C",
        str(repo),
        "diff",
        "--name-status",
        "--no-renames",
        baseline_ref,
    ]
    completed = subprocess.run(command, check=True, capture_output=True, text=True)
    entries = [
        (line.split("\t", 1)[0], line.split("\t", 1)[1])
        for line in completed.stdout.splitlines()
        if "\t" in line
    ]
    untracked = subprocess.run(
        ["git", "-C", str(repo), "ls-files", "--others", "--exclude-standard"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    entries.extend(("?", path) for path in untracked)
    ordered = tuple(sorted(entries, key=lambda item: (item[1], item[0])))
    return CompleteDiffResult(
        entries=ordered,
        blocked_paths=tuple(path for _status, path in ordered if path not in allowed_paths),
    )


@dataclass(frozen=True)
class StaticCheckResult:
    passed: bool
    reasons: tuple[str, ...] = ()


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
        found = sorted(forbidden & (identifiers | aliases))
        if found:
            reasons.append(f"{module_path}: {','.join(found)}")
    return StaticCheckResult(not reasons, tuple(reasons))


def _empty_current_fixture() -> CurrentFixtureV1:
    payload = b"r07-current-fixture"
    return CurrentFixtureV1(
        fixture_id="current.object",
        current_model_module="rquant.signal_contracts",
        current_model_qualname="CurrentSignalEnvelope",
        canonical_model_bytes=base64.b64encode(payload).decode(),
        parser_module="rquant.signal_contracts",
        parser_qualname="parse_signal_envelope",
        parsed_model_digest=hashlib.sha256(payload).hexdigest(),
        allowed_form="object",
    )


def _probe(
    index: int, *, variant: str = "direct_current", phase: str = "invocation"
) -> BoundaryProbeV1:
    source_anchors = {
        1: (
            "constructor_identity",
            "rquant.strategy_runner.StrategyRunnerStore.process_batch",
            "strategy_runner.py:1769",
        ),
        2: (
            "constructed_current",
            "rquant.daily_summary_stage.DailySummaryStage.build_signal",
            "daily_summary_stage.py:130",
        ),
        3: (
            "constructed_current",
            "rquant.daily_summary_stage.DailySummaryStage._error_signals",
            "daily_summary_stage.py:264",
        ),
        4: (
            "constructed_current",
            "rquant.daily_notification_producer.build_daily_error_signal",
            "daily_notification_producer.py:41",
        ),
        5: (
            "direct_current",
            "rquant.daily_notification_producer.DailyNotificationProducer.emit",
            "daily_notification_producer.py:113",
        ),
        6: ("direct_current", "rquant.signal_bus.SignalBusStore.ingest", "signal_bus.py:594"),
        7: ("stored_byte_current", "rquant.signal_bus.SignalBusStore.route", "signal_bus.py:974"),
        8: (
            "direct_current",
            "rquant.signal_bus.SignalBusStore.commit_source_route",
            "signal_bus.py:1278",
        ),
        9: (
            "source_result_current",
            "rquant.signal_router_runtime.route_runner_signals",
            "signal_router_runtime.py:1207",
        ),
        10: (
            "batch_current",
            "rquant.signal_route_spool.SignalRouteSpool.publish",
            "signal_route_spool.py:828",
        ),
        11: (
            "source_result_current",
            "rquant.signal_route_spool.publish_signal_bus_prefix",
            "signal_route_spool.py:1084",
        ),
        12: (
            "batch_current",
            "rquant.notification_state.NotificationStateStore.replicate",
            "notification_state.py:567",
        ),
        13: (
            "direct_current",
            "rquant.paper_signal_worker.PaperSignalQueueStore.ingest",
            "paper_signal_worker.py:526",
        ),
        14: (
            "stored_byte_current",
            "rquant.paper_signal_worker.PaperSignalQueueStore.ingest",
            "paper_signal_worker.py:526",
        ),
        15: (
            "batch_current",
            "rquant.runtime_serving_snapshot.SignalDeliveryPayload",
            "runtime_serving_snapshot.py:56",
        ),
        16: (
            "source_result_current",
            "rquant.runtime_builder_signal._publish_signal_authority",
            "runtime_builder_signal.py:453",
        ),
        17: (
            "direct_current",
            "rquant.runtime_serving_authority.ServingSourceAuthorityPublisher.publish",
            "runtime_serving_authority.py:129",
        ),
        18: (
            "static_only",
            "rquant.runtime_service_main.build_builtin_registry",
            "runtime_service_main.py:123",
        ),
        19: (
            "static_only",
            "rquant.runtime_service_builtin.build_builtin_registry",
            "runtime_service_builtin.py:1125",
        ),
    }
    inventory_id = f"R07-B{index:02d}"
    declared_variant, entrypoint, source_span = source_anchors[index]
    variant = declared_variant
    action = "consume_tuple" if index == 3 else "none"
    phase = "consumption" if index == 3 else "invocation"
    setup = ProbeSetupV1(
        setup_id=f"setup-{inventory_id.lower()}",
        steps=(ProbeSetupStepV1(kind="receiver", target=inventory_id, expected_binding="exact"),),
        setup_result_digest="a" * 64,
    )
    shape = CallShapeV1(
        receiver_fixture_id=None,
        positional_fixture_ids=(),
        keyword_fixture_ids={},
        call_result_action=action,
    )
    return BoundaryProbeV1(
        probe_id=f"probe-{inventory_id.lower()}",
        inventory_id=inventory_id,
        variant=variant,
        setup_id=setup.setup_id,
        entrypoint=entrypoint,
        positional_fixture_ids=(),
        keyword_fixture_ids={},
        current_member_fixture_ids=("current.object",),
        call_shape=shape,
        expected_exception="LegacySignalWriteActivationError",
        expected_exception_phase=phase,
        sentinel_id=f"sentinel-{inventory_id.lower()}",
        sentinel_kind="static_snapshot" if variant == "static_only" else "boundary_reached",
        mutation_guard_expectation="zero",
        before_snapshot_digest="b" * 64,
        after_snapshot_digest="b" * 64,
        source_span=source_span,
        expected_yielded_count=0,
        current_fixture=_empty_current_fixture(),
        setup=setup,
    )


BOUNDARY_PROBES = (*(_probe(index) for index in range(1, 20)),)

FixtureValue = FixtureValueV1
CurrentFixture = CurrentFixtureV1
ProbeSetup = ProbeSetupV1
CallShape = CallShapeV1
BoundaryReachedSentinel = BoundaryReachedSentinelV1
BoundaryProbe = BoundaryProbeV1


ROOT_SNAPSHOTS: tuple[RootSnapshotV1, ...] = ()
FORBIDDEN_DEFINITION_UNIVERSE = ForbiddenDefinitionUniverseV1(
    source_files=("src/rquant/signal_route_spool.py",),
    symbols=("SignalRouteSpoolV3Writer", "publish_v3", "r07_activation"),
    exports=(),
    registry_keys=(),
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
    ROOT_SNAPSHOTS = _FROZEN_GATE_METADATA.root_snapshots
    FORBIDDEN_DEFINITION_UNIVERSE = _FROZEN_GATE_METADATA.forbidden_definition_universe
