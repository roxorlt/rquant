"""One exercise function per reader surface, each entered through its production builder.

`authority.md` L1226-L1227 is the rule this module exists to obey: *child verification
starts through the actual manifest-backed production builders; direct unit construction
cannot substitute for that path*. So every exercise here does the same four things:

1. materialize exactly what the vector declared, inside the vector's own scratch tree;
2. hand the resulting manifest to the real `runtime_builder_*` factory the production
   registry uses for that service kind;
3. take the collaborator the builder constructed out of the step closure it captured, and
   bind the frozen surface to it, proving `bound.__func__` is the object ruling O2 resolved;
4. call it with the vector's arguments and project the return into bounded canonical JSON.

Step three is the part that cannot be faked. `paper_broker_builder` and its siblings build
their readers during `build(manifest)` and close over them, so reading the cell by name is
how the child gets *the builder's* object rather than one of its own.

Three of the thirteen reader surfaces are exercised. `BLOCKED_SURFACE_REASONS` names the
other ten and says exactly what stops each of them; a vector that asks for one is refused
rather than answered with a substitute. Every blocked reason is a property of the child the
root actually launches — a sanitized eight-key environment and a `mkdtemp` cwd under a
sticky temp root — not of a development machine, so none of them can be papered over by
running the exercise somewhere friendlier.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Final

from ._canonical import canonical_json_bytes, canonical_sha256
from ._request import RequestVector
from ._resolve import bound_surface, resolve_surface
from ._workspace import VectorWorkspace, WorkspaceError, tree_digest

_ENVELOPE_KEYS: Final[tuple[str, ...]] = (
    "call",
    "observed_at",
    "schema_version",
    "service",
    "state",
)
_SERVICE_KEYS: Final[tuple[str, ...]] = (
    "interval_seconds",
    "plane",
    "producer_commit",
    "service_id",
    "service_kind",
    "settings",
    "stale_after_seconds",
)

#: The ten reader surfaces this harness version refuses to answer, and why. Each reason is a
#: delivery gap recorded in `wp4c-report.md`, not a silent omission: the child rejects the
#: whole run rather than returning a substitute observation.
BLOCKED_SURFACE_REASONS: Final[dict[str, str]] = {
    "rquant.signal_route_spool.ReadonlySignalRouteSpool.routed_after_global_sequence": (
        "notifier_builder always imports rquant.runtime_notification_providers when no "
        "provider loader is injected, and that module reaches rquant.config, which builds a "
        "process-wide Settings at import and demands DATA_DIR / DUCKDB_PATH / PARQUET_DIR / "
        "LOG_DIR / TUSHARE_TOKEN_MAIN; the child's frozen eight-key environment carries none "
        "of them"
    ),
    "rquant.notification_state.NotificationStateStore.replicate": (
        "the store is only reachable through notifier_builder, so it is blocked by the same "
        "import-time rquant.config Settings construction"
    ),
    "rquant.signal_router_runtime.ReadonlyStrategyRunnerSignalSource.read_batch": (
        "the reader opens a runner-state SQLite whose audited runner_metadata / "
        "runner_source_identity / runner_signal schema only StrategyRunnerStore.process_batch "
        "can legitimately produce, and that producer needs a full StrategySpec, "
        "FeatureBatchEnvelope, feature frame, and StrategyEvaluator that no bounded vector "
        "carries"
    ),
    "rquant.signal_router_runtime.route_runner_signals": (
        "the router step reads through the same runner-state SQLite as read_batch, so it is "
        "blocked by the same missing producer path"
    ),
    "rquant.runtime_builder_shadow._FilesystemRunnerSource.read_completed_batch": (
        "FilesystemShadowSessionInputLoader.load only constructs the source from an accepted "
        "legacy shadow export, which requires Ed25519 completion and report attestations the "
        "harness cannot mint without holding signing keys"
    ),
    "rquant.runtime_shadow_sources.read_isolated_runner_shadow_snapshot": (
        "same accepted-export precondition as _FilesystemRunnerSource.read_completed_batch"
    ),
    "rquant.runtime_shadow_sources.isolated_signal_observations": (
        "same accepted-export precondition as _FilesystemRunnerSource.read_completed_batch"
    ),
    "rquant.runtime_serving_authority.ServingSourceAuthorityReader.__call__": (
        "runtime_serving_authority walks the authority root's whole parent chain and refuses "
        "any group- or world-writable ancestor; the child's cwd is a mkdtemp under a sticky "
        "1777 temp root, so no authority can be materialized inside the isolated cwd"
    ),
    "rquant.runtime_serving_snapshot.ServingSnapshotAssembler.assemble": (
        "the assembler is only built once six source authorities exist, which the sticky-cwd "
        "parent-chain rule blocks"
    ),
    "rquant.serving_read_models.build_serving_read_models": (
        "its input is the assembler's snapshot, which the sticky-cwd parent-chain rule blocks"
    ),
}


class SurfaceExerciseError(ValueError):
    """A vector could not be exercised through its production builder."""


@dataclass(frozen=True)
class ExerciseContext:
    """Everything one exercise function is allowed to see. No expected result appears."""

    vector: RequestVector
    workspace: VectorWorkspace
    payload: Mapping[str, Any]
    observed_at: datetime
    live_root: Path

    @property
    def call(self) -> Mapping[str, Any]:
        arguments = self.payload["call"]
        if type(arguments) is not dict:
            raise SurfaceExerciseError("vector call must be a JSON object")
        return arguments

    def integer(self, name: str) -> int:
        value = self.call.get(name)
        if type(value) is not int:
            raise SurfaceExerciseError(f"vector call.{name} must be an integer")
        return value


@dataclass(frozen=True)
class ExerciseOutcome:
    """What the surface observed, plus the runtime digest taken just before the call."""

    observation: dict[str, Any]
    runtime_digest_before_call: str


@dataclass(frozen=True)
class SurfaceSpec:
    """How one frozen surface is reached and what it is allowed to disturb."""

    builder: str
    service_kind: str
    writes: bool
    exercise: Callable[[ExerciseContext], ExerciseOutcome]


# ---------------------------------------------------------------------------------------
# Manifest and builder plumbing
# ---------------------------------------------------------------------------------------


def _service_manifest(context: ExerciseContext, spec: SurfaceSpec) -> Any:
    from rquant.runtime_service_control import RuntimeServicePlane
    from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest

    service = context.payload["service"]
    if type(service) is not dict or tuple(sorted(service)) != _SERVICE_KEYS:
        raise SurfaceExerciseError("vector service block is not the exact frozen set")
    if service["service_kind"] != spec.service_kind:
        raise SurfaceExerciseError("vector service kind does not own that reader surface")
    if type(service["settings"]) is not dict:
        raise SurfaceExerciseError("vector service settings must be a JSON object")
    try:
        return RuntimeServiceManifest(
            service_id=service["service_id"],
            service_kind=RuntimeServiceKind(service["service_kind"]),
            plane=RuntimeServicePlane(service["plane"]),
            interval_seconds=service["interval_seconds"],
            stale_after_seconds=service["stale_after_seconds"],
            producer_commit=service["producer_commit"],
            settings=context.workspace.rebase(service["settings"], live=context.live_root),
        )
    except (TypeError, ValueError) as exc:
        raise SurfaceExerciseError("vector service block is not a valid runtime manifest") from exc


def _closure_value(step: Any, name: str, expected: type[Any]) -> Any:
    """Take one collaborator the production builder constructed out of its step closure."""

    code = getattr(step, "__code__", None)
    cells = getattr(step, "__closure__", None)
    if code is None or cells is None:
        raise SurfaceExerciseError("the production builder did not return a closure step")
    names = tuple(code.co_freevars)
    if name not in names:
        raise SurfaceExerciseError(f"the production builder step does not capture {name}")
    value = cells[names.index(name)].cell_contents
    if type(value) is not expected:
        raise SurfaceExerciseError(
            f"the production builder captured {type(value).__name__} as {name}"
        )
    return value


def _paper_broker_step(context: ExerciseContext, spec: SurfaceSpec) -> Any:
    from rquant.runtime_builder_paper import paper_broker_builder

    manifest = _service_manifest(context, spec)
    try:
        return paper_broker_builder(clock=lambda: context.observed_at)(manifest)
    except (TypeError, ValueError) as exc:
        raise SurfaceExerciseError(
            "the paper broker production builder refused the vector"
        ) from exc


# ---------------------------------------------------------------------------------------
# Bounded projections
# ---------------------------------------------------------------------------------------


def _records_projection(records: tuple[Any, ...]) -> dict[str, Any]:
    """A record tuple is summarized, never inlined: the result budget is 65,536 bytes."""

    dumps = [record.model_dump(mode="json") for record in records]
    return {
        "count": len(records),
        "global_sequences": [record.global_sequence for record in records],
        "records_sha256": canonical_sha256(dumps),
        "signal_ids": [record.signal_id for record in records],
    }


def _read_bounds(context: ExerciseContext) -> dict[str, int]:
    return {
        "after_sequence": context.integer("after_sequence"),
        "through_sequence": context.integer("through_sequence"),
        "limit": context.integer("limit"),
    }


# ---------------------------------------------------------------------------------------
# router-paper
# ---------------------------------------------------------------------------------------


def _exercise_signals_after_global_sequence(context: ExerciseContext) -> ExerciseOutcome:
    from rquant.signal_route_spool import ReadonlySignalRouteSpool

    spec = SURFACE_SPECS[context.vector.surface_id]
    step = _paper_broker_step(context, spec)
    source = _closure_value(step, "source", ReadonlySignalRouteSpool)
    runtime_digest = tree_digest(context.workspace.runtime)
    bounds = _read_bounds(context)
    surface = bound_surface(source, context.vector.surface_id)
    records = surface(observed_at=context.observed_at, **bounds)
    return ExerciseOutcome(
        observation={
            "bounds": bounds,
            "source_descriptor": source.source_descriptor().model_dump(mode="json"),
            **_records_projection(records),
        },
        runtime_digest_before_call=runtime_digest,
    )


def _exercise_consume_signal_bus_to_paper(context: ExerciseContext) -> ExerciseOutcome:
    from rquant.paper_signal_consumer import PaperSignalConsumerStateStore
    from rquant.paper_signal_worker import PaperSignalQueueStore
    from rquant.signal_route_spool import ReadonlySignalRouteSpool

    spec = SURFACE_SPECS[context.vector.surface_id]
    step = _paper_broker_step(context, spec)
    source = _closure_value(step, "source", ReadonlySignalRouteSpool)
    queue = _closure_value(step, "queue", PaperSignalQueueStore)
    state = _closure_value(step, "state", PaperSignalConsumerStateStore)
    runtime_digest = tree_digest(context.workspace.runtime)
    surface = resolve_surface(context.vector.surface_id)
    summary = surface(
        source,
        queue,
        state,
        observed_at=context.observed_at,
        limit=context.integer("limit"),
    )
    return ExerciseOutcome(
        observation={
            "collaborators": [
                type(source).__name__,
                type(queue).__name__,
                type(state).__name__,
            ],
            "limit": context.integer("limit"),
            "summary": summary.model_dump(mode="json"),
        },
        runtime_digest_before_call=runtime_digest,
    )


def _exercise_paper_queue_ingest(context: ExerciseContext) -> ExerciseOutcome:
    from rquant.paper_signal_worker import PaperSignalQueueStore
    from rquant.signal_route_spool import ReadonlySignalRouteSpool

    spec = SURFACE_SPECS[context.vector.surface_id]
    step = _paper_broker_step(context, spec)
    source = _closure_value(step, "source", ReadonlySignalRouteSpool)
    queue = _closure_value(step, "queue", PaperSignalQueueStore)
    bounds = _read_bounds(context)
    selected = context.integer("global_sequence")
    records = source.signals_after_global_sequence(observed_at=context.observed_at, **bounds)
    matches = [record for record in records if record.global_sequence == selected]
    if len(matches) != 1:
        raise SurfaceExerciseError("vector call.global_sequence names no single spool record")
    record = matches[0]
    runtime_digest = tree_digest(context.workspace.runtime)
    surface = bound_surface(queue, context.vector.surface_id)
    ingested = surface(
        record.signal,
        received_at=context.observed_at,
        payload_json=record.payload_json,
        payload_hash=record.payload_hash,
        payload_size=len(record.payload_json.encode("utf-8")),
    )
    return ExerciseOutcome(
        observation={
            "bounds": bounds,
            "global_sequence": selected,
            "queue_record": ingested.model_dump(mode="json"),
        },
        runtime_digest_before_call=runtime_digest,
    )


SURFACE_SPECS: Final[dict[str, SurfaceSpec]] = {
    "rquant.signal_route_spool.ReadonlySignalRouteSpool.signals_after_global_sequence": SurfaceSpec(
        builder="rquant.runtime_builder_paper.paper_broker_builder",
        service_kind="paper_broker",
        writes=False,
        exercise=_exercise_signals_after_global_sequence,
    ),
    "rquant.paper_signal_consumer.consume_signal_bus_to_paper": SurfaceSpec(
        builder="rquant.runtime_builder_paper.paper_broker_builder",
        service_kind="paper_broker",
        writes=True,
        exercise=_exercise_consume_signal_bus_to_paper,
    ),
    "rquant.paper_signal_worker.PaperSignalQueueStore.ingest": SurfaceSpec(
        builder="rquant.runtime_builder_paper.paper_broker_builder",
        service_kind="paper_broker",
        writes=True,
        exercise=_exercise_paper_queue_ingest,
    ),
}

IMPLEMENTED_SURFACE_IDS: Final[tuple[str, ...]] = tuple(sorted(SURFACE_SPECS))


# ---------------------------------------------------------------------------------------
# Materialization and the one entry point
# ---------------------------------------------------------------------------------------


def _publish_declared_spool(workspace: VectorWorkspace, declared: Mapping[str, Any]) -> None:
    """Materialize a routed-signal spool through the pair's own producer surface."""

    from rquant.signal_bus import SignalBusRoutedRecord, SignalBusSourceDescriptor
    from rquant.signal_route_spool import SignalRouteSpool

    if type(declared) is not dict or tuple(sorted(declared)) != ("records", "root", "source"):
        raise SurfaceExerciseError("vector spool state must be exactly {records, root, source}")
    if type(declared["root"]) is not str:
        raise SurfaceExerciseError("vector spool root must be a string path")
    if type(declared["records"]) is not list:
        raise SurfaceExerciseError("vector spool records must be an array")
    root = workspace.declared_path(declared["root"], live=workspace.state)
    try:
        descriptor = SignalBusSourceDescriptor.model_validate(declared["source"])
        records = tuple(
            SignalBusRoutedRecord.model_validate(record) for record in declared["records"]
        )
        SignalRouteSpool(root).publish(source=descriptor, records=records)
    except (TypeError, ValueError) as exc:
        raise SurfaceExerciseError("vector spool state is not a publishable prefix") from exc


def _parse_observed_at(value: Any) -> datetime:
    from rquant.runtime_contracts import normalize_aware_utc

    if type(value) is not str:
        raise SurfaceExerciseError("vector observed_at must be an RFC 3339 string")
    try:
        return normalize_aware_utc(datetime.fromisoformat(value))
    except (TypeError, ValueError) as exc:
        raise SurfaceExerciseError("vector observed_at is not an aware UTC timestamp") from exc


def exercise_vector(vector: RequestVector, root: Path) -> dict[str, Any]:
    """Run one vector through its production builder and return its canonical result."""

    surface_id = vector.surface_id
    if surface_id in BLOCKED_SURFACE_REASONS:
        raise SurfaceExerciseError(
            f"{surface_id} is not exercised by this harness: {BLOCKED_SURFACE_REASONS[surface_id]}"
        )
    spec = SURFACE_SPECS.get(surface_id)
    if spec is None:
        raise SurfaceExerciseError(f"{surface_id} has no exercise in this harness")

    payload = vector.parsed_input()
    if type(payload) is not dict or tuple(sorted(payload)) != _ENVELOPE_KEYS:
        raise SurfaceExerciseError("vector input is not the exact frozen envelope")
    if type(payload["schema_version"]) is not int or payload["schema_version"] != 1:
        raise SurfaceExerciseError("unsupported vector input schema version")
    declared_state = payload["state"]
    if type(declared_state) is not dict:
        raise SurfaceExerciseError("vector state must be a JSON object")

    workspace = VectorWorkspace(root, vector.vector_id)
    try:
        workspace.materialize(declared_state)
    except WorkspaceError as exc:
        raise SurfaceExerciseError(str(exc)) from exc
    spool = declared_state.get("spool")
    if spool is not None:
        _publish_declared_spool(workspace, spool)

    state_before = tree_digest(workspace.state)
    context = ExerciseContext(
        vector=vector,
        workspace=workspace,
        payload=payload,
        observed_at=_parse_observed_at(payload["observed_at"]),
        live_root=workspace.live_root(writes=spec.writes),
    )
    outcome = spec.exercise(context)
    state_after = tree_digest(workspace.state)
    runtime_after = tree_digest(workspace.runtime)
    if state_before != state_after:
        raise SurfaceExerciseError("the surface modified the vector's materialized declaration")
    runtime_unchanged = outcome.runtime_digest_before_call == runtime_after
    if not spec.writes and not runtime_unchanged:
        raise SurfaceExerciseError("a read-only surface modified the builder's runtime tree")
    result: dict[str, Any] = {
        "schema_version": 1,
        "builder": spec.builder,
        "declared_writer": spec.writes,
        "observation": outcome.observation,
        "runtime_unchanged": runtime_unchanged,
        "state_unchanged": True,
        "surface_id": surface_id,
    }
    canonical_json_bytes(result)
    return result


__all__ = [
    "BLOCKED_SURFACE_REASONS",
    "IMPLEMENTED_SURFACE_IDS",
    "SURFACE_SPECS",
    "ExerciseContext",
    "ExerciseOutcome",
    "SurfaceExerciseError",
    "SurfaceSpec",
    "exercise_vector",
]
