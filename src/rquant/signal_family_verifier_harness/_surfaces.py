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

Step three is the part that cannot be faked. `notifier_builder`, `paper_broker_builder`,
and `serving_publisher_builder` build their readers during `build(manifest)` and close over
them, so reading the cell by name is how the child gets *the builder's* object rather than
one of its own.

All thirteen reader surfaces are exercised. Five of them need producer-side state no bounded
vector can carry, and they get it through the ruling E-1 fixture channel: the immutable test
manifest names in-generation, full-manifest covered, policy-hashed producer artifacts, and
the root checks every one of them before this child starts and again after it exits.

The two `strategy-router` readers take the copying form — a canonical runner-state SQL dump
and a frozen routing policy are copied into the vector's own tree and the database is rebuilt
there. The three `strategy-shadow` readers take the in-place form: an accepted legacy shadow
export binds its Ed25519 recovery marker to the session directory's `st_dev`/`st_ino`, so a
copy is a different directory and the production reader refuses it. Those vectors name
`@generation/…` paths and the surfaces read the export where it was published.

Both fixture sets are producer output. `scripts/build-signal-family-producer-fixtures.py`
drives the real `StrategyRunnerStore.process_batch`;
`scripts/build-signal-family-shadow-fixture.py` drives that plus `route_runner_signals`,
`ReadonlySignalRouteAuthority.read_drain_evidence`, and
`StrategyRunnerStore.publish_session_close_receipt` before publishing through the real
export publishers. Neither writes a table by hand, and neither puts a signing key anywhere
this child can reach.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Final

from ._canonical import canonical_json_bytes, canonical_sha256, sha256_hex
from ._request import AuthorizedGenerationFile, RequestVector
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

#: Reader surfaces this harness version refuses to answer, and why. The map is empty: all
#: thirteen are exercised. It stays as the fail-closed mechanism — a surface added to the
#: frozen allowlist without an exercise here is refused rather than answered with a
#: substitute — and `tests/unit/test_signal_family_verifier_harness.py` pins it at empty.
BLOCKED_SURFACE_REASONS: Final[dict[str, str]] = {}



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

    def text(self, name: str) -> str:
        value = self.call.get(name)
        if type(value) is not str or not value:
            raise SurfaceExerciseError(f"vector call.{name} must be a nonempty string")
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
        settings = context.workspace.rebase(service["settings"], live=context.live_root)
    except WorkspaceError as exc:
        # A path the workspace refuses is a bounded, specific refusal; folding it into the
        # generic "not a valid runtime manifest" below would hide which path and why.
        raise SurfaceExerciseError(str(exc)) from exc
    try:
        return RuntimeServiceManifest(
            service_id=service["service_id"],
            service_kind=RuntimeServiceKind(service["service_kind"]),
            plane=RuntimeServicePlane(service["plane"]),
            interval_seconds=service["interval_seconds"],
            stale_after_seconds=service["stale_after_seconds"],
            producer_commit=service["producer_commit"],
            settings=settings,
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


def _closure_callable(step: Any, name: str) -> Any:
    """Take one callable collaborator the production builder resolved into its closure.

    `_closure_value` pins an exact class, which is the stronger check where the builder keeps
    an object. A router builder keeps *functions* — the source loader it wired and the frozen
    routing policy resolver — so the type check here is that the cell holds something callable
    and that its identity came from the builder rather than from this module.
    """

    code = getattr(step, "__code__", None)
    cells = getattr(step, "__closure__", None)
    if code is None or cells is None:  # pragma: no cover - builders return closures
        raise SurfaceExerciseError("the production builder did not return a closure step")
    names = tuple(code.co_freevars)
    if name not in names:
        raise SurfaceExerciseError(f"the production builder step does not capture {name}")
    value = cells[names.index(name)].cell_contents
    if not callable(value):
        raise SurfaceExerciseError(f"the production builder captured a noncallable {name}")
    return value


def _notifier_step(context: ExerciseContext, spec: SurfaceSpec) -> Any:
    from rquant.runtime_builder_signal import notifier_builder

    manifest = _service_manifest(context, spec)
    try:
        return notifier_builder(clock=lambda: context.observed_at)(manifest)
    except (TypeError, ValueError) as exc:
        raise SurfaceExerciseError("the notifier production builder refused the vector") from exc


def _serving_publisher_step(context: ExerciseContext, spec: SurfaceSpec) -> Any:
    from rquant.runtime_builder_serving import serving_publisher_builder

    manifest = _service_manifest(context, spec)
    try:
        return serving_publisher_builder(
            snapshot_loader=None,
            clock=lambda: context.observed_at,
        )(manifest)
    except (TypeError, ValueError) as exc:
        raise SurfaceExerciseError(
            "the serving publisher production builder refused the vector"
        ) from exc


def _closure_assembler(step: Any) -> Any:
    """The assembler the serving builder wired, taken through its own bound loader.

    `serving_publisher_builder` keeps only `assembler.assemble` in the closure, which is the
    strongest form of this evidence available: the bound method carries both the object the
    builder constructed and the function it is bound to.
    """

    from rquant.runtime_serving_snapshot import ServingSnapshotAssembler

    code = getattr(step, "__code__", None)
    cells = getattr(step, "__closure__", None)
    if code is None or cells is None:  # pragma: no cover - builders return closures
        raise SurfaceExerciseError("the production builder did not return a closure step")
    names = tuple(code.co_freevars)
    if "resolved_snapshot_loader" not in names:
        raise SurfaceExerciseError("the serving builder step does not capture its loader")
    loader = cells[names.index("resolved_snapshot_loader")].cell_contents
    assembler = getattr(loader, "__self__", None)
    if type(assembler) is not ServingSnapshotAssembler:
        raise SurfaceExerciseError(
            "the serving builder did not wire a ServingSnapshotAssembler; an injected "
            "snapshot loader cannot substitute for the owner-authority path"
        )
    if loader.__func__ is not resolve_surface(
        "rquant.runtime_serving_snapshot.ServingSnapshotAssembler.assemble"
    ):
        raise SurfaceExerciseError("the serving builder's loader is not the frozen assemble")
    return assembler


def _signal_router_step(context: ExerciseContext, spec: SurfaceSpec) -> Any:
    """Build the router in its default manifest-authority mode, with nothing injected.

    `signal_router_builder` refuses to combine injected dependencies with manifest authority,
    so passing neither is the only way to reach the branch that constructs a real
    `ReadonlyStrategyRunnerSignalSource` over the runner-state database and loads the frozen
    routing policy from disk. That branch is the production one.
    """

    from rquant.runtime_builder_signal import signal_router_builder

    manifest = _service_manifest(context, spec)
    try:
        return signal_router_builder(clock=lambda: context.observed_at)(manifest)
    except (TypeError, ValueError) as exc:
        raise SurfaceExerciseError(
            "the signal router production builder refused the vector"
        ) from exc


def _router_source(context: ExerciseContext, step: Any) -> Any:
    """The reader the router builder wired for the vector's own source id."""

    from rquant.signal_router_runtime import ReadonlyStrategyRunnerSignalSource

    loader = _closure_callable(step, "resolved_source_loader")
    try:
        source = loader(context.text("source_id"))
    except (KeyError, TypeError, ValueError) as exc:
        raise SurfaceExerciseError("the router builder has no source for that id") from exc
    if type(source) is not ReadonlyStrategyRunnerSignalSource:
        raise SurfaceExerciseError(
            "the router builder did not wire a ReadonlyStrategyRunnerSignalSource; an "
            "injected source loader cannot substitute for the manifest-authority path"
        )
    return source


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


#: `ServingSnapshotAssembler` keeps one reader per owner dataset; the vector names which
#: one the `__call__` surface is exercised on.
_READER_ATTRIBUTE_BY_DATASET: Final[dict[str, str]] = {
    "lab_jobs": "lab_jobs_reader",
    "paper_accounts": "paper_accounts_reader",
    "promotions": "promotions_reader",
    "reference_slow_authority": "reference_slow_reader",
    "runtime_health": "runtime_health_reader",
    "signals": "signal_reader",
}


def _snapshot_projection(snapshot: Any) -> dict[str, Any]:
    """A serving snapshot carries every owner payload, so only its identity is returned."""

    return {
        "observed_at": snapshot.read_model.observed_at.isoformat().replace("+00:00", "Z"),
        "snapshot_sha256": canonical_sha256(snapshot.model_dump(mode="json")),
        "source_generations": dict(snapshot.source_generations),
        "watermarks": [
            {
                "dataset_id": watermark.dataset_id,
                "generation_id": watermark.generation_id,
                "sequence": watermark.sequence,
                "status": watermark.status.value,
            }
            for watermark in snapshot.watermarks
        ],
    }


def _read_bounds(context: ExerciseContext) -> dict[str, int]:
    return {
        "after_sequence": context.integer("after_sequence"),
        "through_sequence": context.integer("through_sequence"),
        "limit": context.integer("limit"),
    }


# ---------------------------------------------------------------------------------------
# router-notifier
# ---------------------------------------------------------------------------------------


def _exercise_routed_after_global_sequence(context: ExerciseContext) -> ExerciseOutcome:
    from rquant.signal_route_spool import ReadonlySignalRouteSpool

    spec = SURFACE_SPECS[context.vector.surface_id]
    step = _notifier_step(context, spec)
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


def _exercise_notification_state_replicate(context: ExerciseContext) -> ExerciseOutcome:
    from rquant.notification_state import NotificationStateStore
    from rquant.signal_route_spool import ReadonlySignalRouteSpool

    spec = SURFACE_SPECS[context.vector.surface_id]
    step = _notifier_step(context, spec)
    source = _closure_value(step, "source", ReadonlySignalRouteSpool)
    store = _closure_value(step, "store", NotificationStateStore)
    bounds = _read_bounds(context)
    descriptor = source.source_descriptor()
    records = source.routed_after_global_sequence(observed_at=context.observed_at, **bounds)
    runtime_digest = tree_digest(context.workspace.runtime)
    surface = bound_surface(store, context.vector.surface_id)
    summary = surface(descriptor, records, observed_at=context.observed_at)
    return ExerciseOutcome(
        observation={
            "bounds": bounds,
            "replicated_input_count": len(records),
            "summary": summary.model_dump(mode="json"),
        },
        runtime_digest_before_call=runtime_digest,
    )


# ---------------------------------------------------------------------------------------
# notifier-serving
# ---------------------------------------------------------------------------------------


def _exercise_serving_authority_reader(context: ExerciseContext) -> ExerciseOutcome:
    step = _serving_publisher_step(context, SURFACE_SPECS[context.vector.surface_id])
    assembler = _closure_assembler(step)
    dataset_id = context.text("dataset_id")
    reader = getattr(assembler, f"{_READER_ATTRIBUTE_BY_DATASET[dataset_id]}", None)
    if reader is None:
        raise SurfaceExerciseError("vector call.dataset_id names no assembler reader")
    runtime_digest = tree_digest(context.workspace.runtime)
    surface = bound_surface(reader, context.vector.surface_id)
    read = surface(context.observed_at)
    return ExerciseOutcome(
        observation={
            "dataset_id": read.dataset_id,
            "expected_dataset_id": dataset_id,
            "generation_id": read.generation_id,
            "payload_sha256": canonical_sha256(read.payload.model_dump(mode="json")),
            "reason": read.reason,
            "sequence": read.sequence,
            "status": read.status.value,
        },
        runtime_digest_before_call=runtime_digest,
    )


def _exercise_serving_snapshot_assemble(context: ExerciseContext) -> ExerciseOutcome:
    step = _serving_publisher_step(context, SURFACE_SPECS[context.vector.surface_id])
    assembler = _closure_assembler(step)
    runtime_digest = tree_digest(context.workspace.runtime)
    surface = bound_surface(assembler, context.vector.surface_id)
    snapshot = surface(context.observed_at)
    return ExerciseOutcome(
        observation=_snapshot_projection(snapshot),
        runtime_digest_before_call=runtime_digest,
    )


def _exercise_build_serving_read_models(context: ExerciseContext) -> ExerciseOutcome:
    step = _serving_publisher_step(context, SURFACE_SPECS[context.vector.surface_id])
    assembler = _closure_assembler(step)
    snapshot = assembler.assemble(context.observed_at)
    runtime_digest = tree_digest(context.workspace.runtime)
    surface = resolve_surface(context.vector.surface_id)
    tables = surface(snapshot.read_model)
    # The read models are pandas frames whose cells are not all JSON scalars, so each one is
    # reduced to its column order, its row count, and a digest of its CSV rendering, which is
    # a deterministic function of the frame's content. Only that summary leaves the child.
    rendered = {
        name: {
            "columns": [str(column) for column in frame.columns],
            "content_sha256": sha256_hex(frame.to_csv(index=False).encode("utf-8")),
            "row_count": int(len(frame.index)),
        }
        for name, frame in tables.items()
    }
    return ExerciseOutcome(
        observation={
            "row_counts": {name: value["row_count"] for name, value in rendered.items()},
            "table_names": sorted(rendered),
            "tables_sha256": canonical_sha256(rendered),
        },
        runtime_digest_before_call=runtime_digest,
    )


# ---------------------------------------------------------------------------------------
# strategy-router
# ---------------------------------------------------------------------------------------


def _runner_batch_projection(batch: Any) -> dict[str, Any]:
    """A runner batch carries whole signal envelopes, so only its identity is returned."""

    descriptor = batch.snapshot.descriptor
    return {
        "count": len(batch.records),
        "descriptor": descriptor.model_dump(mode="json"),
        "records_sha256": canonical_sha256(
            [record.model_dump(mode="json") for record in batch.records]
        ),
        "sequences": [record.sequence for record in batch.records],
        "signal_ids": [record.signal.signal_id for record in batch.records],
    }


def _exercise_runner_source_read_batch(context: ExerciseContext) -> ExerciseOutcome:
    spec = SURFACE_SPECS[context.vector.surface_id]
    step = _signal_router_step(context, spec)
    source = _router_source(context, step)
    runtime_digest = tree_digest(context.workspace.runtime)
    after_sequence = context.integer("after_sequence")
    limit = context.integer("limit")
    surface = bound_surface(source, context.vector.surface_id)
    batch = surface(after_sequence=after_sequence, limit=limit)
    return ExerciseOutcome(
        observation={
            "after_sequence": after_sequence,
            "batch": _runner_batch_projection(batch),
            "limit": limit,
            "source_id": source.source_id,
        },
        runtime_digest_before_call=runtime_digest,
    )


def _exercise_route_runner_signals(context: ExerciseContext) -> ExerciseOutcome:
    from rquant.signal_bus import SignalBusStore
    from rquant.signal_router_runtime import SignalRouteCursorStore

    spec = SURFACE_SPECS[context.vector.surface_id]
    step = _signal_router_step(context, spec)
    source = _router_source(context, step)
    bus = _closure_value(step, "bus", SignalBusStore)
    cursors = _closure_value(step, "cursors", SignalRouteCursorStore)
    resolver = _closure_callable(step, "resolved_target_resolver")
    runtime_digest = tree_digest(context.workspace.runtime)
    limit = context.integer("limit")
    surface = resolve_surface(context.vector.surface_id)
    summary = surface(
        source_id=source.source_id,
        source=source,
        bus=bus,
        cursors=cursors,
        routed_at=context.observed_at,
        target_resolver=resolver,
        limit=limit,
    )
    return ExerciseOutcome(
        observation={
            "collaborators": [
                type(source).__name__,
                type(bus).__name__,
                type(cursors).__name__,
                type(resolver).__name__,
            ],
            "limit": limit,
            "summary": summary.model_dump(mode="json"),
        },
        runtime_digest_before_call=runtime_digest,
    )


# ---------------------------------------------------------------------------------------
# strategy-shadow
# ---------------------------------------------------------------------------------------


def _shadow_session_step(context: ExerciseContext, spec: SurfaceSpec) -> Any:
    """Build the Shadow session with nothing injected, so the loader is the real one."""

    from rquant.runtime_builder_shadow import shadow_session_builder

    manifest = _service_manifest(context, spec)
    try:
        return shadow_session_builder(clock=lambda: context.observed_at)(manifest)
    except (TypeError, ValueError) as exc:
        raise SurfaceExerciseError(
            "the shadow session production builder refused the vector"
        ) from exc


def _shadow_runner_source(context: ExerciseContext, step: Any) -> tuple[Any, Any]:
    """The `(binding, source)` pair the builder's own input loader constructed.

    `FilesystemShadowSessionInputLoader.load` is the only production path to
    `_FilesystemRunnerSource`, and it only returns one after every accepted legacy shadow
    export — monitor, surge, and both isolated runners — has passed
    `load_accepted_legacy_shadow_export` under the production filesystem policy.
    """

    from rquant.runtime_builder_shadow import (
        FilesystemShadowSessionInputLoader,
        _FilesystemRunnerSource,
    )

    loader = _closure_value(step, "resolved_input_loader", FilesystemShadowSessionInputLoader)
    settings = _closure_callable_free(step, "settings")
    manifest = _closure_callable_free(step, "manifest")
    strategy_id = context.text("strategy_id")
    try:
        inputs = loader.load(
            settings=settings,
            trade_date=_parse_trade_date(context.text("trade_date")),
            expected_export_commit=manifest.producer_commit,
        )
    except Exception as exc:  # noqa: BLE001 - every failure here is a refused vector
        raise SurfaceExerciseError(
            "the shadow input loader refused the in-generation export"
        ) from exc
    for binding, source in inputs.runner_sources:
        if binding.strategy_id == strategy_id:
            if type(source) is not _FilesystemRunnerSource:
                raise SurfaceExerciseError(
                    "the shadow input loader did not wire a _FilesystemRunnerSource"
                )
            return binding, source
    raise SurfaceExerciseError("vector call.strategy_id names no shadow runner source")


def _closure_callable_free(step: Any, name: str) -> Any:
    """One free variable of the builder's step, whatever its type."""

    code = getattr(step, "__code__", None)
    cells = getattr(step, "__closure__", None)
    if code is None or cells is None:  # pragma: no cover - builders return closures
        raise SurfaceExerciseError("the production builder did not return a closure step")
    names = tuple(code.co_freevars)
    if name not in names:
        raise SurfaceExerciseError(f"the production builder step does not capture {name}")
    return cells[names.index(name)].cell_contents


def _parse_trade_date(value: str) -> Any:
    from datetime import date as date_type

    try:
        return date_type.fromisoformat(value)
    except ValueError as exc:
        raise SurfaceExerciseError("vector call.trade_date is not an ISO date") from exc


def _runner_batch_identity(batch: Any) -> dict[str, Any]:
    return {
        "count": len(batch.records),
        "descriptor": batch.snapshot.descriptor.model_dump(mode="json"),
        "records_sha256": canonical_sha256(
            [record.model_dump(mode="json") for record in batch.records]
        ),
        "sequences": [record.sequence for record in batch.records],
    }


def _exercise_shadow_completed_batch(context: ExerciseContext) -> ExerciseOutcome:
    spec = SURFACE_SPECS[context.vector.surface_id]
    step = _shadow_session_step(context, spec)
    binding, source = _shadow_runner_source(context, step)
    runtime_digest = tree_digest(context.workspace.runtime)
    bounds = {
        "after_sequence": context.integer("after_sequence"),
        "limit": context.integer("limit"),
    }
    surface = bound_surface(source, context.vector.surface_id)
    batch = surface(trade_date=_parse_trade_date(context.text("trade_date")), **bounds)
    return ExerciseOutcome(
        observation={
            "batch": _runner_batch_identity(batch),
            "bounds": bounds,
            "source_id": source.source_id,
            "strategy_id": binding.strategy_id,
        },
        runtime_digest_before_call=runtime_digest,
    )


def _exercise_isolated_runner_shadow_snapshot(context: ExerciseContext) -> ExerciseOutcome:
    from rquant.runtime_shadow_validation import Ed25519CompletionAttestationKeyring

    spec = SURFACE_SPECS[context.vector.surface_id]
    step = _shadow_session_step(context, spec)
    binding, source = _shadow_runner_source(context, step)
    keyring = _closure_value(step, "completion_keyring", Ed25519CompletionAttestationKeyring)
    runtime_digest = tree_digest(context.workspace.runtime)
    surface = resolve_surface(context.vector.surface_id)
    snapshot = surface(
        source,
        trade_date=_parse_trade_date(context.text("trade_date")),
        observed_at=context.observed_at,
        binding=binding,
        expected_calendar_authority_id=context.text("calendar_authority_id"),
        attestation_verifier=keyring,
    )
    return ExerciseOutcome(
        observation={
            "observation_count": len(snapshot.observations),
            "observation_ids": [
                str(observation.observation_id) for observation in snapshot.observations
            ],
            "raw_input_id": snapshot.raw_input_id,
            "snapshot_sha256": canonical_sha256(snapshot.model_dump(mode="json")),
            "source_id": snapshot.source_id,
            "upstream_snapshot_id": snapshot.upstream_snapshot_id,
        },
        runtime_digest_before_call=runtime_digest,
    )


def _exercise_isolated_signal_observations(context: ExerciseContext) -> ExerciseOutcome:
    spec = SURFACE_SPECS[context.vector.surface_id]
    step = _shadow_session_step(context, spec)
    binding, source = _shadow_runner_source(context, step)
    trade_date = _parse_trade_date(context.text("trade_date"))
    batch = source.read_completed_batch(
        trade_date=trade_date,
        after_sequence=context.integer("after_sequence"),
        limit=context.integer("limit"),
    )
    runtime_digest = tree_digest(context.workspace.runtime)
    surface = resolve_surface(context.vector.surface_id)
    observations = surface(
        [record.signal for record in batch.records],
        trade_date=trade_date,
        binding=binding,
        current_producer_commit=context.text("producer_commit"),
    )
    return ExerciseOutcome(
        observation={
            "count": len(observations),
            "observation_ids": [
                str(observation.observation_id) for observation in observations
            ],
            "observations_sha256": canonical_sha256(
                [observation.model_dump(mode="json") for observation in observations]
            ),
            "strategy_id": binding.strategy_id,
        },
        runtime_digest_before_call=runtime_digest,
    )


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
    "rquant.signal_route_spool.ReadonlySignalRouteSpool.routed_after_global_sequence": SurfaceSpec(
        builder="rquant.runtime_builder_signal.notifier_builder",
        service_kind="notifier",
        writes=False,
        exercise=_exercise_routed_after_global_sequence,
    ),
    "rquant.notification_state.NotificationStateStore.replicate": SurfaceSpec(
        builder="rquant.runtime_builder_signal.notifier_builder",
        service_kind="notifier",
        writes=True,
        exercise=_exercise_notification_state_replicate,
    ),
    "rquant.runtime_serving_authority.ServingSourceAuthorityReader.__call__": SurfaceSpec(
        builder="rquant.runtime_builder_serving.serving_publisher_builder",
        service_kind="serving_publisher",
        writes=False,
        exercise=_exercise_serving_authority_reader,
    ),
    "rquant.runtime_serving_snapshot.ServingSnapshotAssembler.assemble": SurfaceSpec(
        builder="rquant.runtime_builder_serving.serving_publisher_builder",
        service_kind="serving_publisher",
        writes=False,
        exercise=_exercise_serving_snapshot_assemble,
    ),
    "rquant.serving_read_models.build_serving_read_models": SurfaceSpec(
        builder="rquant.runtime_builder_serving.serving_publisher_builder",
        service_kind="serving_publisher",
        writes=False,
        exercise=_exercise_build_serving_read_models,
    ),
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
    "rquant.signal_router_runtime.ReadonlyStrategyRunnerSignalSource.read_batch": SurfaceSpec(
        builder="rquant.runtime_builder_signal.signal_router_builder",
        service_kind="signal_router",
        writes=False,
        exercise=_exercise_runner_source_read_batch,
    ),
    "rquant.signal_router_runtime.route_runner_signals": SurfaceSpec(
        builder="rquant.runtime_builder_signal.signal_router_builder",
        service_kind="signal_router",
        writes=True,
        exercise=_exercise_route_runner_signals,
    ),
    "rquant.runtime_builder_shadow._FilesystemRunnerSource.read_completed_batch": SurfaceSpec(
        builder="rquant.runtime_builder_shadow.shadow_session_builder",
        service_kind="shadow_session",
        writes=False,
        exercise=_exercise_shadow_completed_batch,
    ),
    "rquant.runtime_shadow_sources.read_isolated_runner_shadow_snapshot": SurfaceSpec(
        builder="rquant.runtime_builder_shadow.shadow_session_builder",
        service_kind="shadow_session",
        writes=False,
        exercise=_exercise_isolated_runner_shadow_snapshot,
    ),
    "rquant.runtime_shadow_sources.isolated_signal_observations": SurfaceSpec(
        builder="rquant.runtime_builder_shadow.shadow_session_builder",
        service_kind="shadow_session",
        writes=False,
        exercise=_exercise_isolated_signal_observations,
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


_SERVING_PAYLOAD_ATTRIBUTE_BY_DATASET: Final[dict[str, str]] = {
    "lab_jobs": "LabJobsPayload",
    "paper_accounts": "PaperAccountsPayload",
    "promotions": "PromotionsPayload",
    "reference_slow_authority": "ReferenceSlowPayload",
    "runtime_health": "RuntimeHealthPayload",
    "signals": "SignalDeliveryPayload",
}


def _publish_declared_serving_authorities(
    workspace: VectorWorkspace,
    declared: Mapping[str, Any],
) -> None:
    """Materialize the six owner authorities through the pair's own producer surface.

    `ServingSourceAuthorityPublisher.publish` is producer-surface evidence for
    `notifier-serving` (`authority.md` L1218), so using it to lay down the vector's declared
    state keeps the reader side honest: the assembler reads exactly what a real publisher
    wrote, byte for byte, and not a hand-built directory.
    """

    from rquant import runtime_serving_snapshot as snapshot_models
    from rquant.runtime_contracts import canonical_sha256 as v2_canonical_sha256
    from rquant.runtime_serving_authority import ServingSourceAuthorityPublisher
    from rquant.runtime_serving_snapshot import SourceReadResult
    from rquant.serving_contracts import FreshnessStatus

    expected = ("datasets", "producer_commit", "published_at", "root")
    if type(declared) is not dict or tuple(sorted(declared)) != expected:
        raise SurfaceExerciseError(
            "vector serving_authorities must be exactly {datasets, producer_commit, "
            "published_at, root}"
        )
    if type(declared["datasets"]) is not list or not declared["datasets"]:
        raise SurfaceExerciseError("vector serving_authorities datasets must be a nonempty array")
    root = workspace.declared_path(declared["root"], live=workspace.state)
    published_at = _parse_observed_at(declared["published_at"])
    for entry in declared["datasets"]:
        if type(entry) is not dict or tuple(sorted(entry)) != ("dataset_id", "payload", "sequence"):
            raise SurfaceExerciseError(
                "each declared authority must be exactly {dataset_id, payload, sequence}"
            )
        dataset_id = entry["dataset_id"]
        attribute = _SERVING_PAYLOAD_ATTRIBUTE_BY_DATASET.get(dataset_id)
        if attribute is None:
            raise SurfaceExerciseError("vector names an authority dataset outside the six owners")
        try:
            payload = getattr(snapshot_models, attribute).model_validate(entry["payload"])
            values: dict[str, Any] = {
                "dataset_id": dataset_id,
                "sequence": entry["sequence"],
                "event_time": published_at,
                "published_at": published_at,
                "status": FreshnessStatus.FRESH,
                "reason": None,
                "payload": payload,
            }
            values["generation_id"] = v2_canonical_sha256(values)
            ServingSourceAuthorityPublisher(
                root=root / dataset_id,
                producer_commit=declared["producer_commit"],
                dataset_id=dataset_id,
                payload_kind=payload.payload_kind,
                clock=lambda: published_at,
            ).publish(SourceReadResult.model_validate(values))
        except (TypeError, ValueError) as exc:
            raise SurfaceExerciseError(
                f"vector serving authority {dataset_id} is not publishable"
            ) from exc


def _parse_observed_at(value: Any) -> datetime:
    from rquant.runtime_contracts import normalize_aware_utc

    if type(value) is not str:
        raise SurfaceExerciseError("vector observed_at must be an RFC 3339 string")
    try:
        return normalize_aware_utc(datetime.fromisoformat(value))
    except (TypeError, ValueError) as exc:
        raise SurfaceExerciseError("vector observed_at is not an aware UTC timestamp") from exc


def exercise_vector(
    vector: RequestVector,
    root: Path,
    *,
    generation_root: Path | None = None,
    authorized_fixtures: Mapping[str, AuthorizedGenerationFile] | None = None,
) -> dict[str, Any]:
    """Run one vector through its production builder and return its canonical result.

    `generation_root` and `authorized_fixtures` are the root-derived halves of the ruling
    E-1 fixture channel. They are keyword-only and default to nothing so that a vector which
    declares no fixture cannot be affected by them, and a vector which does declare one
    cannot be answered at all unless the root supplied the authorization first.
    """

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

    workspace = VectorWorkspace(
        root,
        vector.vector_id,
        generation_root=generation_root,
        authorized_fixtures=authorized_fixtures,
    )
    try:
        workspace.materialize(declared_state)
    except WorkspaceError as exc:
        raise SurfaceExerciseError(str(exc)) from exc
    spool = declared_state.get("spool")
    if spool is not None:
        _publish_declared_spool(workspace, spool)
    authorities = declared_state.get("serving_authorities")
    if authorities is not None:
        _publish_declared_serving_authorities(workspace, authorities)

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
    if not spec.writes and outcome.runtime_digest_before_call != runtime_after:
        raise SurfaceExerciseError("a read-only surface modified the builder's runtime tree")
    # The read-only verdict is enforced, never reported. Whether a *writer* left the runtime
    # tree byte-identical depends on when SQLite last checkpointed, which differs between the
    # policy author's process and the child's, so it can never be part of a frozen result.
    # 裁决 4 / ruling E-3: the frozen result shape carries no `state_unchanged`. The
    # child still fails fast above when a surface disturbs its own materialized
    # declaration, but the enforceable claim — that the in-generation producer state the
    # vector read is byte-identical before and after this run — is the root's, taken from
    # its own digest of the generation on both sides of the child. A constant `true` here
    # would have been the child vouching for itself.
    result: dict[str, Any] = {
        "schema_version": 1,
        "builder": spec.builder,
        "declared_writer": spec.writes,
        "observation": outcome.observation,
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
