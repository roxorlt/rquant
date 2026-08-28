"""The v1 Phase C vector set, shared by the harness unit suite and the offline world.

One vector per reader surface the harness actually exercises. Every vector is a pure value:
paths are declared as `@workspace/...` (the materialized declaration) or `@runtime/...`
(what the production builder owns), so the same bytes work inside any child cwd, and the
canonical result never carries a random temporary path.

The expected results are *not* produced here. The policy author derives them by running the
same exercise functions the child runs; `expected_results_for` does exactly that and is used
only on the policy side, never inside the child.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import shutil
import sys
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from rquant.delivery_contracts import DeliveryChannel, DeliveryTarget
from rquant.signal_bus import (
    RouteReceiptDisposition,
    SignalBusRoutedRecord,
    SignalBusSourceDescriptor,
    SignalRouteReceipt,
)
from rquant.signal_contracts import SignalAction, SignalEnvelope
from rquant.signal_family_successor_registry import ACCEPTED_FAMILY_IDS
from rquant.signal_family_verification import (
    SignalFamilyGenerationFileV1,
    SignalFamilyVectorV1,
    SurfaceId,
)
from rquant.strict_json import canonical_json_bytes
from tests.paper_cost_fixtures import paper_execution_cost_spec

FAMILY_ID = ACCEPTED_FAMILY_IDS[0]
OBSERVED_AT = datetime(2026, 8, 24, 7, 30, tzinfo=UTC)
PRODUCER_COMMIT = "f" * 40
SOURCE_GENERATION_ID = "3" * 64
ROUTE_SOURCE_ID = "n-shape-v1"

_SPOOL_ROOT = "@workspace/signal-spool"

# ---------------------------------------------------------------------------------------
# The ruling E-1 in-generation producer fixture set
# ---------------------------------------------------------------------------------------

#: Where the checked-in fixtures live in the repository, and where they live once the offline
#: world has copied them into the generation tree it assembles.
PRODUCER_FIXTURE_ROOT = (
    Path(__file__).resolve().parent.parent / "fixtures" / "signal_family_producer"
)
GENERATION_FIXTURE_PREFIX = "signal-family/fixtures"
_ROUTER_FIXTURE_DIRECTORY = "strategy-router"
_RUNNER_STATE_DUMP = "runner-state-v1.sql"
_ROUTING_POLICY = "routing-policy-v1.json"

#: The generation ships fixtures read-only, exactly like every other generation file, and the
#: child materializes its private copy at the same mode. `FIXTURE_MODIFIED_AT` is declared
#: rather than inherited because `runtime_routing_policy` refuses a frozen policy whose mtime
#: is newer than the observation instant, and a file the child just wrote always would be.
FIXTURE_MODE = 0o444
FIXTURE_MODIFIED_AT = "2026-08-24T07:00:00Z"

_ROUTER_STATE_PATH = "@workspace/runner-state.sqlite3"
_ROUTER_DUMP_PATH = "@workspace/runner-state-v1.sql"
_ROUTER_POLICY_PATH = "@workspace/routing-policy.json"


def producer_fixture_descriptor() -> dict[str, Any]:
    """The policy-side facts about the checked-in fixture set, as its build script wrote them."""

    payload = (PRODUCER_FIXTURE_ROOT / _ROUTER_FIXTURE_DIRECTORY / "descriptor.json").read_bytes()
    decoded = json.loads(payload)
    assert isinstance(decoded, dict)
    return decoded


def _fixture_relative_paths() -> tuple[str, ...]:
    return (
        f"{GENERATION_FIXTURE_PREFIX}/{_ROUTER_FIXTURE_DIRECTORY}/{_ROUTING_POLICY}",
        f"{GENERATION_FIXTURE_PREFIX}/{_ROUTER_FIXTURE_DIRECTORY}/{_RUNNER_STATE_DUMP}",
    )


def _fixture_source(relative: str) -> Path:
    return PRODUCER_FIXTURE_ROOT / relative[len(GENERATION_FIXTURE_PREFIX) + 1 :]


def generation_fixture_declarations() -> tuple[SignalFamilyGenerationFileV1, ...]:
    """The checked-in half of the `generation_files` tuple: the `strategy-router` fixture."""

    return tuple(
        SignalFamilyGenerationFileV1(
            relative_path=relative,
            sha256=hashlib.sha256(_fixture_source(relative).read_bytes()).hexdigest(),
            size=_fixture_source(relative).stat().st_size,
            mode=FIXTURE_MODE,
        )
        for relative in _fixture_relative_paths()
    )


def shadow_fixture_supported() -> bool:
    """Whether the `strategy-shadow` producer fixture can exist on this host.

    `shadow_session_builder` reaches its three readers only through
    `FilesystemShadowSessionInputLoader` under
    `LegacyShadowFilesystemPolicy(mode="linux-production")`, and
    `legacy_shadow_export._validate_mount_policy` refuses that mode unless
    `sys.platform == "linux"`. The fixture is published by the production export publishers,
    which run the same predicate, so both halves are Linux-only for the same reason.
    """

    return sys.platform == "linux"


@dataclass(frozen=True)
class InstalledGenerationFixtures:
    """One generation tree's fixture set, as the policy author sees it."""

    generation_root: Path
    declarations: tuple[SignalFamilyGenerationFileV1, ...]
    shadow_descriptor: dict[str, Any] | None

    def authorized(self) -> dict[str, Any]:
        from rquant.signal_family_verifier_harness import AuthorizedGenerationFile

        return {
            declared.relative_path: AuthorizedGenerationFile(
                relative_path=declared.relative_path,
                sha256=declared.sha256,
                size=declared.size,
                mode=declared.mode,
            )
            for declared in self.declarations
        }


def _shadow_builder() -> Any:
    script = (
        Path(__file__).resolve().parent.parent.parent
        / "scripts"
        / "build-signal-family-shadow-fixture.py"
    )
    spec = importlib.util.spec_from_file_location("_signal_family_shadow_builder", script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["_signal_family_shadow_builder"] = module
    spec.loader.exec_module(module)
    return module


def install_generation_fixtures(generation_path: Path) -> InstalledGenerationFixtures:
    """Put every producer fixture into a generation tree and declare what it now holds.

    The `strategy-router` fixture is checked in and copied. The `strategy-shadow` fixture is
    *published in place* by the production export publishers, because its Ed25519 recovery
    marker binds the session directory's inode and a copy would therefore be a different
    directory (see `scripts/build-signal-family-shadow-fixture.py`). On a host where the
    production reader path cannot be entered at all, the shadow half is simply absent and
    the vector set is the ten surfaces that remain reachable.
    """

    declarations = list(generation_fixture_declarations())
    for declared in declarations:
        target = generation_path / declared.relative_path
        target.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
        shutil.copyfile(_fixture_source(declared.relative_path), target)
        target.chmod(declared.mode)
    shadow_descriptor: dict[str, Any] | None = None
    if shadow_fixture_supported():
        builder = _shadow_builder()
        scratch = Path(tempfile.mkdtemp(prefix="rquant-shadow-build-", dir=Path.home()))
        scratch.chmod(0o700)
        try:
            result = builder.build_shadow_fixture(
                generation_path.resolve(),
                relative_prefix=GENERATION_FIXTURE_PREFIX,
                scratch_root=scratch,
            )
        finally:
            shutil.rmtree(scratch, ignore_errors=True)
        shadow_descriptor = result.descriptor
        for relative in result.relative_paths:
            published = generation_path / relative
            declarations.append(
                SignalFamilyGenerationFileV1(
                    relative_path=relative,
                    sha256=hashlib.sha256(published.read_bytes()).hexdigest(),
                    size=published.stat().st_size,
                    mode=FIXTURE_MODE,
                )
            )
    return InstalledGenerationFixtures(
        generation_root=generation_path.resolve(),
        declarations=tuple(
            sorted(declarations, key=lambda declared: declared.relative_path)
        ),
        shadow_descriptor=shadow_descriptor,
    )


def _generation_file_state(relative: str, workspace_path: str) -> dict[str, Any]:
    return {
        "mode": FIXTURE_MODE,
        "modified_at": FIXTURE_MODIFIED_AT,
        "path": workspace_path,
        "sha256": hashlib.sha256(_fixture_source(relative).read_bytes()).hexdigest(),
        "source_relative_path": relative,
    }


def _router_state() -> dict[str, Any]:
    policy_relative, dump_relative = _fixture_relative_paths()
    return {
        "generation_files": [
            _generation_file_state(policy_relative, _ROUTER_POLICY_PATH),
            _generation_file_state(dump_relative, _ROUTER_DUMP_PATH),
        ],
        "sqlite_sources": [
            {
                "mode": FIXTURE_MODE,
                "path": _ROUTER_STATE_PATH,
                "script_path": _ROUTER_DUMP_PATH,
            }
        ],
    }


def _signal_router_service() -> dict[str, Any]:
    descriptor = producer_fixture_descriptor()
    return {
        "interval_seconds": 1.0,
        "plane": "live",
        "producer_commit": PRODUCER_COMMIT,
        "service_id": "signal-router",
        "service_kind": "signal_router",
        "settings": {
            "batch_limit": 10,
            "routing_policy_fingerprint": descriptor["routing_policy_fingerprint"],
            "routing_policy_path": _ROUTER_POLICY_PATH,
            "signal_bus_path": "@runtime/signal-bus.sqlite3",
            "signal_spool_root": "@runtime/router-spool",
            "sources": [
                {
                    "expected_evaluator_contract_fingerprint": descriptor[
                        "evaluator_contract_fingerprint"
                    ],
                    "expected_strategy_registration_fingerprint": descriptor[
                        "strategy_registration_fingerprint"
                    ],
                    "expected_strategy_spec_fingerprint": descriptor[
                        "strategy_spec_fingerprint"
                    ],
                    "runner_state_path": _ROUTER_STATE_PATH,
                    "source_id": descriptor["source_id"],
                }
            ],
        },
        "stale_after_seconds": 60.0,
    }


def _observed_at_text() -> str:
    return OBSERVED_AT.isoformat().replace("+00:00", "Z")


def _signal(seed: str, action: SignalAction) -> SignalEnvelope:
    return SignalEnvelope(
        schema_version=1,
        strategy_id="n-shape",
        strategy_version="1",
        parameter_fingerprint=seed * 64,
        dataset_snapshot_id="d" * 64,
        feature_snapshot_id="e" * 64,
        event_time=OBSERVED_AT - timedelta(seconds=120),
        available_at=OBSERVED_AT - timedelta(seconds=60),
        candidate_id="600000.SH",
        action=action,
        reason_codes=("wp4c-vector",),
        evidence={},
        expires_at=OBSERVED_AT + timedelta(minutes=30),
        producer_commit=PRODUCER_COMMIT,
    )


def _routed_record(sequence: int, signal: SignalEnvelope) -> SignalBusRoutedRecord:
    payload_json = signal.model_dump_json()
    receipt = SignalRouteReceipt(
        source_id=ROUTE_SOURCE_ID,
        source_sequence=sequence,
        signal_id=signal.signal_id,
        decision_fingerprint="1" * 64,
        disposition=RouteReceiptDisposition.ROUTED,
        target_manifest_hash="2" * 64,
        targets=(DeliveryTarget(recipient_id="admin", channel=DeliveryChannel.PUSHDEER),),
        target_count=1,
        routed_at=OBSERVED_AT - timedelta(seconds=30),
    )
    return SignalBusRoutedRecord(
        global_sequence=sequence,
        signal_id=signal.signal_id,
        payload_hash=hashlib.sha256(payload_json.encode("utf-8")).hexdigest(),
        payload_json=payload_json,
        signal=signal,
        received_at=OBSERVED_AT - timedelta(seconds=45),
        receipt=receipt,
    )


def spool_state(*, actions: tuple[SignalAction, ...]) -> dict[str, Any]:
    """A routed-signal prefix declared as data; the harness publishes it with the spool."""

    records = [
        _routed_record(index, _signal(chr(ord("a") + index - 1), action))
        for index, action in enumerate(actions, start=1)
    ]
    descriptor = SignalBusSourceDescriptor(
        generation_id=SOURCE_GENERATION_ID,
        high_watermark=len(records),
    )
    return {
        "records": [record.model_dump(mode="json") for record in records],
        "root": _SPOOL_ROOT,
        "source": descriptor.model_dump(mode="json"),
    }


_CALENDAR_CONTENT = canonical_json_bytes(
    [{"cal_date": "2026-08-24", "exchange": "SSE", "is_open": True}]
).decode("utf-8")
_CALENDAR_SHA256 = hashlib.sha256(_CALENDAR_CONTENT.encode("utf-8")).hexdigest()


def _notifier_service() -> dict[str, Any]:
    return {
        "interval_seconds": 1.0,
        "plane": "live",
        "producer_commit": PRODUCER_COMMIT,
        "service_id": "notifier",
        "service_kind": "notifier",
        "settings": {
            "batch_limit": 10,
            "lease_seconds": 60,
            "notification_state_path": "@runtime/notification-state.sqlite3",
            "signal_spool_root": _SPOOL_ROOT,
            "worker_id": "wp4c-notifier",
        },
        "stale_after_seconds": 75.0,
    }


_AUTHORITY_ROOT = "@workspace/serving-authorities"
_AUTHORITY_PUBLISHED_AT = "2026-08-24T07:29:00Z"
_OWNER_DATASETS: tuple[str, ...] = (
    "lab_jobs",
    "paper_accounts",
    "promotions",
    "reference_slow_authority",
    "runtime_health",
    "signals",
)


def _authority_payload(dataset_id: str) -> dict[str, Any]:
    if dataset_id != "reference_slow_authority":
        return {}
    return {
        "reference_generation_id": "9" * 64,
        "revision": 1,
        "price_basis": "raw_session",
        "adjustment_basis": "tushare_adj_factor",
        "available_at": _AUTHORITY_PUBLISHED_AT,
    }


def _serving_state() -> dict[str, Any]:
    return {
        "directories": [_AUTHORITY_ROOT],
        "files": [],
        "serving_authorities": {
            "datasets": [
                {
                    "dataset_id": dataset_id,
                    "payload": _authority_payload(dataset_id),
                    "sequence": 1,
                }
                for dataset_id in _OWNER_DATASETS
            ],
            "producer_commit": PRODUCER_COMMIT,
            "published_at": _AUTHORITY_PUBLISHED_AT,
            "root": _AUTHORITY_ROOT,
        },
    }


def _serving_publisher_service() -> dict[str, Any]:
    return {
        "interval_seconds": 1.0,
        "plane": "serving",
        "producer_commit": PRODUCER_COMMIT,
        "service_id": "serving-publisher",
        "service_kind": "serving_publisher",
        "settings": {
            "schema_version": 3,
            "serving_root": "@runtime/serving",
            "source_authorities": [
                {"dataset_id": dataset_id, "root": f"{_AUTHORITY_ROOT}/{dataset_id}"}
                for dataset_id in _OWNER_DATASETS
            ],
        },
        "stale_after_seconds": 50.0,
    }


def _paper_broker_service() -> dict[str, Any]:
    return {
        "interval_seconds": 1.0,
        "plane": "live",
        "producer_commit": PRODUCER_COMMIT,
        "service_id": "paper-broker",
        "service_kind": "paper_broker",
        "settings": {
            "account_id": "paper-main",
            "broker_path": "@runtime/broker.sqlite3",
            "buy_quantity": 1_000,
            "consumer_state_path": "@runtime/paper-consumer.sqlite3",
            "execution_constraint_root": "@workspace/execution-constraints",
            "execution_cost_spec": paper_execution_cost_spec().model_dump(mode="json"),
            "execution_lag_seconds": 60,
            "initial_cash": str(Decimal("100000")),
            "limit": 10,
            "queue_path": "@runtime/paper-queue.sqlite3",
            "raw_spool_root": "@workspace/raw-spool",
            "reduce_quantity": 500,
            "sell_quantity": 1_000,
            "signal_spool_root": _SPOOL_ROOT,
            "trade_calendar_path": "@workspace/trade-calendar.json",
            "trade_calendar_sha256": _CALENDAR_SHA256,
        },
        "stale_after_seconds": 180.0,
    }


def _paper_state(*, actions: tuple[SignalAction, ...]) -> dict[str, Any]:
    return {
        "directories": ["@workspace/raw-spool", "@workspace/execution-constraints"],
        "files": [{"content": _CALENDAR_CONTENT, "path": "@workspace/trade-calendar.json"}],
        "spool": spool_state(actions=actions),
    }


_SHADOW_SIGNER_COMMAND = (
    "/usr/bin/sudo",
    "-n",
    "/usr/local/libexec/rquant-shadow-report-signer",
)


def _shadow_service(descriptor: dict[str, Any]) -> dict[str, Any]:
    """The Shadow session manifest, pointed at the export published in the generation."""

    prefix = "@generation/"
    return {
        "interval_seconds": 300.0,
        "plane": "research",
        "producer_commit": descriptor["producer_commit"],
        "service_id": "shadow-session",
        "service_kind": "shadow_session",
        "settings": {
            "calendar_content_sha256": descriptor["calendar_content_sha256"],
            "calendar_expected_commit": descriptor["calendar_producer_commit"],
            "calendar_path": prefix + descriptor["calendar_relative_path"],
            "completion_active_key_id": descriptor["completion_active_key_id"],
            "completion_active_public_key_pem": descriptor[
                "completion_active_public_key_pem"
            ],
            "isolated_runner_root": prefix + descriptor["isolated_runner_relative_root"],
            "legacy_monitor_root": prefix + descriptor["monitor_relative_root"],
            "legacy_surge_root": prefix + descriptor["surge_relative_root"],
            "match_tolerance_microseconds": 60_000_000,
            "mode": "shadow",
            "producer_version": descriptor["producer_version"],
            "report_active_key_id": descriptor["report_active_key_id"],
            "report_active_public_key_pem": descriptor["report_active_public_key_pem"],
            "report_producer_instance_id": "shadow-primary",
            "report_producer_service_id": "shadow-session",
            "report_root": "@runtime/shadow-reports",
            "runner_manifest_bindings": descriptor["runner_manifest_bindings"],
            "signer_command": list(_SHADOW_SIGNER_COMMAND),
            "signer_timeout_seconds": 1.0,
            "strategy_bindings": descriptor["strategy_bindings"],
        },
        "stale_after_seconds": 172_800.0,
    }


def _shadow_state() -> dict[str, Any]:
    """The Shadow readers materialize nothing: the export is read where it was published."""

    return {"directories": [], "files": []}


def _shadow_vector_specifications(
    descriptor: dict[str, Any],
) -> tuple[tuple[str, SurfaceId, str], ...]:
    service = _shadow_service(descriptor)
    state = _shadow_state()
    trade_date = descriptor["trade_date"]
    return (
        (
            "strategy-shadow",
            SurfaceId.FILESYSTEM_RUNNER_SOURCE_READ_COMPLETED_BATCH,
            _envelope(
                call={
                    "after_sequence": 0,
                    "limit": 10,
                    "strategy_id": "n_shape",
                    "trade_date": trade_date,
                },
                service=service,
                state=state,
            ),
        ),
        (
            "strategy-shadow",
            SurfaceId.READ_ISOLATED_RUNNER_SHADOW_SNAPSHOT,
            _envelope(
                call={
                    "calendar_authority_id": descriptor["calendar_content_sha256"],
                    "strategy_id": "n_shape",
                    "trade_date": trade_date,
                },
                service=service,
                state=state,
            ),
        ),
        (
            "strategy-shadow",
            SurfaceId.ISOLATED_SIGNAL_OBSERVATIONS,
            _envelope(
                call={
                    "after_sequence": 0,
                    "limit": 10,
                    "producer_commit": descriptor["producer_commit"],
                    "strategy_id": "growth_board_surge",
                    "trade_date": trade_date,
                },
                service=service,
                state=state,
            ),
        ),
    )


def _envelope(
    *,
    call: dict[str, Any],
    service: dict[str, Any],
    state: dict[str, Any],
) -> str:
    return canonical_json_bytes(
        {
            "call": call,
            "observed_at": _observed_at_text(),
            "schema_version": 1,
            "service": service,
            "state": state,
        }
    ).decode("utf-8")


_READ_BOUNDS: dict[str, Any] = {"after_sequence": 0, "limit": 10, "through_sequence": 1}


def vector_specifications(
    shadow_descriptor: dict[str, Any] | None = None,
) -> tuple[tuple[str, SurfaceId, str], ...]:
    """`(pair_id, surface_id, input_json)` for every surface reachable on this host."""

    paper_state = _paper_state(actions=(SignalAction.B_INTENT,))
    notifier_state = {
        "directories": [],
        "files": [],
        "spool": spool_state(actions=(SignalAction.WATCH,)),
    }
    serving_state = _serving_state()
    router_state = _router_state()
    router_service = _signal_router_service()
    source_id = producer_fixture_descriptor()["source_id"]
    return (
        (
            "strategy-router",
            SurfaceId.READONLY_STRATEGY_RUNNER_SIGNAL_SOURCE_READ_BATCH,
            _envelope(
                call={"after_sequence": 0, "limit": 10, "source_id": source_id},
                service=router_service,
                state=router_state,
            ),
        ),
        (
            "strategy-router",
            SurfaceId.ROUTE_RUNNER_SIGNALS,
            _envelope(
                call={"limit": 10, "source_id": source_id},
                service=router_service,
                state=router_state,
            ),
        ),
        (
            "router-notifier",
            SurfaceId.READONLY_SIGNAL_ROUTE_SPOOL_ROUTED_AFTER_GLOBAL_SEQUENCE,
            _envelope(
                call=dict(_READ_BOUNDS),
                service=_notifier_service(),
                state=notifier_state,
            ),
        ),
        (
            "router-notifier",
            SurfaceId.NOTIFICATION_STATE_STORE_REPLICATE,
            _envelope(
                call=dict(_READ_BOUNDS),
                service=_notifier_service(),
                state=notifier_state,
            ),
        ),
        (
            "notifier-serving",
            SurfaceId.SERVING_SOURCE_AUTHORITY_READER_CALL,
            _envelope(
                call={"dataset_id": "signals"},
                service=_serving_publisher_service(),
                state=serving_state,
            ),
        ),
        (
            "notifier-serving",
            SurfaceId.SERVING_SNAPSHOT_ASSEMBLER_ASSEMBLE,
            _envelope(
                call={},
                service=_serving_publisher_service(),
                state=serving_state,
            ),
        ),
        (
            "notifier-serving",
            SurfaceId.BUILD_SERVING_READ_MODELS,
            _envelope(
                call={},
                service=_serving_publisher_service(),
                state=serving_state,
            ),
        ),
        (
            "router-paper",
            SurfaceId.READONLY_SIGNAL_ROUTE_SPOOL_SIGNALS_AFTER_GLOBAL_SEQUENCE,
            _envelope(
                call=dict(_READ_BOUNDS),
                service=_paper_broker_service(),
                state=paper_state,
            ),
        ),
        (
            "router-paper",
            SurfaceId.CONSUME_SIGNAL_BUS_TO_PAPER,
            _envelope(
                call={"limit": 10},
                service=_paper_broker_service(),
                state=paper_state,
            ),
        ),
        (
            "router-paper",
            SurfaceId.PAPER_SIGNAL_QUEUE_STORE_INGEST,
            _envelope(
                call={**_READ_BOUNDS, "global_sequence": 1},
                service=_paper_broker_service(),
                state=paper_state,
            ),
        ),
        *(
            ()
            if shadow_descriptor is None
            else _shadow_vector_specifications(shadow_descriptor)
        ),
    )


def blocked_surface_vector(surface_id: SurfaceId) -> tuple[SignalFamilyVectorV1, str]:
    """A vector for a surface the harness refuses, plus a placeholder expected result.

    The placeholder exists only so a policy can be minted around this vector. The child
    never produces it: it rejects the whole run rather than inventing an observation for a
    surface it cannot reach, which is exactly what this vector is here to prove.
    """

    from rquant.signal_family_verification import READER_SURFACES

    pair_id = next(
        pair for pair, surfaces in READER_SURFACES.items() if surface_id in surfaces
    )
    vector = SignalFamilyVectorV1.create(
        pair_id=pair_id,
        family_id=FAMILY_ID,
        surface_id=surface_id,
        input_json=canonical_json_bytes({"unreachable": True}).decode("utf-8"),
    )
    return vector, canonical_json_bytes({"unreachable": True}).decode("utf-8")


def harness_vectors(
    shadow_descriptor: dict[str, Any] | None = None,
) -> tuple[SignalFamilyVectorV1, ...]:
    """The sorted duplicate-free vector tuple the policy hashes."""

    built = [
        SignalFamilyVectorV1.create(
            pair_id=pair_id,
            family_id=FAMILY_ID,
            surface_id=surface_id,
            input_json=input_json,
        )
        for pair_id, surface_id, input_json in vector_specifications(shadow_descriptor)
    ]
    return tuple(sorted(built, key=lambda vector: vector.vector_id))


def authorized_fixtures() -> dict[str, Any]:
    """The root-authorized map for the checked-in `strategy-router` fixture alone."""

    from rquant.signal_family_verifier_harness import AuthorizedGenerationFile

    return {
        declared.relative_path: AuthorizedGenerationFile(
            relative_path=declared.relative_path,
            sha256=declared.sha256,
            size=declared.size,
            mode=declared.mode,
        )
        for declared in generation_fixture_declarations()
    }


def expected_results_for(
    vectors: tuple[SignalFamilyVectorV1, ...],
    root: Path,
    *,
    generation_root: Path | None = None,
    installed: InstalledGenerationFixtures | None = None,
) -> dict[str, str]:
    """Policy-side derivation: run the same exercise the child runs and keep the bytes.

    This never runs inside the child. It exists because whoever signs the external policy
    has to know the expected result before the child is ever launched. When no installed
    fixture set is supplied the checked-in `strategy-router` half is staged into a scratch
    generation, which is the same bytes the world installs; the results carry no absolute
    path either way.
    """

    from rquant.signal_family_verifier_harness import RequestVector, exercise_vector

    fixture_root = (
        installed.generation_root
        if installed is not None
        else _fixture_generation_root(generation_root, root)
    )
    fixtures = installed.authorized() if installed is not None else authorized_fixtures()
    results: dict[str, str] = {}
    for index, vector in enumerate(vectors):
        request_vector = RequestVector(
            vector_id=vector.vector_id,
            pair_id=vector.pair_id,
            family_id=vector.family_id,
            surface_id=vector.surface_id.value,
            input_json=vector.input_json,
        )
        workspace_root = root / f"expected-{index:02d}"
        workspace_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        results[vector.vector_id] = canonical_json_bytes(
            exercise_vector(
                request_vector,
                workspace_root,
                generation_root=fixture_root,
                authorized_fixtures=fixtures,
            )
        ).decode("utf-8")
    return results


def _fixture_generation_root(generation_root: Path | None, scratch: Path) -> Path:
    """A generation-shaped tree holding the fixture set, for the policy-side derivation."""

    if generation_root is not None:
        return generation_root
    staged = scratch / "generation"
    if not staged.exists():
        staged.mkdir(mode=0o700, parents=True)
        install_generation_fixtures(staged)
    return staged


__all__ = [
    "FAMILY_ID",
    "InstalledGenerationFixtures",
    "shadow_fixture_supported",
    "FIXTURE_MODE",
    "FIXTURE_MODIFIED_AT",
    "GENERATION_FIXTURE_PREFIX",
    "PRODUCER_FIXTURE_ROOT",
    "blocked_surface_vector",
    "OBSERVED_AT",
    "PRODUCER_COMMIT",
    "authorized_fixtures",
    "expected_results_for",
    "generation_fixture_declarations",
    "harness_vectors",
    "install_generation_fixtures",
    "producer_fixture_descriptor",
    "spool_state",
    "vector_specifications",
]
