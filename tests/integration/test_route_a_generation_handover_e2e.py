"""#248 acceptance: the four durable artifacts a release used to leave behind.

The 2026-09-09 Route A window installed bundle generation `20d948d1…` over `3cf6160c…`
and four things refused their own previous generation's state:

* three `rquant-runtime-strategy@` units exited after ~2 minutes with `strategy spec does
  not match persisted runner identity`, looped on `Restart=`, and pushed three real alerts;
* `signal_router` went DEGRADED every iteration with `SignalRouteConflictError: source
  '…' generation changed`;
* both candidate publishers went DEGRADED with `strategy candidate authority is bound to
  a different identity`;
* one stopped heartbeat from the previous generation made the *whole* runtime health
  payload fail with `RuntimeHealthAuthorityIntegrityError`, and serving degraded behind it.

The window fixed all four by moving state aside by hand. This file is that window without
the hands: package L's two-generation install world, the four artifacts written by the
**first** generation's own writers driven by the **first** generation's installed
manifests, and then the second generation's roles started through the wrapper's own argv
inside each unit's `ReadWritePaths`. All four must enter their main loop.

One seam, stated plainly: the first generation's artifacts are written by calling the same
writer classes its roles call, with the fingerprints read out of
`generations/<first>/manifests/…`, rather than by launching the first generation's roles
through a second published authority chain. What is on disk is byte-for-byte what those
roles write — the identity in each artifact comes from the first generation's manifest, not
from a constant — and what this file is about is the *second* generation reading it.

The negative half is the point of the package and gets the same weight: a foreign identity
in each of the four shapes is still refused.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from rquant.runtime_generation_lineage import load_runtime_generation_tree
from rquant.runtime_health_authority import RuntimeHealthControlSource, RuntimeHealthSourceReader
from rquant.runtime_service_control import (
    RuntimeServiceControl,
    RuntimeServiceStatus,
    RuntimeStepResult,
)
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.signal_bus import RouteSourceDescriptor, SignalBusStore, SignalRouteConflictError
from rquant.strategy_candidate_snapshot import (
    StrategyCandidateSnapshotIntegrityError,
    StrategyCandidateSnapshotSpool,
)
from rquant.strategy_runner import StrategyRunnerStore
from tests.integration.test_route_a_all_roles_sandbox_e2e import (
    cold_chain,  # noqa: F401 -- the two-generation fixture, reused verbatim
    instance_of,
    run_role,
)
from tests.integration.test_route_a_legacy_binding_e2e import RouteAWorld
from tests.integration.test_route_a_live_chain_idle_e2e import FROZEN_NOW, _instance_name

pytestmark = pytest.mark.integration

STRATEGY_ROLE = "strategy_live"
ROUTER_ROLE = "signal_router"
CANDIDATE_ROLE = "candidate_publisher"
TRADE_DATE = date(2026, 8, 3)
FOREIGN = "e" * 64


# ---------------------------------------------------------------------------------------
# The first generation, read off disk rather than assumed
# ---------------------------------------------------------------------------------------


def first_generation_id(route: RouteAWorld) -> str:
    previous = route.receipt.previous_generation_hash
    assert previous is not None, "the fixture installs the second generation over the first"
    tree = load_runtime_generation_tree(route.runtime_root)
    assert tree.current_generation_id != previous
    return str(previous)


def first_generation_manifest(route: RouteAWorld, service_id: str) -> RuntimeServiceManifest:
    path = (
        route.runtime_root
        / "generations"
        / first_generation_id(route)
        / "manifests"
        / f"{_instance_name(service_id)}.json"
    )
    return RuntimeServiceManifest.model_validate_json(path.read_bytes())


def manifests_of(
    route: RouteAWorld,
    kind: RuntimeServiceKind,
) -> tuple[RuntimeServiceManifest, ...]:
    return tuple(item for item in route.profile.manifests if item.service_kind is kind)


def _strategy_spec(manifest: RuntimeServiceManifest) -> Any:
    """The spec that generation published, loaded out of that generation's registry."""

    from rquant.definition_registry import ImmutableDefinitionRegistry
    from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry

    registry = ImmutableDefinitionRegistry(
        Path(str(manifest.settings["definition_registry_root"])),
        execution_registry=BuiltinStrategyEvaluatorRegistry(
            producer_commit=manifest.producer_commit
        ).trusted_executable_registry(),
    )
    registration = registry.read_strategy_spec(
        str(manifest.settings["strategy_registration_fingerprint"]),
        as_of=FROZEN_NOW,
    )
    assert registration is not None
    return registration.spec


def write_first_generation_state(route: RouteAWorld) -> dict[str, Any]:
    """Everything the previous generation leaves on disk, in its real v0.33.4 shape."""

    written: dict[str, Any] = {"runners": {}, "sources": {}, "candidates": {}}

    #: (1) three `live/strategies/<svc>/runner.sqlite3`, each carrying the first
    #: generation's own strategy spec and evaluator contract fingerprints
    for current in manifests_of(route, RuntimeServiceKind.STRATEGY_LIVE):
        previous = first_generation_manifest(route, current.service_id)
        assert previous.settings["strategy_spec_fingerprint"] != (
            current.settings["strategy_spec_fingerprint"]
        )
        runner = StrategyRunnerStore(
            Path(str(current.settings["runner_state_path"])),
            spec=_strategy_spec(previous),
            evaluator_contract_fingerprint=str(
                previous.settings["evaluator_contract_fingerprint"]
            ),
        )
        written["runners"][current.service_id] = {
            "path": runner.path,
            "spec_fingerprint": str(previous.settings["strategy_spec_fingerprint"]),
            "source_generation_id": runner.source_generation_id,
        }

    #: (2) `live/signal-bus/signal_bus.sqlite3`, holding one route source row per strategy
    #: bound to the runner generation the first generation's runner database minted
    router = manifests_of(route, RuntimeServiceKind.SIGNAL_ROUTER)[0]
    previous_router = first_generation_manifest(route, router.service_id)
    bus = SignalBusStore(Path(str(router.settings["signal_bus_path"])))
    for source in previous_router.settings["sources"]:
        service_id = _service_id_of_runner(route, Path(str(source["runner_state_path"])))
        runner = written["runners"][service_id]
        bus.bind_route_source(
            RouteSourceDescriptor(
                source_id=str(source["source_id"]),
                generation_id=runner["source_generation_id"],
                strategy_spec_fingerprint=str(source["expected_strategy_spec_fingerprint"]),
                first_sequence=1,
                high_watermark=0,
            ),
            routing_policy_fingerprint=str(previous_router.settings["routing_policy_fingerprint"]),
            observed_at=FROZEN_NOW,
        )
        written["sources"][str(source["source_id"])] = runner["source_generation_id"]

    #: (3) `live/candidates/<svc>/authority.json`, created once, bound to the first
    #: generation's definition and executable fingerprints
    for current in manifests_of(route, RuntimeServiceKind.CANDIDATE_PUBLISHER):
        previous = first_generation_manifest(route, current.service_id)
        root = Path(str(current.settings["snapshot_root"]))
        StrategyCandidateSnapshotSpool(root).publish_strategy_records(
            strategy_id=str(previous.settings["strategy_id"]),
            strategy_version="1",
            definition_fingerprint=str(previous.settings["definition_fingerprint"]),
            executable_fingerprint=str(previous.settings["executable_fingerprint"]),
            candidate_schema_fingerprint=str(previous.settings["candidate_schema_fingerprint"]),
            static_feature_schema=dict(previous.settings["static_feature_schema"]),
            source_snapshot_ids={"candidate_input": "1" * 64},
            trade_date=TRADE_DATE,
            captured_at=FROZEN_NOW,
            producer_commit=previous.producer_commit,
            rows=(),
        )
        written["candidates"][current.service_id] = {
            "root": root,
            "definition_fingerprint": str(previous.settings["definition_fingerprint"]),
            "executable_fingerprint": str(previous.settings["executable_fingerprint"]),
        }

    #: (4) one stopped heartbeat from the first generation, which is what every research
    #: role leaves behind now that #217 makes it exit immediately
    catalog = manifests_of(route, RuntimeServiceKind.LAB_ARTIFACT_CATALOG)[0]
    previous_catalog = first_generation_manifest(route, catalog.service_id)
    assert previous_catalog.service_spec.identity != catalog.service_spec.identity
    control_root = _control_root(route, catalog)
    control = RuntimeServiceControl(
        control_root,
        spec=previous_catalog.service_spec,
        clock=lambda: FROZEN_NOW,
    )
    control.start()
    control.record_success(RuntimeStepResult(input_sequence=1, output_sequence=1))
    control.stop(reason="previous generation exited")
    written["heartbeat"] = {
        "control_root": control_root,
        "service_id": catalog.service_id,
        "previous_identity": previous_catalog.service_spec.identity,
        "current_spec": catalog.service_spec,
    }
    return written


def _service_id_of_runner(route: RouteAWorld, runner_state_path: Path) -> str:
    service_id = load_runtime_generation_tree(route.runtime_root).service_id_for_instance(
        runner_state_path.parent.name
    )
    assert service_id is not None
    return service_id


def _control_root(route: RouteAWorld, manifest: RuntimeServiceManifest) -> Path:
    bucket = {
        RuntimeServiceKind.LAB_ARTIFACT_CATALOG: "artifact-catalogs",
    }[manifest.service_kind]
    return route.runtime_root / "control" / bucket / _instance_name(manifest.service_id)


@pytest.fixture
def released_over(cold_chain: RouteAWorld) -> tuple[RouteAWorld, dict[str, Any]]:  # noqa: F811
    return cold_chain, write_first_generation_state(cold_chain)


# ---------------------------------------------------------------------------------------
# The release: every shape hands over, nothing fails closed
# ---------------------------------------------------------------------------------------


def test_the_first_generation_really_wrote_all_four_shapes(
    released_over: tuple[RouteAWorld, dict[str, Any]],
) -> None:
    """The premise, asserted rather than assumed."""

    route, state = released_over

    assert len(state["runners"]) == 3
    for entry in state["runners"].values():
        assert entry["path"].is_file()
    assert len(state["sources"]) == 3
    assert len(state["candidates"]) >= 1
    for entry in state["candidates"].values():
        assert (entry["root"] / "authority.json").is_file()
    heartbeat_path = RuntimeServiceControl._path_for(
        state["heartbeat"]["control_root"],
        state["heartbeat"]["current_spec"],
    )
    stored = json.loads(heartbeat_path.read_text(encoding="utf-8"))
    assert stored["spec_fingerprint"] == state["heartbeat"]["previous_identity"]
    assert stored["status"] == "stopped"
    assert stored["stopped_at"] is not None
    #: and every one of them is bound to the generation before the current one
    tree = load_runtime_generation_tree(route.runtime_root)
    assert first_generation_id(route) in tree.generation_ids


def test_every_strategy_archives_and_recreates_its_runner_database(
    released_over: tuple[RouteAWorld, dict[str, Any]],
) -> None:
    route, state = released_over
    previous = first_generation_id(route)

    for instance in instance_of(route, STRATEGY_ROLE):
        run = run_role(route, STRATEGY_ROLE, instance=instance)
        assert run.entered, f"{instance}: {run.refusal!r}\n{run.traceback}"
        assert run.violations == []

    for service_id, entry in state["runners"].items():
        path = entry["path"]
        archived = path.with_name(f"{path.name}.{previous}.archived")
        assert archived.is_file(), service_id
        assert _persisted_spec_fingerprint(archived) == entry["spec_fingerprint"]
        current = next(
            item
            for item in manifests_of(route, RuntimeServiceKind.STRATEGY_LIVE)
            if item.service_id == service_id
        )
        assert _persisted_spec_fingerprint(path) == str(
            current.settings["strategy_spec_fingerprint"]
        )


def test_the_router_carries_the_route_ledger_across_and_keeps_running(
    released_over: tuple[RouteAWorld, dict[str, Any]],
) -> None:
    route, state = released_over
    for instance in instance_of(route, STRATEGY_ROLE):
        assert run_role(route, STRATEGY_ROLE, instance=instance).entered

    run = run_role(route, ROUTER_ROLE, instance=instance_of(route, ROUTER_ROLE)[0])

    assert run.entered, f"{run.refusal!r}\n{run.traceback}"
    assert run.violations == []
    assert "generation changed" not in (run.last_error or "")
    router = manifests_of(route, RuntimeServiceKind.SIGNAL_ROUTER)[0]
    bus = SignalBusStore(Path(str(router.settings["signal_bus_path"])))
    for source_id, old_generation in state["sources"].items():
        rotations = bus.route_source_rotations(source_id)
        assert [item.previous_source_generation_id for item in rotations] == [old_generation]
        #: nothing was ever routed under the old generation, so nothing was abandoned
        assert rotations[0].abandoned_sequences == 0


def test_every_candidate_publisher_rebinds_its_authority(
    released_over: tuple[RouteAWorld, dict[str, Any]],
) -> None:
    route, state = released_over
    previous = first_generation_id(route)

    for instance in instance_of(route, CANDIDATE_ROLE):
        run = run_role(route, CANDIDATE_ROLE, instance=instance)
        assert run.entered, f"{instance}: {run.refusal!r}\n{run.traceback}"
        assert "bound to a different identity" not in (run.last_error or "")

    for service_id, entry in state["candidates"].items():
        archive = entry["root"] / f"rotated-{previous}"
        assert archive.is_dir(), service_id
        assert (archive / "authority.json").is_file()
        binding = json.loads((entry["root"] / "authority.json").read_text(encoding="utf-8"))
        assert binding["definition_fingerprint"] != entry["definition_fingerprint"]
        assert binding["executable_fingerprint"] != entry["executable_fingerprint"]
        archived = json.loads((archive / "authority.json").read_text(encoding="utf-8"))
        assert archived["definition_fingerprint"] == entry["definition_fingerprint"]


def test_the_health_payload_survives_the_superseded_heartbeat(
    released_over: tuple[RouteAWorld, dict[str, Any]],
) -> None:
    route, state = released_over
    catalog = manifests_of(route, RuntimeServiceKind.LAB_ARTIFACT_CATALOG)[0]
    feature = manifests_of(route, RuntimeServiceKind.FEATURE_LIVE)[0]
    feature_control = route.runtime_root / "control" / "features" / _instance_name(
        feature.service_id
    )
    control = RuntimeServiceControl(
        feature_control,
        spec=feature.service_spec,
        clock=lambda: FROZEN_NOW,
    )
    control.start()
    control.record_success(RuntimeStepResult(input_sequence=1, output_sequence=1))
    control.stop(reason="fixture complete")

    from rquant.runtime_generation_lineage import previous_spec_identities

    sources = (
        RuntimeHealthControlSource(
            control_root=state["heartbeat"]["control_root"],
            spec=catalog.service_spec,
        ),
        RuntimeHealthControlSource(control_root=feature_control, spec=feature.service_spec),
    )
    result = RuntimeHealthSourceReader(
        sources=sources,
        serving_service_id="serving.publisher.v1",
        previous_spec_identities=previous_spec_identities(
            route.runtime_root,
            service_ids=(catalog.service_id, feature.service_id),
        ),
    )(FROZEN_NOW)

    entries = {item.service_id: item for item in result.payload.runtime_services}
    assert set(entries) == {catalog.service_id, feature.service_id}
    assert f"superseded:{catalog.service_id}" in (result.reason or "")
    assert entries[catalog.service_id].heartbeat is None
    assert entries[feature.service_id].heartbeat is not None


# ---------------------------------------------------------------------------------------
# The negative half: a foreign identity is still refused, one shape at a time
# ---------------------------------------------------------------------------------------


def test_a_foreign_runner_identity_still_stops_the_strategy(
    released_over: tuple[RouteAWorld, dict[str, Any]],
) -> None:
    route, state = released_over
    service_id, entry = next(iter(state["runners"].items()))
    _rewrite_spec_fingerprint(entry["path"], FOREIGN)
    instance = _instance_name(service_id)

    run = run_role(route, STRATEGY_ROLE, instance=instance)

    assert not run.entered
    assert "persisted runner identity" in str(run.refusal)
    assert not list(entry["path"].parent.glob("*.archived"))


def test_a_foreign_source_generation_still_stops_the_router(
    released_over: tuple[RouteAWorld, dict[str, Any]],
) -> None:
    route, state = released_over
    for instance in instance_of(route, STRATEGY_ROLE):
        assert run_role(route, STRATEGY_ROLE, instance=instance).entered
    router = manifests_of(route, RuntimeServiceKind.SIGNAL_ROUTER)[0]
    bus_path = Path(str(router.settings["signal_bus_path"]))
    #: a source row nobody in this lineage ever published
    _rewrite_route_source_spec(bus_path, FOREIGN)

    run = run_role(route, ROUTER_ROLE, instance=instance_of(route, ROUTER_ROLE)[0])

    assert "generation changed" in (run.last_error or "") or isinstance(
        run.refusal,
        SignalRouteConflictError,
    )


def test_a_foreign_candidate_binding_is_left_alone_and_still_refuses(
    released_over: tuple[RouteAWorld, dict[str, Any]],
) -> None:
    """The publisher does not re-bind what it did not write, and publishing still refuses.

    Two halves, because the role has two moments. At *startup* it looks at the binding on
    disk and, finding fingerprints no generation of ours published, does nothing at all —
    which is what it must do: it may not archive somebody else's state. At *publish* the
    refusal is exactly the one the host reported, unchanged.
    """

    route, state = released_over
    service_id, entry = next(iter(state["candidates"].items()))
    root = entry["root"]
    previous = first_generation_manifest(route, service_id)
    current = next(
        item
        for item in manifests_of(route, RuntimeServiceKind.CANDIDATE_PUBLISHER)
        if item.service_id == service_id
    )
    #: a binding whose fingerprints belong to no generation of ours
    for child in (root / "generations").iterdir():
        child.unlink()
    for name in ("generation-index.json", "current.json", "authority.json"):
        (root / name).unlink()
    StrategyCandidateSnapshotSpool(root).publish_strategy_records(
        strategy_id=str(previous.settings["strategy_id"]),
        strategy_version="1",
        definition_fingerprint=FOREIGN,
        executable_fingerprint="d" * 64,
        candidate_schema_fingerprint=str(previous.settings["candidate_schema_fingerprint"]),
        static_feature_schema=dict(previous.settings["static_feature_schema"]),
        source_snapshot_ids={"candidate_input": "2" * 64},
        trade_date=TRADE_DATE,
        captured_at=FROZEN_NOW,
        producer_commit=previous.producer_commit,
        rows=(),
    )

    run_role(route, CANDIDATE_ROLE, instance=_instance_name(service_id))

    assert not any(item.name.startswith("rotated-") for item in root.iterdir())
    binding = json.loads((root / "authority.json").read_text(encoding="utf-8"))
    assert binding["definition_fingerprint"] == FOREIGN

    from rquant.runtime_generation_lineage import candidate_authority_lineage

    spool = StrategyCandidateSnapshotSpool(
        root,
        previous_generation_of_binding=candidate_authority_lineage(
            route.runtime_root,
            service_id=service_id,
        ),
    )
    with pytest.raises(
        StrategyCandidateSnapshotIntegrityError,
        match="bound to a different identity",
    ):
        spool.publish_strategy_records(
            strategy_id=str(current.settings["strategy_id"]),
            strategy_version="1",
            definition_fingerprint=str(current.settings["definition_fingerprint"]),
            executable_fingerprint=str(current.settings["executable_fingerprint"]),
            candidate_schema_fingerprint=str(current.settings["candidate_schema_fingerprint"]),
            static_feature_schema=dict(current.settings["static_feature_schema"]),
            source_snapshot_ids={"candidate_input": "3" * 64},
            trade_date=TRADE_DATE,
            captured_at=FROZEN_NOW,
            producer_commit=current.producer_commit,
            rows=(),
        )
    assert not any(item.name.startswith("rotated-") for item in root.iterdir())


def test_a_live_heartbeat_with_a_foreign_fingerprint_still_degrades_that_role(
    released_over: tuple[RouteAWorld, dict[str, Any]],
) -> None:
    route, state = released_over
    catalog = manifests_of(route, RuntimeServiceKind.LAB_ARTIFACT_CATALOG)[0]
    path = RuntimeServiceControl._path_for(
        state["heartbeat"]["control_root"],
        catalog.service_spec,
    )
    stored = json.loads(path.read_text(encoding="utf-8"))
    stored["spec_fingerprint"] = FOREIGN
    stored["status"] = RuntimeServiceStatus.RUNNING.value
    stored["stopped_at"] = None
    stored["stop_reason"] = None
    path.write_text(json.dumps(stored), encoding="utf-8")

    from rquant.runtime_generation_lineage import previous_spec_identities

    result = RuntimeHealthSourceReader(
        sources=(
            RuntimeHealthControlSource(
                control_root=state["heartbeat"]["control_root"],
                spec=catalog.service_spec,
            ),
        ),
        serving_service_id="serving.publisher.v1",
        previous_spec_identities=previous_spec_identities(
            route.runtime_root,
            service_ids=(catalog.service_id,),
        ),
    )(FROZEN_NOW)

    entry = result.payload.runtime_services[0]
    assert entry.status is RuntimeServiceStatus.DEGRADED
    assert f"unreadable:{catalog.service_id}" in (result.reason or "")
    assert f"superseded:{catalog.service_id}" not in (result.reason or "")


# ---------------------------------------------------------------------------------------


def _persisted_spec_fingerprint(path: Path) -> str:
    connection = sqlite3.connect(path)
    try:
        row = connection.execute(
            "SELECT strategy_spec_fingerprint FROM runner_metadata WHERE singleton = 1"
        ).fetchone()
    finally:
        connection.close()
    return str(row[0])


def _rewrite_spec_fingerprint(path: Path, fingerprint: str) -> None:
    connection = sqlite3.connect(path, isolation_level=None)
    try:
        connection.execute(
            "UPDATE runner_metadata SET strategy_spec_fingerprint = ? WHERE singleton = 1",
            (fingerprint,),
        )
    finally:
        connection.close()


def _rewrite_route_source_spec(path: Path, fingerprint: str) -> None:
    connection = sqlite3.connect(path, isolation_level=None)
    try:
        connection.execute(
            "UPDATE signal_route_source SET strategy_spec_fingerprint = ?",
            (fingerprint,),
        )
    finally:
        connection.close()
