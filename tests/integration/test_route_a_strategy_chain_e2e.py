"""#218 A acceptance: the live plane's strategy chain over a real Route A deployment.

The 2026-09-07 Route A window died at the very first step of the chain: all three
`strategy_live` services raised `strategy completion signer profile contains invalid
manifests`, so `signal_router` never found a `runner.sqlite3` and `paper_broker` never
found a route spool. `route-a-218-scout.md` §1 traced the message to a pydantic
re-validation of the profile manifests the loader had already frozen, and #200 reached the
host because its acceptance was a stub, so nothing here is stubbed on the path under test:

* the world is `tests.integration.test_route_a_legacy_binding_e2e`'s — a real
  `install_runtime_deployment_bundle` legacy root, a real
  `runtime-authority-stage --legacy-runtime-root` generation published into a root-owned
  chain, and argv the wrapper's own `resolve_launch` derived and verified;
* the profile is the real `build_production_runtime_profile` output, whose nine
  nested-settings manifests are exactly what the re-validation choked on;
* `runtime_service_main.run()` executes with that argv and enters the real service loop.

Two things the world's `_inputs` fixture leaves as placeholders have to become real here,
because the code under test reads them rather than the profile's copy of them: the Shadow
completion Ed25519 public key (the signer builds a keyring out of it) and the frozen
routing policy document (the router hashes it). Both are generated per test.

The chain's own startup order is the second thing this file pins, and it is not the order
the runbook assumed — see the case about a strategy started before the signal bus exists.
"""

from __future__ import annotations

import hashlib
import sys
from decimal import Decimal
from pathlib import Path
from types import MappingProxyType
from typing import Any

import pytest

import rquant.runtime_service_main as service_main
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from tests.integration.test_route_a_legacy_binding_e2e import (
    PRODUCTION_ROOT,
    RouteAWorld,
    _route_a_world,
)
from tests.paper_cost_fixtures import paper_cost_policy
from tests.shadow_ed25519_support import create_shadow_ed25519_test_authority

pytestmark = pytest.mark.integration

STRATEGY_ROLE = "strategy_live"
ROUTER_ROLE = "signal_router"
NOTIFIER_ROLE = "notifier"

#: The message the host reported for all three strategies, and the one this file removes.
INVALID_MANIFESTS = "strategy completion signer profile contains invalid manifests"


def _routing_policy_payload() -> bytes:
    """The production generator's own minimal policy document, byte for byte."""

    scripts = Path(__file__).resolve().parents[2] / "scripts"
    if str(scripts) not in sys.path:
        sys.path.insert(0, str(scripts))
    from build_runtime_production_inputs import build_routing_policy_payload

    return build_routing_policy_payload(
        recipient_id="admin",
        channel="pushdeer",
        default_no_target_reason="no_target",
    )


@pytest.fixture
def route_a(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> RouteAWorld:
    """A published Route A chain whose profile a strategy service can actually open.

    The paper broker database is created here for the same reason the legacy-binding file
    creates the reference registry: on a real host the paper-broker service owns it and the
    strategy only reads it, and which service owns which file is not what is under test.
    The signal bus deliberately stays absent — the first case below is about that.
    """

    import tests.unit.test_runtime_production_profile as profile_fixtures

    authority = create_shadow_ed25519_test_authority(tmp_path / "shadow-completion-keys")
    public_key_pem = authority.keyring._keys[authority.keyring.active_key_id].decode("utf-8")
    policy_payload = _routing_policy_payload()
    real_inputs = profile_fixtures._inputs

    def real_shadow_and_policy_inputs(path: Path) -> Any:
        inputs = real_inputs(path)
        policy_path = Path(inputs.routing_policy_path)
        policy_path.parent.mkdir(parents=True, exist_ok=True)
        policy_path.write_bytes(policy_payload)
        policy_path.chmod(0o444)
        return inputs.model_copy(
            update={
                "shadow_completion_active_key_id": authority.keyring.active_key_id,
                "shadow_completion_active_public_key_pem": public_key_pem,
                "routing_policy_fingerprint": hashlib.sha256(policy_payload).hexdigest(),
            }
        )

    monkeypatch.setattr(profile_fixtures, "_inputs", real_shadow_and_policy_inputs)
    route = _route_a_world(tmp_path, monkeypatch)
    route.stage_and_publish()
    _open_paper_broker(route)
    return route


def _strategy_manifests(route: RouteAWorld) -> tuple[RuntimeServiceManifest, ...]:
    return tuple(
        manifest
        for manifest in route.profile.manifests
        if manifest.service_kind is RuntimeServiceKind.STRATEGY_LIVE
    )


def _open_paper_broker(route: RouteAWorld) -> None:
    from rquant.paper_broker import PaperBrokerStore

    manifest = _strategy_manifests(route)[0]
    path = Path(str(manifest.settings["paper_broker_path"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    PaperBrokerStore(
        path,
        account_id=str(manifest.settings["paper_account_id"]),
        initial_cash=Decimal("100000"),
        cost_policy=paper_cost_policy(),
    )


def _signal_bus_path(route: RouteAWorld) -> Path:
    return Path(str(_strategy_manifests(route)[0].settings["signal_bus_path"]))


def _open_signal_bus(route: RouteAWorld) -> Path:
    """Create `live/signal-bus/signal_bus.sqlite3` the way the router's own store does."""

    from rquant.signal_bus import SignalBusStore

    path = _signal_bus_path(route)
    path.parent.mkdir(parents=True, exist_ok=True)
    SignalBusStore(path)
    return path


def _argv(route: RouteAWorld, role: str, instance: str) -> list[str]:
    """The wrapper's own argv for one instance, with the frozen control root prefix moved.

    `RouteAWorld.argv` does the same thing but insists the role has exactly one instance;
    `strategy_live` has three.
    """

    argv = list(route.world.resolve(role, instance)["module_argv"])
    index = argv.index("--control-root") + 1
    argv[index] = str(route.runtime_root / Path(argv[index]).relative_to(PRODUCTION_ROOT))
    return argv


def _instances(route: RouteAWorld, role: str) -> tuple[str, ...]:
    return tuple(route.world.instances(route.plan)[role])


def _runner_databases(route: RouteAWorld) -> list[Path]:
    return sorted((route.runtime_root / "live" / "strategies").glob("*/runner.sqlite3"))


def _start_strategies(route: RouteAWorld) -> None:
    for instance in _instances(route, STRATEGY_ROLE):
        assert route.run_role(STRATEGY_ROLE, argv=_argv(route, STRATEGY_ROLE, instance)) == 0


# ---------------------------------------------------------------------------------------
# The premise: the installed profile is the one that broke the re-validation
# ---------------------------------------------------------------------------------------


def _has_frozen_container(manifest: RuntimeServiceManifest) -> bool:
    return any(
        isinstance(value, MappingProxyType | tuple) for value in manifest.settings.values()
    )


def test_the_installed_profile_carries_the_nested_settings_that_broke_the_revalidation(
    route_a: RouteAWorld,
) -> None:
    """Nine manifests in the real profile, and a bare re-validation of one still refuses."""

    nested = [manifest for manifest in route_a.profile.manifests if _has_frozen_container(manifest)]
    assert len(nested) >= 9, [manifest.service_id for manifest in nested]

    with pytest.raises(ValueError, match="invalid-json-value"):
        RuntimeServiceManifest.model_validate(nested[0])

    #: and the strategies' own settings are all scalars, which is why the host's message
    #: was identical for all three of them
    for manifest in _strategy_manifests(route_a):
        assert not _has_frozen_container(manifest)


# ---------------------------------------------------------------------------------------
# The acceptance
# ---------------------------------------------------------------------------------------


def test_the_completion_signer_opens_for_every_strategy_over_a_real_installed_profile(
    route_a: RouteAWorld,
) -> None:
    """The direct form of the fix: the call the host died in, against the real profile."""

    manifests = _strategy_manifests(route_a)
    assert len(manifests) == 3

    for manifest in manifests:
        signer, key_id = service_main.build_runtime_strategy_completion_attestation_signer(
            route_a.runtime_root, manifest=manifest
        )
        assert key_id == signer.key_id
        assert key_id


def test_the_strategy_roles_reach_their_service_loop_over_a_real_current(
    route_a: RouteAWorld,
) -> None:
    """All three roles enter `run_runtime_service_manifest` and run one real iteration."""

    _open_signal_bus(route_a)
    _start_strategies(route_a)

    databases = _runner_databases(route_a)
    assert len(databases) == 3
    assert all(path.is_file() for path in databases)


def test_a_strategy_role_starts_before_the_router_and_leaves_its_runner_database(
    route_a: RouteAWorld,
) -> None:
    """The startup order the runbook has to use, pinned as behaviour rather than as prose.

    This case used to assert the opposite, and the assertion was the bug: `strategy_live`
    opened a read-only route authority over `live/signal-bus/signal_bus.sqlite3` while it
    was building its step, and only `signal_router` creates that file — which in turn
    refused to start until every strategy's `runner.sqlite3` existed. Neither unit could
    be ordered against the other, so the runbook told the operator to start the
    strategies, let all three fail, start the router inside a 50 s window, and start them
    again (#220). Package J removed the cycle: the bus is read at one point, the
    session-close attestation, and it is opened there rather than up front.
    """

    bus = _signal_bus_path(route_a)
    assert not bus.exists()

    _start_strategies(route_a)

    assert len(_runner_databases(route_a)) == 3
    assert not bus.exists()

    #: and the router, started after them, finds the three databases waiting for it
    router = _instances(route_a, ROUTER_ROLE)[0]
    assert route_a.run_role(ROUTER_ROLE, argv=_argv(route_a, ROUTER_ROLE, router)) == 0
    assert bus.is_file()


def test_the_router_publishes_the_spool_source_document_the_readers_wait_for(
    route_a: RouteAWorld,
) -> None:
    """Readiness probe two: `spool/source.json` exists only after one router step ran."""

    _open_signal_bus(route_a)
    _start_strategies(route_a)
    spool = route_a.runtime_root / "live" / "signal-bus" / "spool"
    assert not spool.exists()

    router = _instances(route_a, ROUTER_ROLE)[0]
    assert route_a.run_role(ROUTER_ROLE, argv=_argv(route_a, ROUTER_ROLE, router)) == 0

    assert (spool / "records").is_dir()
    assert (spool / "source.json").is_file()


def test_the_notifier_starts_once_the_router_has_published_the_spool(
    route_a: RouteAWorld,
) -> None:
    """The tail of the chain: a spool reader that used to die on `route spool is unavailable`."""

    _open_signal_bus(route_a)
    _start_strategies(route_a)
    router = _instances(route_a, ROUTER_ROLE)[0]
    assert route_a.run_role(ROUTER_ROLE, argv=_argv(route_a, ROUTER_ROLE, router)) == 0

    notifier = _instances(route_a, NOTIFIER_ROLE)[0]
    assert route_a.run_role(NOTIFIER_ROLE, argv=_argv(route_a, NOTIFIER_ROLE, notifier)) == 0
