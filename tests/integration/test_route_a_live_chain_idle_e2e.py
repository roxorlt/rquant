"""#231/#232/#220 acceptance: the live chain starting cold, idle, and sandboxed.

The 2026-09-08 Route A window (v0.33.1, authority sequence 2) is the case this file is
built out of. Nothing was trading, no signal existed anywhere on the host, and four roles
still could not start:

* `strategy_live` x3 — `OSError: [Errno 30] Read-only file system:
  '/home/lighthouse/rquant/data/runtime/live/features/.feature-spool.lock'` (#231). The
  strategy unit's `ReadWritePaths` is `live/strategies/%i` and the spool's lock lives in
  the feature role's directory.
* `signal_router` — `ValueError: runner source is unavailable:
  .../live/strategies/svc-3326…/runner.sqlite3`, five times (#232). The three strategy
  directories were empty, because the strategies had died before the line that creates
  the database, and idle strategies were never going to be pushed a signal that night.
* `paper_broker` and `notifier` — waiting on the router's spool (#220).

Every one of those exits fired `OnFailure=rquant-alert@%n.service`.

So the test is the window: two real installed generations, the second over the first --
which is also the install that prepares the schema rollout plans every kind-backed role
opens on its way in, acknowledged here the way the runbook acknowledges them (#229) -- a
real staged and published authority chain, the wrapper's own argv and child environment,
an empty feature spool that the feature role has initialised and nothing has published
into, a clock outside market hours on a date the bundle's calendar does not open, and
each role running under its own unit's `ReadWritePaths` — read verbatim out of
`deploy/systemd/`, so the sandbox this test applies is the one the host applies, and a
role that writes outside it fails here for the same reason it would fail there.

The order is the runbook's C-3 order, and it is now the natural one: strategy x3, then
`signal_router`, then `paper_broker`, then `notifier`.
"""

from __future__ import annotations

import hashlib
import os
import re
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

import rquant.runtime_service_builtin as builtin_module
import rquant.runtime_service_main as service_main
from rquant.feature_spool import FeatureBatchSpool
from rquant.runtime_capabilities import RUNTIME_CAPABILITY_CREDENTIAL_NAME
from rquant.runtime_deployment_bundle import acknowledge_runtime_schema_rollout_preparation
from rquant.runtime_exec_wrapper import _verify
from rquant.runtime_peer_artifacts import PeerArtifactUnavailableError
from rquant.runtime_service_control import RuntimeServiceControl, RuntimeServiceStatus
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from tests.integration.test_route_a_legacy_binding_e2e import (
    PRODUCTION_ROOT,
    RouteAWorld,
    _production_bundle,
    _StopAfterOneIteration,
)
from tests.integration.test_route_a_strategy_chain_e2e import _routing_policy_payload
from tests.runtime_readonly_sandbox import readonly_runtime, tree_state
from tests.shadow_ed25519_support import create_shadow_ed25519_test_authority
from tests.unit.test_runtime_authority_publish import UID, World

pytestmark = pytest.mark.integration


def _idle_clock() -> datetime:
    """22:00 UTC, which is 06:00 the next morning in Shanghai: before the auction.

    Two things constrain this instant. It has to be outside a trading session, which is
    the premise of the whole file, and it must not be behind the fixture's own files: the
    router reads a frozen routing policy and refuses one whose mtime is in the future
    (`runtime_routing_policy._read_frozen_policy`), and every fixture file is written
    while the test runs. Tomorrow evening satisfies both whenever the suite runs, and the
    bundle's calendar opens exactly one date in 2026-08, so no date reached here is ever
    an open one.
    """

    return (datetime.now(UTC) + timedelta(days=1)).replace(
        hour=22,
        minute=0,
        second=0,
        microsecond=0,
    )


FROZEN_NOW = _idle_clock()

#: the generation this window's bundle was installed over, as in the third Route A window
PREVIOUS_COMMIT = "1e2d3c4b5a69788796a5b4c3d2e1f00918273645"

STRATEGY_ROLE = "strategy_live"
ROUTER_ROLE = "signal_router"
BROKER_ROLE = "paper_broker"
NOTIFIER_ROLE = "notifier"

#: role -> the unit file its sandbox is read out of.
_UNIT_FILES = {
    STRATEGY_ROLE: "rquant-runtime-strategy@.service",
    ROUTER_ROLE: "rquant-runtime-signal-router@.service",
    BROKER_ROLE: "rquant-runtime-paper-broker@.service",
    NOTIFIER_ROLE: "rquant-runtime-notifier@.service",
}
_UNIT_ROOT = Path(__file__).resolve().parents[2] / "deploy" / "systemd"

#: The message the host reported for all three strategies, and the one this file removes.
FEATURE_SPOOL_LOCK = ".feature-spool.lock"


# ---------------------------------------------------------------------------------------
# The sandbox each role runs under, taken from its own unit
# ---------------------------------------------------------------------------------------


def read_write_paths(role: str, *, instance: str, runtime_root: Path) -> tuple[Path, ...]:
    """`ReadWritePaths=` from the role's unit, with `%i` and the frozen prefix resolved.

    Reading the unit rather than restating it is the point: if the sandbox a role runs
    under ever changes, this test changes with it, and a role that starts writing outside
    what its unit grants fails here before it fails on the host.
    """

    text = (_UNIT_ROOT / _UNIT_FILES[role]).read_text(encoding="utf-8")
    declared = re.findall(r"^ReadWritePaths=(.*)$", text, flags=re.MULTILINE)
    assert declared, role
    paths: list[Path] = []
    for entry in " ".join(declared).split():
        #: a leading "-" is systemd's "ignore if absent", not part of the path
        absolute = Path(entry.lstrip("-").replace("%i", instance))
        paths.append(runtime_root / absolute.relative_to(PRODUCTION_ROOT))
    return tuple(paths)


# ---------------------------------------------------------------------------------------
# Driving one role the way the wrapper does
# ---------------------------------------------------------------------------------------


def _instance_name(service_id: str) -> str:
    return "svc-" + hashlib.sha256(service_id.encode("utf-8")).hexdigest()


def manifests_for(route: RouteAWorld, role: str) -> tuple[RuntimeServiceManifest, ...]:
    kind = RuntimeServiceKind(role)
    return tuple(
        manifest for manifest in route.profile.manifests if manifest.service_kind is kind
    )


def launch(route: RouteAWorld, role: str, instance: str, credentials: Path | None) -> Any:
    """The wrapper's own answer for this instance: argv plus the child environment."""

    return _verify.resolve_launch(
        role,
        instance=instance,
        profile_path=str(route.world.profile_path),
        authority_path=str(route.world.authority_path),
        generation_root=str(route.world.generations),
        trusted_root=str(route.world.root),
        expected_owner_uid=UID,
        source_environment=(
            {"LANG": "C", "TZ": "UTC"}
            if credentials is None
            else {"LANG": "C", "TZ": "UTC", "CREDENTIALS_DIRECTORY": str(credentials)}
        ),
    )


def _relocated(route: RouteAWorld, module_argv: list[str]) -> list[str]:
    """The one seam package A froze: `--control-root` under a path no test can own."""

    argv = list(module_argv)
    index = argv.index("--control-root") + 1
    argv[index] = str(route.runtime_root / Path(argv[index]).relative_to(PRODUCTION_ROOT))
    return argv


def deliver_credential(directory: Path, plaintext: bytes) -> Path:
    """Lay the capability credential out the way `LoadCredentialEncrypted` does."""

    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    directory.chmod(0o700)
    path = directory / RUNTIME_CAPABILITY_CREDENTIAL_NAME
    path.write_bytes(plaintext)
    path.chmod(0o400)
    observed = path.lstat()
    assert observed.st_uid == os.geteuid()
    assert observed.st_nlink == 1
    assert observed.st_mode & 0o077 == 0
    return directory


def run_role(
    route: RouteAWorld,
    role: str,
    *,
    instance: str,
    credentials: Path | None = None,
    sandboxed: bool = True,
) -> tuple[int, list[Any], Any]:
    """One role, one loop iteration, inside the sandbox its own unit describes."""

    resolved = launch(route, role, instance, credentials)
    argv = _relocated(route, list(resolved["module_argv"]))
    arguments = service_main.build_parser().parse_args(argv)
    stop = _StopAfterOneIteration()
    real_event = service_main.Event
    real_registry = builtin_module.build_builtin_registry
    service_main.Event = lambda: stop  # type: ignore[assignment]
    builtin_module.build_builtin_registry = (  # type: ignore[assignment]
        lambda **kwargs: real_registry(clock=lambda: FROZEN_NOW, **kwargs)
    )
    writable = (
        read_write_paths(role, instance=instance, runtime_root=route.runtime_root)
        if sandboxed
        else (route.runtime_root,)
    )
    try:
        with (
            readonly_runtime(route.runtime_root, writable=writable) as violations,
            mock.patch.dict(os.environ, dict(resolved["environment"]), clear=True),
        ):
            code = service_main.run(arguments)
    finally:
        service_main.Event = real_event  # type: ignore[assignment]
        builtin_module.build_builtin_registry = real_registry  # type: ignore[assignment]
    assert stop.iterations == 1, f"{role} never entered its service loop"
    control_root = Path(argv[argv.index("--control-root") + 1])
    manifest = next(
        manifest
        for manifest in manifests_for(route, role)
        if _instance_name(manifest.service_id) == instance
    )
    heartbeat = RuntimeServiceControl.read_heartbeat(control_root, manifest.service_spec)
    return code, violations, heartbeat


def instances_of(route: RouteAWorld, role: str) -> tuple[str, ...]:
    return tuple(
        _instance_name(manifest.service_id) for manifest in manifests_for(route, role)
    )


# ---------------------------------------------------------------------------------------
# The host's own starting state
# ---------------------------------------------------------------------------------------


def feature_root(route: RouteAWorld) -> Path:
    return Path(str(manifests_for(route, STRATEGY_ROLE)[0].settings["feature_spool_root"]))


def signal_bus_path(route: RouteAWorld) -> Path:
    return Path(str(manifests_for(route, STRATEGY_ROLE)[0].settings["signal_bus_path"]))


def spool_root(route: RouteAWorld) -> Path:
    return Path(str(manifests_for(route, NOTIFIER_ROLE)[0].settings["signal_spool_root"]))


def broker_ledger(route: RouteAWorld) -> Path:
    return Path(str(manifests_for(route, STRATEGY_ROLE)[0].settings["paper_broker_path"]))


def runner_databases(route: RouteAWorld) -> list[Path]:
    return sorted((route.runtime_root / "live" / "strategies").glob("*/runner.sqlite3"))


@pytest.fixture
def cold_chain(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> RouteAWorld:
    """Two installed generations, acknowledged, and no live-plane role ever run.

    Two, because the 2026-09-08 window was the second generation installed over the
    first, and because installing over a previous generation is what prepares the schema
    rollout plans — the state stores every kind-backed role opens on its way in (#227,
    #229). A one-generation world would leave that whole surface out of the chain this
    file is about. The PREPARE round is acknowledged the way the installer's own command
    does it, which is the runbook step between B-7 and C-3.

    Package F's fixture for the same world creates the paper broker's ledger up front,
    "because on a real host the paper-broker service owns it and the strategy only reads
    it". That is exactly what this file may not assume: on 2026-09-08 the broker had not
    started either, and the strategy's construction depended on its ledger. So the two
    inputs the code reads rather than the profile — the Shadow completion public key and
    the frozen routing policy — are made real here, and so is the third one this file is
    the first to need: the paper broker's PIT trade calendar, which its quote resolver
    hashes while the step is built. All three come from the production generator, so the
    bytes are the host's bytes. Nothing else is created.
    """

    import tests.unit.test_runtime_production_profile as profile_fixtures

    authority = create_shadow_ed25519_test_authority(tmp_path / "shadow-completion-keys")
    public_key_pem = authority.keyring._keys[authority.keyring.active_key_id].decode("utf-8")
    policy_payload = _routing_policy_payload()  # this puts `scripts` on `sys.path`
    from build_runtime_production_inputs import build_pit_trade_calendar_payload
    #: the one date `_route_a_world`'s market calendar authority opens, and nothing else
    trade_calendar_payload = build_pit_trade_calendar_payload(
        ((date(2026, 8, 3), True, datetime(2026, 8, 3, tzinfo=UTC)),)
    )
    real_inputs = profile_fixtures._inputs

    def real_shadow_and_policy_inputs(path: Path) -> Any:
        inputs = real_inputs(path)
        policy_path = Path(inputs.routing_policy_path)
        policy_path.parent.mkdir(parents=True, exist_ok=True)
        policy_path.write_bytes(policy_payload)
        policy_path.chmod(0o444)
        trade_calendar_path = Path(inputs.trade_calendar_path)
        trade_calendar_path.parent.mkdir(parents=True, exist_ok=True)
        trade_calendar_path.write_bytes(trade_calendar_payload)
        trade_calendar_path.chmod(0o600)
        return inputs.model_copy(
            update={
                "shadow_completion_active_key_id": authority.keyring.active_key_id,
                "shadow_completion_active_public_key_pem": public_key_pem,
                "routing_policy_fingerprint": hashlib.sha256(policy_payload).hexdigest(),
                "trade_calendar_sha256": hashlib.sha256(trade_calendar_payload).hexdigest(),
            }
        )

    monkeypatch.setattr(profile_fixtures, "_inputs", real_shadow_and_policy_inputs)

    world = World(tmp_path / "root", monkeypatch).build()
    runtime_root = tmp_path / "host" / "data" / "runtime"
    _production_bundle(
        tmp_path / "previous",
        monkeypatch,
        producer_commit=PREVIOUS_COMMIT,
        runtime_root=runtime_root,
        schema_bootstrap_reason="#231 acceptance bootstrap",
    )
    inputs, profile, receipt, sealed = _production_bundle(
        tmp_path / "target",
        monkeypatch,
        producer_commit=world.commit,
        runtime_root=runtime_root,
        #: only the first install into an empty root may carry a bootstrap reason, and
        #: the second install without one is what prepares the rollout plans
        schema_bootstrap_reason=None,
        #: one registry root cannot hold two commits' definitions (#225)
        definition_registry_root=runtime_root.parent / f"definitions-{world.commit[:7]}",
    )
    assert receipt.previous_generation_hash is not None
    assert receipt.schema_rollout_plan_ids

    #: `_production_bundle` relocates every external input under `<runtime root>/../external`
    #: when it is given a runtime root, and the roles read the relocated paths. The two
    #: documents the profile only records a hash of have to be written there too — the
    #: same bytes, so the hashes the profile carries still match.
    for path, payload, mode in (
        (Path(inputs.routing_policy_path), policy_payload, 0o444),
        (Path(inputs.trade_calendar_path), trade_calendar_payload, 0o600),
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        path.chmod(mode)

    route = RouteAWorld(world, inputs.runtime_root)
    route.profile = profile
    route.receipt = receipt
    route.sealed_credentials = sealed
    route.stage_and_publish()
    #: `rquant runtime-schema-rollout acknowledge`, which the runbook runs after the
    #: install and before the units: without it every kind-backed role refuses with
    #: `schema producer startup is waiting for every producer PREPARE ACK` (#229)
    acknowledge_runtime_schema_rollout_preparation(route.runtime_root, now=FROZEN_NOW)
    return route


@pytest.fixture
def idle_chain(cold_chain: RouteAWorld) -> RouteAWorld:
    """A published chain on which only `feature_live` has ever run, and published nothing.

    `install_runtime_deployment_bundle` creates `live/features` and leaves it empty; the
    feature role's own builder is what turns it into a spool, and on an idle host it does
    that and then publishes no batch at all. Every other artifact the four roles read is
    deliberately absent, because on 2026-09-08 it was.
    """

    FeatureBatchSpool(feature_root(cold_chain))
    assert not signal_bus_path(cold_chain).exists()
    assert not spool_root(cold_chain).exists()
    assert not broker_ledger(cold_chain).exists()
    assert runner_databases(cold_chain) == []
    return cold_chain


# ---------------------------------------------------------------------------------------
# The chain, in the runbook's order, with no signal anywhere
# ---------------------------------------------------------------------------------------


def test_the_world_is_the_second_generation_installed_over_the_first(
    idle_chain: RouteAWorld,
) -> None:
    """The premise, asserted before anything is asserted about the roles.

    The window this file is built out of was the second generation installed over the
    first, which is also the install that prepares the schema rollout plans every
    kind-backed role opens on its way in. A one-generation world would quietly leave
    that surface out.
    """

    generations = sorted(
        path.name
        for path in (idle_chain.runtime_root / "generations").iterdir()
        if path.is_dir()
    )
    assert len(generations) == 2, generations
    assert idle_chain.receipt.previous_generation_hash in generations
    assert idle_chain.receipt.generation_hash in generations
    assert (idle_chain.runtime_root / "current").is_symlink()
    assert Path(os.readlink(idle_chain.runtime_root / "current")).name == (
        idle_chain.receipt.generation_hash
    )

    rollouts = idle_chain.runtime_root / "control" / "schema-rollouts"
    assert sorted(path.name for path in rollouts.iterdir()) == sorted(
        idle_chain.receipt.schema_rollout_plan_ids
    )



def test_the_whole_live_chain_starts_idle_in_the_runbook_order(
    idle_chain: RouteAWorld,
    tmp_path: Path,
) -> None:
    """strategy x3 -> signal_router -> paper_broker -> notifier, every one in its loop."""

    spool_before = tree_state(feature_root(idle_chain))

    for instance in instances_of(idle_chain, STRATEGY_ROLE):
        code, violations, heartbeat = run_role(idle_chain, STRATEGY_ROLE, instance=instance)
        assert code == 0
        assert violations == [], violations
        assert heartbeat is not None
        assert heartbeat.status is RuntimeServiceStatus.STOPPED
        assert heartbeat.total_successes == 1, heartbeat.last_error
        assert heartbeat.last_error is None

    #: #232: the file the router waits for exists before the router is even started
    assert len(runner_databases(idle_chain)) == 3

    #: #231: the producer's directory is byte-for-byte what the feature role left
    assert tree_state(feature_root(idle_chain)) == spool_before

    router = instances_of(idle_chain, ROUTER_ROLE)[0]
    code, violations, heartbeat = run_role(idle_chain, ROUTER_ROLE, instance=router)
    assert code == 0
    assert violations == [], violations
    assert heartbeat is not None
    assert heartbeat.total_successes == 1, heartbeat.last_error
    assert signal_bus_path(idle_chain).is_file()
    assert (spool_root(idle_chain) / "source.json").is_file()

    broker = instances_of(idle_chain, BROKER_ROLE)[0]
    code, violations, heartbeat = run_role(idle_chain, BROKER_ROLE, instance=broker)
    assert code == 0
    assert violations == [], violations
    assert heartbeat is not None
    #: the broker builds its step and reaches its loop, which is what #220 denied it, and
    #: it creates the ledger the strategies read on the way. Its one iteration then waits
    #: on `authorities/paper-execution`, whose current pointer `paper_constraint_publisher`
    #: owns and has not published on this idle chain: a different producer, a different
    #: package, and a wait inside the loop rather than an exit out of it.
    assert broker_ledger(idle_chain).is_file()
    assert heartbeat.total_successes + heartbeat.total_failures == 1
    assert "route spool" not in (heartbeat.last_error or "")
    assert PeerArtifactUnavailableError.__name__ not in (heartbeat.last_error or "")

    notifier = instances_of(idle_chain, NOTIFIER_ROLE)[0]
    credentials = deliver_credential(
        tmp_path / "credentials" / NOTIFIER_ROLE,
        idle_chain.sealed_credentials[notifier],
    )
    code, violations, heartbeat = run_role(
        idle_chain,
        NOTIFIER_ROLE,
        instance=notifier,
        credentials=credentials,
    )
    assert code == 0
    assert violations == [], violations
    assert heartbeat is not None
    #: same shape as the broker: the tail of the chain reaches its loop, and what its one
    #: iteration then waits on is the fixture's placeholder operational database, not the
    #: route spool that #220 left it without
    assert heartbeat.total_successes + heartbeat.total_failures == 1
    assert "route spool" not in (heartbeat.last_error or "")
    assert PeerArtifactUnavailableError.__name__ not in (heartbeat.last_error or "")

    #: and nothing on the chain produced a signal, which is the premise
    assert tree_state(feature_root(idle_chain)) == spool_before


def test_the_router_started_first_creates_the_bus_and_waits_by_name(
    idle_chain: RouteAWorld,
) -> None:
    """#220 from the other side: the cycle is gone in both directions.

    The runbook order starts the strategies first, so this case is the one the runbook
    used to forbid — and the one an operator reaches for after a restart. The router now
    creates the bus before it looks for a runner database, and says which file it is
    waiting for instead of exiting.
    """

    router = instances_of(idle_chain, ROUTER_ROLE)[0]
    code, violations, heartbeat = run_role(idle_chain, ROUTER_ROLE, instance=router)

    assert code == 0
    assert violations == [], violations
    assert signal_bus_path(idle_chain).is_file()
    assert (spool_root(idle_chain) / "source.json").is_file()
    assert heartbeat is not None
    assert heartbeat.total_failures == 1
    assert PeerArtifactUnavailableError.__name__ in (heartbeat.last_error or "")
    assert "runner.sqlite3" in (heartbeat.last_error or "")

    #: and the wait is on the heartbeat as data, not only as prose: which file, since
    #: when, and for how long. Without a failure threshold this is the only thing that
    #: distinguishes "waiting for a peer that has not started" from "wedged".
    assert heartbeat.waiting_for is not None
    assert heartbeat.waiting_for.endswith("runner.sqlite3")
    assert heartbeat.waiting_since is not None
    assert heartbeat.waited_seconds is not None
    assert heartbeat.waited_seconds >= 0

    #: and the strategies, started after it, come up against the bus it left
    for instance in instances_of(idle_chain, STRATEGY_ROLE):
        code, violations, heartbeat = run_role(idle_chain, STRATEGY_ROLE, instance=instance)
        assert code == 0
        assert violations == [], violations
        assert heartbeat is not None
        assert heartbeat.total_successes == 1, heartbeat.last_error


def test_a_strategy_waits_by_name_while_the_feature_role_has_published_nothing(
    cold_chain: RouteAWorld,
) -> None:
    """The one peer a strategy cannot idle without, and it waits rather than exiting."""

    assert not (feature_root(cold_chain) / "source-identity.json").exists()

    instance = instances_of(cold_chain, STRATEGY_ROLE)[0]
    code, violations, heartbeat = run_role(cold_chain, STRATEGY_ROLE, instance=instance)

    assert code == 0
    assert violations == [], violations
    assert len(runner_databases(cold_chain)) == 1
    assert heartbeat is not None
    assert heartbeat.total_failures == 1
    assert "feature spool" in (heartbeat.last_error or "")


# ---------------------------------------------------------------------------------------
# Reverse: an artifact that is there and wrong still stops the reader
# ---------------------------------------------------------------------------------------


def test_a_corrupt_runner_database_still_stops_the_router(idle_chain: RouteAWorld) -> None:
    """Absence defers; a runner database that is not one refuses, while the step is built."""

    for instance in instances_of(idle_chain, STRATEGY_ROLE):
        run_role(idle_chain, STRATEGY_ROLE, instance=instance)
    databases = runner_databases(idle_chain)
    assert len(databases) == 3
    for sibling in sorted(databases[0].parent.glob("runner.sqlite3-*")):
        sibling.unlink()
    databases[0].write_bytes(b"this is not a runner database")

    router = instances_of(idle_chain, ROUTER_ROLE)[0]
    with pytest.raises(ValueError, match="runner source") as raised:
        run_role(idle_chain, ROUTER_ROLE, instance=router)
    assert not isinstance(raised.value, PeerArtifactUnavailableError)


def test_a_corrupt_route_spool_still_stops_the_paper_broker(idle_chain: RouteAWorld) -> None:
    """The tail of the chain keeps its own fail-closed check over the router's spool."""

    for instance in instances_of(idle_chain, STRATEGY_ROLE):
        run_role(idle_chain, STRATEGY_ROLE, instance=instance)
    router = instances_of(idle_chain, ROUTER_ROLE)[0]
    run_role(idle_chain, ROUTER_ROLE, instance=router)

    source = spool_root(idle_chain) / "source.json"
    assert source.is_file()
    source.write_bytes(b"{}")

    broker = instances_of(idle_chain, BROKER_ROLE)[0]
    _code, violations, heartbeat = run_role(idle_chain, BROKER_ROLE, instance=broker)

    #: `source.source_descriptor()` is the first thing the broker's step does, so this is
    #: the spool check refusing, and not the constraint pointer the idle chain also lacks
    assert violations == [], violations
    assert heartbeat is not None
    assert heartbeat.total_successes == 0
    assert heartbeat.total_failures == 1
    assert "spool" in (heartbeat.last_error or ""), heartbeat.last_error
    assert PeerArtifactUnavailableError.__name__ not in (heartbeat.last_error or "")

    #: and the same spool, replaced outright, is refused while the step is still built
    for entry in sorted(spool_root(idle_chain).rglob("*"), reverse=True):
        entry.unlink() if entry.is_file() else entry.rmdir()
    spool_root(idle_chain).rmdir()
    spool_root(idle_chain).write_bytes(b"not a spool directory")
    with pytest.raises(Exception) as raised:
        run_role(idle_chain, BROKER_ROLE, instance=broker)
    assert not isinstance(raised.value, PeerArtifactUnavailableError)
    assert "spool" in str(raised.value)
