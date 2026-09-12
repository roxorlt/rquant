"""Every instanced runtime role, started once under its own unit's sandbox.

Route A's bare-run scouting (runbook R-20) starts a role with `runtime-exec.pyz` and no
sandbox at all. Two of the 2026-09-08 window's defects were invisible to it and to every
test in this repository, because both are about *where a role may write*:

* `paper_constraint_publisher` (#242) entered its loop bare and failed at build time under
  its unit on every start, five restarts and six `OnFailure` relays, because the reference
  registry was a WAL database and `authorities/reference-slow` is read-only in every unit
  except the publisher's;
* `notifier` (#241) stayed `active` and was DEGRADED on every iteration, because it pinned
  the PageControl generation it reads by creating a directory beside the outbox, in the
  `control` root that belongs to `rquant-page-control.service`.

Package J's e2e applies the real per-unit sandbox to the six roles of the live chain. This
file applies it to all 25 instanced roles of `PRODUCTION_ROLE_POLICY`, over the same real
world: two installed generations (the second is what prepares the schema rollout plans),
a real staged and published authority chain, the wrapper's own argv and child environment,
credentials delivered the way `LoadCredentialEncrypted` delivers them, and a clock outside
market hours on a date the bundle's calendar does not open, so no role reaches for a
session or a network.

Every role's `ReadWritePaths`, `ReadOnlyPaths` and `InaccessiblePaths` are read verbatim
out of `deploy/systemd/`, so the *path set* this file applies is the host's path set, and
a role that starts writing outside its unit fails here before it fails there.

The sandbox itself is not the host's. It is a Python-level simulation (`os.*` and
`builtins.open`), and three things it cannot do are worth naming rather than glossing:

* a write made from C -- SQLite's wal-index, DuckDB, pyarrow, a subprocess -- never
  reaches those wrappers. `tree_state` around every directory the role does not own is
  the end-state check that covers them, and #242's own case needed a real `chmod 0500`
  because the write that mattered came from libsqlite3;
* writes *outside* the runtime root are recorded (`RoleRun.outside`) and not refused, even
  though `ProtectSystem=strict` refuses them on a host, because a test process must be
  able to write its own temporary directory and venv;
* systemd drops a `-` prefixed grant entirely when the path does not exist at start, while
  this harness lets a role create it. `rquant-runtime-artifact-catalog@.service` is the
  live example: its outbox directory has to exist before the unit starts, and "start
  retention first" does not guarantee that.
"""

from __future__ import annotations

import errno
import os
import re
import sys
import tempfile
import traceback
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

import rquant.cli as cli_module
import rquant.runtime_recovery_service as recovery_service
import rquant.runtime_service_builtin as builtin_module
import rquant.runtime_service_main as service_main
from rquant.runtime_authority import PRODUCTION_ROLE_POLICY
from rquant.runtime_capabilities import RUNTIME_CAPABILITY_CREDENTIAL_NAME
from rquant.runtime_exec_wrapper import _verify
from rquant.runtime_service_control import RuntimeServiceControl
from tests.integration.test_route_a_legacy_binding_e2e import (
    PRODUCTION_ROOT,
    RouteAWorld,
    _StopAfterOneIteration,
)
from tests.integration.test_route_a_live_chain_idle_e2e import (
    FROZEN_NOW,
    _instance_name,
    cold_chain,
)
from tests.runtime_readonly_sandbox import SandboxViolation, readonly_runtime, tree_state
from tests.support.systemd_credential_delivery import deliver
from tests.unit.test_runtime_authority_publish import UID

#: re-exported so pytest resolves package J's two-generation world in this module
__all__ = ["cold_chain"]

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]
_UNIT_ROOT = REPO_ROOT / "deploy" / "systemd"

#: role -> the unit file whose sandbox it runs under. 25 roles, 25 units: 22 are
#: `@`-templates instantiated with the role's own `svc-<64 hex>` label, and three are
#: plain units carrying that label as a literal (`page_control`, `artifact_retention`) or
#: sharing one control root between two modes (`runtime_recovery*`).
ROLE_UNITS: dict[str, str] = {
    "artifact_retention": "rquant-artifact-retention.service",
    "auction_match_source": "rquant-runtime-auction-match@.service",
    "auction_universe_publisher": "rquant-runtime-auction-universe@.service",
    "candidate_publisher": "rquant-runtime-candidate@.service",
    "daily_close_source": "rquant-runtime-daily-close@.service",
    "daily_pipeline_orchestrator": "rquant-runtime-daily-orchestrator@.service",
    "feature_live": "rquant-runtime-feature@.service",
    "lab_artifact_catalog": "rquant-runtime-artifact-catalog@.service",
    "lab_jobs_publisher": "rquant-runtime-lab-jobs@.service",
    "market_minute_source": "rquant-runtime-market-minute@.service",
    "notifier": "rquant-runtime-notifier@.service",
    "page_control": "rquant-page-control.service",
    "paper_broker": "rquant-runtime-paper-broker@.service",
    "paper_constraint_publisher": "rquant-runtime-paper-constraint@.service",
    "promotions_publisher": "rquant-runtime-promotions@.service",
    "reference_slow_publisher": "rquant-runtime-reference-slow-publisher@.service",
    "reference_slow_source": "rquant-runtime-reference-slow-source@.service",
    "runtime_health_publisher": "rquant-runtime-runtime-health@.service",
    "runtime_recovery": "rquant-runtime-recovery@.service",
    "runtime_recovery_rehearsal": "rquant-runtime-recovery-rehearsal@.service",
    "serving_publisher": "rquant-runtime-serving@.service",
    "shadow_session": "rquant-runtime-shadow@.service",
    "signal_router": "rquant-runtime-signal-router@.service",
    "strategy_live": "rquant-runtime-strategy@.service",
    "watchlist_quote_source": "rquant-runtime-watchlist-quote@.service",
}

#: the seven roles whose unit carries `LoadCredentialEncrypted=`
CREDSTORE_ROLES = frozenset(
    {
        "artifact_retention",
        "auction_match_source",
        "daily_close_source",
        "market_minute_source",
        "notifier",
        "reference_slow_publisher",
        "reference_slow_source",
    }
)

#: the order the runbook starts them in: health first, then the sources every plane reads,
#: then the live chain in the order package J settled, then the rest.
START_ORDER: tuple[str, ...] = (
    "runtime_health_publisher",
    "reference_slow_source",
    "reference_slow_publisher",
    "market_minute_source",
    #: after the minute source: the constraint publisher opens that spool, and the spool's
    #: own `batches/` directory is the producer's to create
    "paper_constraint_publisher",
    "watchlist_quote_source",
    "auction_universe_publisher",
    "auction_match_source",
    "daily_close_source",
    "daily_pipeline_orchestrator",
    "shadow_session",
    "candidate_publisher",
    "feature_live",
    "strategy_live",
    "signal_router",
    "paper_broker",
    "notifier",
    "serving_publisher",
    "promotions_publisher",
    "lab_jobs_publisher",
    "lab_artifact_catalog",
    "artifact_retention",
    "runtime_recovery",
    "runtime_recovery_rehearsal",
    "page_control",
)


# ---------------------------------------------------------------------------------------
# The sandbox, read out of the unit
# ---------------------------------------------------------------------------------------


def _unit_paths(
    text: str,
    directive: str,
    *,
    instance: str,
    runtime_root: Path,
) -> tuple[Path, ...]:
    declared = re.findall(rf"^{directive}=(.*)$", text, flags=re.MULTILINE)
    paths: list[Path] = []
    for entry in " ".join(declared).split():
        #: a leading "-" is systemd's "ignore if absent", not part of the path
        absolute = Path(entry.lstrip("-").replace("%i", instance))
        if absolute == PRODUCTION_ROOT or PRODUCTION_ROOT in absolute.parents:
            paths.append(runtime_root / absolute.relative_to(PRODUCTION_ROOT))
        else:
            #: `/var/lib/rquant/...`, `/home/lighthouse/rquant/logs`, `.env`: outside the
            #: runtime root, so they matter only as `InaccessiblePaths` entries
            paths.append(absolute)
    return tuple(paths)


def sandbox_of(role: str, *, instance: str, runtime_root: Path) -> dict[str, tuple[Path, ...]]:
    """`ReadWritePaths`, `ReadOnlyPaths` and `InaccessiblePaths` for one instantiated unit.

    Read rather than restated: if a unit's grant changes, this changes with it, and the
    claim "this role wrote nothing outside its unit" is a claim about the deployed file.
    """

    text = (_UNIT_ROOT / ROLE_UNITS[role]).read_text(encoding="utf-8")
    return {
        directive: _unit_paths(text, directive, instance=instance, runtime_root=runtime_root)
        for directive in ("ReadWritePaths", "ReadOnlyPaths", "InaccessiblePaths")
    }


def instance_of(route: RouteAWorld, role: str) -> tuple[str, ...]:
    """Every `svc-<64 hex>` this role runs as in the published plan."""

    labels = route.world.instances(route.plan).get(role, ())
    assert labels, role
    return tuple(labels)


# ---------------------------------------------------------------------------------------
# Driving one role the way the wrapper does
# ---------------------------------------------------------------------------------------


def launch(route: RouteAWorld, role: str, instance: str, credentials: Path | None) -> Any:
    source_environment = {"LANG": "C", "TZ": "UTC"}
    if credentials is not None:
        source_environment["CREDENTIALS_DIRECTORY"] = str(credentials)
    return _verify.resolve_launch(
        role,
        instance=instance,
        profile_path=str(route.world.profile_path),
        authority_path=str(route.world.authority_path),
        generation_root=str(route.world.generations),
        trusted_root=str(route.world.root),
        expected_owner_uid=UID,
        source_environment=source_environment,
    )


def relocated(route: RouteAWorld, module_argv: list[str]) -> list[str]:
    """The one seam package A froze: `--control-root` under a path no test can own."""

    argv = list(module_argv)
    index = argv.index("--control-root") + 1
    argv[index] = str(route.runtime_root / Path(argv[index]).relative_to(PRODUCTION_ROOT))
    return argv


class RoleRun:
    """What one role did: whether it reached its pass, and what it wrote outside its unit."""

    def __init__(self, role: str, instance: str) -> None:
        self.role = role
        self.instance = instance
        self.entered = False
        self.exit_code: int | None = None
        self.violations: list[Any] = []
        self.status: str | None = None
        self.last_error: str | None = None
        #: the peer artifact this role's last iteration was waiting on, if it was (#232)
        self.waiting_for: str | None = None
        #: what this role's last iteration did with the read-only replica (#256, #260)
        self.replica_opened: bool | None = None
        self.replica_read_bytes: int | None = None
        self.refusal: BaseException | None = None
        self.traceback: str | None = None
        #: writes the host would refuse because they are outside the runtime root
        self.outside: list[Any] = []
        #: directories the role does not own whose contents changed around the run
        self.changed_trees: tuple[str, ...] = ()

    def __repr__(self) -> str:  # pragma: no cover - diagnostics only
        return (
            f"RoleRun(role={self.role!r}, entered={self.entered}, code={self.exit_code}, "
            f"status={self.status}, waiting_for={self.waiting_for!r}, "
            f"violations={self.violations}, "
            f"last_error={self.last_error!r}, refusal={self.refusal!r})"
        )


def run_role(
    route: RouteAWorld,
    role: str,
    *,
    instance: str,
    credentials: Path | None = None,
    writable_override: tuple[Path, ...] | None = None,
) -> RoleRun:
    """One role, one pass, inside the sandbox its own unit describes.

    The wrapper's own verification runs *before* the sandbox is entered, as it does in
    package J: on the host the wrapper reads the root-owned chain under the same mount
    namespace, but it only reads, and putting it inside would measure the chain's own
    layout rather than the role's writes.
    """

    resolved = launch(route, role, instance, credentials)
    argv = relocated(route, list(resolved["module_argv"]))
    sandbox = sandbox_of(role, instance=instance, runtime_root=route.runtime_root)
    writable = sandbox["ReadWritePaths"] if writable_override is None else writable_override
    run = RoleRun(role, instance)

    stop = _StopAfterOneIteration()
    real_event = service_main.Event
    real_registry = builtin_module.build_builtin_registry
    service_main.Event = lambda: stop  # type: ignore[assignment]
    builtin_module.build_builtin_registry = (  # type: ignore[assignment]
        lambda **kwargs: real_registry(clock=lambda: FROZEN_NOW, **kwargs)
    )
    #: the end-state proof package J added and this file was missing: the syscall wrappers
    #: cannot see a write made from C (SQLite's wal-index, DuckDB, pyarrow, a subprocess),
    #: so every directory the role does not own is compared byte-for-byte around the run
    unowned = tuple(
        child
        for parent in (route.runtime_root / "live", route.runtime_root / "authorities")
        if parent.is_dir()
        for child in sorted(parent.iterdir())
        if child.is_dir()
        #: a producer subtree this role neither owns nor holds a grant inside of
        and not any(
            granted == child or granted in child.parents or child in granted.parents
            for granted in writable
        )
    )
    trees_before = {path: tree_state(path) for path in unowned}
    outside: list[SandboxViolation] = []
    try:
        with (
            readonly_runtime(
                route.runtime_root,
                writable=writable,
                inaccessible=sandbox["InaccessiblePaths"],
                #: what a test process must be allowed to write wherever it runs
                outside_exempt=(Path(tempfile.gettempdir()), Path(sys.prefix), REPO_ROOT),
                outside=outside,
            ) as violations,
            mock.patch.dict(os.environ, dict(resolved["environment"]), clear=True),
        ):
            try:
                if resolved["module"] == "rquant.runtime_service_main":
                    arguments = service_main.build_parser().parse_args(argv)
                    run.exit_code = service_main.run(arguments)
                    run.entered = stop.iterations == 1
                elif resolved["module"] == "rquant.runtime_recovery_service":
                    #: a oneshot, not a loop: reaching its payload is its whole pass
                    run.exit_code = recovery_service.main(argv)
                    run.entered = run.exit_code == 0
                else:  # pragma: no cover - the roster has no other module
                    raise AssertionError(f"{role} runs an unhandled module")
            except BaseException as error:  # noqa: BLE001 - recorded, then reported
                run.refusal = error
                run.traceback = "".join(
                    traceback.format_exception(type(error), error, error.__traceback__)
                )
    finally:
        service_main.Event = real_event  # type: ignore[assignment]
        builtin_module.build_builtin_registry = real_registry  # type: ignore[assignment]
        run.violations = list(violations)
        run.outside = list(outside)
        run.changed_trees = tuple(
            str(path.relative_to(route.runtime_root))
            for path, before in trees_before.items()
            if tree_state(path) != before
        )

    if resolved["module"] == "rquant.runtime_service_main":
        control_root = Path(argv[argv.index("--control-root") + 1])
        manifest = next(
            manifest
            for manifest in route.profile.manifests
            if _instance_name(manifest.service_id) == instance
        )
        heartbeat = RuntimeServiceControl.read_heartbeat(control_root, manifest.service_spec)
        if heartbeat is not None:
            run.status = heartbeat.status.value
            run.last_error = heartbeat.last_error
            run.waiting_for = heartbeat.waiting_for
            run.replica_opened = heartbeat.replica_opened
            run.replica_read_bytes = heartbeat.replica_read_bytes
    return run


# ---------------------------------------------------------------------------------------
# The world every role gets
# ---------------------------------------------------------------------------------------


@pytest.fixture
def minute_snapshot(monkeypatch: pytest.MonkeyPatch) -> bytes:
    """The historical minute parquet `feature_live` opens before anything else.

    The production inputs fixture names the path and a placeholder id but writes no file,
    so the feature role has never been startable in a Route A world -- package J's own
    fixture creates the feature spool by hand rather than running the role. This writes a
    real empty-schema parquet and takes the id from its bytes, which is the move package J
    made for the routing policy and the trade calendar.

    It has to be installed before `cold_chain` builds the world, and written again at the
    path the install relocated it to, with the same bytes so the profile's id still holds.
    """

    import hashlib

    import pandas as pd

    import tests.unit.test_runtime_production_profile as profile_fixtures

    payload_path = Path(os.environ.get("TMPDIR", "/tmp")) / "pkgl-minute-snapshot.parquet"
    pd.DataFrame(
        columns=(
            "ts_code",
            "trade_time",
            "available_at",
            "open",
            "high",
            "low",
            "close",
            "vol",
            "amount",
        )
    ).to_parquet(payload_path, index=False)
    payload = payload_path.read_bytes()
    payload_path.unlink()
    real_inputs = profile_fixtures._inputs

    def with_minute_snapshot(path: Path) -> Any:
        inputs = real_inputs(path)
        snapshot = Path(inputs.historical_minutes_snapshot_path)
        snapshot.parent.mkdir(parents=True, exist_ok=True)
        snapshot.write_bytes(payload)
        snapshot.chmod(0o600)
        return inputs.model_copy(
            update={"historical_minutes_snapshot_id": hashlib.sha256(payload).hexdigest()}
        )

    monkeypatch.setattr(profile_fixtures, "_inputs", with_minute_snapshot)
    return payload


@pytest.fixture
def relocated_minute_snapshot(cold_chain: RouteAWorld, minute_snapshot: bytes) -> None:
    """The same bytes at the path the bundle install relocated the snapshot to."""

    manifest = next(
        manifest
        for manifest in cold_chain.profile.manifests
        if manifest.service_kind.value == "feature_live"
    )
    snapshot = Path(str(manifest.settings["historical_minutes_snapshot_path"]))
    snapshot.parent.mkdir(parents=True, exist_ok=True)
    snapshot.write_bytes(minute_snapshot)
    snapshot.chmod(0o600)


@pytest.fixture
def credentials_root(
    cold_chain: RouteAWorld,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, Path]:
    """One `/run/credentials/<unit>` per credstore role, laid out as systemd lays it out.

    Root-owned 0440 `capabilities.json` admitted to the service user through an ACL, inside
    a root-owned 0550 directory on systemd's own mount -- `systemd_credential_delivery`
    moves the three facts a non-root test cannot produce and nothing else (package E/I).
    """

    directories: dict[str, Path] = {}
    #: one root for all seven, because that is what `/run/credentials` is: the unit name is
    #: the directory inside it, and the seams `install_delivery` moves are per-root
    root = tmp_path / "credentials"
    for role in sorted(CREDSTORE_ROLES):
        for instance in instance_of(cold_chain, role):
            unit = ROLE_UNITS[role]
            name = (
                f"{unit[: -len('@.service')]}@{instance}.service"
                if unit.endswith("@.service")
                else unit
            )
            delivery = deliver(
                monkeypatch,
                root=root,
                unit=name,
                payload=cold_chain.sealed_credentials[instance],
                mode=0o440,
            )
            assert delivery.path.name == RUNTIME_CAPABILITY_CREDENTIAL_NAME
            directories[instance] = delivery.directory
    return directories


@pytest.fixture
def provisioned_recovery(cold_chain: RouteAWorld, monkeypatch: pytest.MonkeyPatch) -> None:
    """The two documents the recovery oneshots read, and a recorder for their payload.

    `provision` is the real producer (#218 C). The payload behind it needs a published
    backup generation under `/var/lib/rquant/runtime-recovery/backups`, which is another
    subsystem's acceptance, so it is recorded rather than replaced -- exactly the seam
    `test_route_a_recovery_binding_e2e` already uses. Everything this file measures --
    the binding, the config reads, and every write the pass makes -- is real.
    """

    import sys

    scripts = Path(__file__).resolve().parents[2] / "scripts"
    if str(scripts) not in sys.path:  # pragma: no cover - import bootstrap
        sys.path.insert(0, str(scripts))
    from provision_runtime_recovery_credentials import provision

    recovery = cold_chain.profile.recovery
    Path(recovery.credential_file).parent.mkdir(parents=True, exist_ok=True)
    Path(recovery.backup_config_path).parent.mkdir(parents=True, exist_ok=True)
    provision(
        cold_chain.runtime_root,
        as_of=datetime(2026, 9, 7, 8, 0, tzinfo=UTC),
        replay_start_date=FROZEN_NOW.date().replace(month=7, day=1),
        replay_end_date=FROZEN_NOW.date().replace(month=7, day=31),
    )
    monkeypatch.setattr(cli_module, "cmd_runtime_recovery", lambda _resolved: 0)


# ---------------------------------------------------------------------------------------
# The roster
# ---------------------------------------------------------------------------------------


def test_the_roster_is_every_instanced_role_and_each_one_names_a_unit() -> None:
    """25 roles, 25 units, and no role in the policy without a sandbox to run under."""

    instanced = {entry.name for entry in PRODUCTION_ROLE_POLICY if entry.instanced}

    assert instanced == set(ROLE_UNITS)
    assert len(ROLE_UNITS) == 25
    assert set(START_ORDER) == set(ROLE_UNITS)
    for role, unit in ROLE_UNITS.items():
        path = _UNIT_ROOT / unit
        assert path.is_file(), role
        text = path.read_text(encoding="utf-8")
        assert "ProtectSystem=strict" in text, role
        assert "ProtectHome=read-only" in text, role
        assert re.search(r"^ReadWritePaths=", text, flags=re.MULTILINE), role


def test_every_credstore_role_is_the_set_of_units_that_load_a_credential() -> None:
    """The credential list is derived from `deploy/systemd`, not remembered."""

    observed = {
        role
        for role, unit in ROLE_UNITS.items()
        if "LoadCredentialEncrypted=" in (_UNIT_ROOT / unit).read_text(encoding="utf-8")
    }

    assert observed == CREDSTORE_ROLES


# ---------------------------------------------------------------------------------------
# The acceptance
# ---------------------------------------------------------------------------------------


#: The roles this world cannot build, and the exact input each one is missing. None of
#: these is a sandbox failure -- every one reaches its builder and stops on a document the
#: production inputs fixture does not produce -- and each is recorded rather than skipped,
#: so a role that starts failing for a *different* reason fails this test.
CANNOT_BUILD: dict[str, str] = {
    "shadow_session": "completion attestation public key is not an Ed25519 key",
    "daily_pipeline_orchestrator": "daily pipeline storage binding was replaced",
    "artifact_retention": "No such file or directory",
}

#: Writes outside a unit that are known and not yet fixed, recorded exactly so a *new*
#: one fails this file. Empty is the intended state: `feature_live` was the last entry and
#: is fixed (`runtime_builder_feature.py`, #231's fifth site). The widening guard below no
#: longer depends on this being non-empty -- that was review S-3.
KNOWN_OUT_OF_SANDBOX: dict[str, str] = {}

#: Writes into another role's directory that only `tree_state` can see, because SQLite
#: makes them from C and the syscall wrappers never observe them (review S-4). Both are
#: the #227/#242 shape in a third artifact: **opening a WAL database read-only creates the
#: `-shm` wal-index next to it**, and these two readers do that in a producer's directory.
#: On a host that is `EROFS` the moment the producer's `-shm` is not already there.
#: Recorded, not fixed: the runner and broker stores are WAL because their owners need
#: WAL, so the fix is a journal-mode decision for two more authorities and belongs with
#: its own acceptance -- the report carries the argument. A *new* entry fails this file.
#: Only the first of these is a write by the role named. The per-role `tree_state` diffs
#: separate them:
#:
#: * `signal_router` -- `created=[runner.sqlite3-shm, runner.sqlite3-wal] x3`. A real
#:   write into a directory its unit grants read-only: it opens each strategy's WAL
#:   `runner.sqlite3` and SQLite creates the wal-index beside it. **This is the entry the
#:   runbook's `-shm` start-order rule is about, and it is about `signal_router` alone.**
#: * `notifier` -- `created=[] removed=[broker.sqlite3-shm, broker.sqlite3-wal]
#:   modified=[broker.sqlite3]`, and `reference_slow_publisher` -- the same shape over
#:   `live/reference-slow/quota.sqlite3`. **Harness artifact, not a write by this role**:
#:   an earlier role in the *same test process* left a SQLite connection open (see the
#:   `SourceQuotaStore._connect` note in the package report), and the interpreter collected
#:   and checkpointed it inside this role's measurement window, which deletes the sidecars.
#:   `reference_slow_publisher` has no quota path in its manifest at all and never touches
#:   that directory; on the host it has published several generations without a write grant
#:   there. Under systemd each role is its own process, so neither can happen. #245 closed
#:   the quota-ledger half of this: `SourceQuotaStore` now closes each operation's
#:   connection, so the reference-slow sidecars go when the operation does rather than
#:   whenever the collector runs. The containment below is unchanged -- the paper broker
#:   still holds its connection, and the entry is allowed, not required.
#:
#: All three stay as tripwires -- a *new* role appearing here is a regression worth
#: reading -- but the assertion is a containment, because whether a peer's sidecars are
#: already on disk depends on when the producer in the same pass closed its connection
#: (`reference_slow_publisher` showed up on Linux and not on macOS for exactly that
#: reason). The one entry that must never be absent is asserted on its own.
KNOWN_C_LEVEL_WRITES: dict[str, tuple[str, ...]] = {
    #: a real out-of-sandbox write
    "signal_router": ("live/strategies",),
    #: harness artifacts: a same-process producer connection closed during the window
    "notifier": ("live/paper-brokers",),
    "reference_slow_publisher": ("live/reference-slow",),
}


def test_every_role_starts_under_its_own_unit_and_writes_nowhere_else(
    minute_snapshot: bytes,
    cold_chain: RouteAWorld,
    relocated_minute_snapshot: None,
    credentials_root: dict[str, Path],
    provisioned_recovery: None,
) -> None:
    """All 24 startable roles, each once, each inside its own unit's path set.

    `page_control` is the twenty-fifth and is asserted separately: its entry point
    resolves the runtime root from a frozen constant rather than from the argv the
    wrapper derives, so there is no argv this harness can hand it.
    """

    runs: list[RoleRun] = []
    for role in START_ORDER:
        if role == "page_control":
            continue
        for instance in instance_of(cold_chain, role):
            runs.append(
                run_role(
                    cold_chain,
                    role,
                    instance=instance,
                    credentials=credentials_root.get(instance),
                )
            )

    outside = {
        run.role: [violation.path for violation in run.violations]
        for run in runs
        if run.violations
    }
    assert set(outside) == set(KNOWN_OUT_OF_SANDBOX), outside
    for role, expected in KNOWN_OUT_OF_SANDBOX.items():
        assert len(outside[role]) == 1, outside[role]
        assert outside[role][0].endswith(expected), outside[role]
    #: the end-state check the syscall wrappers cannot make: no directory a role does not
    #: own changed while it ran, including through writes made from C
    touched = {run.role: run.changed_trees for run in runs if run.changed_trees}
    assert set(touched) <= set(KNOWN_C_LEVEL_WRITES), touched
    for role, subtrees in touched.items():
        assert subtrees == KNOWN_C_LEVEL_WRITES[role], (role, subtrees)
    assert "signal_router" in touched, touched

    refused = {run.role: repr(run.refusal) for run in runs if run.refusal is not None}
    assert set(refused) == set(CANNOT_BUILD), refused
    for role, reason in CANNOT_BUILD.items():
        assert reason in refused[role], refused[role]

    stalled = {run.role for run in runs if not run.entered}
    assert stalled == set(CANNOT_BUILD), stalled
    assert {run.role for run in runs} == set(ROLE_UNITS) - {"page_control"}
    #: and everything else did reach its loop, inside the paths its own unit grants
    assert len([run for run in runs if run.entered]) == 25


def test_the_page_control_entry_point_cannot_take_a_runtime_root_from_its_argv() -> None:
    """Why the twenty-fifth role is not started here, measured rather than asserted.

    `main(argv)` parses `--manifest`, `--control-root`, `--expected-commit` and
    `--expected-generation` and then calls `_serve(runtime_root=None)`, which falls back
    to `LINUX_PRODUCTION_RUNTIME_ROOT`. The `runtime_root=` keyword exists for tests and
    is not on the wrapper's path, so a sandboxed start from the wrapper's own argv is
    only possible on a host that owns `/home/lighthouse/rquant/data/runtime`.
    """

    import inspect

    import rquant.page_control_service as page_control

    source = inspect.getsource(page_control.main)

    assert "return _serve(runtime_root=runtime_root, expected_commit=expected_commit)" in source
    assert "runtime_root" not in {action.dest for action in page_control.build_parser()._actions}
    serve = inspect.getsource(page_control._serve)
    assert "resolved_runtime_root = runtime_root or LINUX_PRODUCTION_RUNTIME_ROOT" in serve


def test_the_paper_constraint_publisher_starts_over_a_really_read_only_authority(
    cold_chain: RouteAWorld,
) -> None:
    """#242 at the layer the syscall sandbox cannot reach: SQLite's own `open`.

    `readonly_runtime` denies writes at the Python wrappers, and SQLite creates its
    wal-index from C, so only a directory the kernel refuses can prove this one. Taking
    the write bits off `authorities/reference-slow` is that directory: the reader has to
    get through it without creating anything, which a WAL registry cannot do.
    """

    if os.geteuid() == 0:
        pytest.skip("root ignores the directory mode this case denies")
    role = "paper_constraint_publisher"
    instance = instance_of(cold_chain, role)[0]
    authority = cold_chain.runtime_root / "authorities" / "reference-slow"
    registry = authority / "reference.sqlite3"
    assert registry.is_file()
    #: byte 18 of the header: 1 is a rollback journal, 2 is WAL
    assert registry.read_bytes()[18] == 1
    os.chmod(authority, 0o500)
    try:
        run = run_role(cold_chain, role, instance=instance)
    finally:
        os.chmod(authority, 0o700)

    assert run.refusal is None, run
    assert run.entered, run
    assert run.violations == [], run.violations
    assert sorted(path.name for path in authority.iterdir()) == [
        ".reference.sqlite3.publication.lock",
        "reference.sqlite3",
    ]


def test_a_wal_reference_registry_stops_the_paper_constraint_publisher_by_name(
    cold_chain: RouteAWorld,
) -> None:
    """The negative: the state the host was in, refused with the path in the message."""

    import sqlite3
    from contextlib import closing

    role = "paper_constraint_publisher"
    instance = instance_of(cold_chain, role)[0]
    registry = cold_chain.runtime_root / "authorities" / "reference-slow" / "reference.sqlite3"
    with closing(sqlite3.connect(registry, isolation_level=None)) as connection:
        assert connection.execute("PRAGMA journal_mode = WAL").fetchone()[0] == "wal"
    for suffix in ("-wal", "-shm"):
        sidecar = registry.with_name(registry.name + suffix)
        if sidecar.exists():
            sidecar.unlink()

    run = run_role(cold_chain, role, instance=instance)

    assert not run.entered
    assert run.violations == [], run.violations
    message = str(run.refusal)
    assert str(registry) in message, message
    assert "WAL" in message, message
    assert "reference.sqlite3-shm" in message, message
    assert "reference_slow_publisher" in message, message


def test_a_corrupt_reference_registry_stops_the_paper_constraint_publisher_by_name(
    cold_chain: RouteAWorld,
) -> None:
    """The other negative: an unreadable registry refuses, and the refusal names the file.

    This is the host's own line -- `ReferenceDataIntegrityError("reference registry is
    invalid")`, with no path, no directory and no errno in it -- turned into one that says
    which file, what SQLite said about it, and whether the directory refuses new entries.
    """

    role = "paper_constraint_publisher"
    instance = instance_of(cold_chain, role)[0]
    registry = cold_chain.runtime_root / "authorities" / "reference-slow" / "reference.sqlite3"
    with open(registry, "r+b") as handle:
        handle.seek(100)
        handle.write(b"\x00" * 1024)

    run = run_role(cold_chain, role, instance=instance)

    assert not run.entered
    assert run.violations == [], run.violations
    message = str(run.refusal)
    assert str(registry) in message, message
    assert "DatabaseError" in message or "OperationalError" in message, message


def test_the_notifier_writes_nothing_in_the_page_control_root(
    cold_chain: RouteAWorld,
    credentials_root: dict[str, Path],
) -> None:
    """#241 over the real bundle: the outbox's directory is not the notifier's to write."""

    #: the notifier opens the routed-signal spool the router creates, so the router runs
    #: first, exactly as the runbook's C-3 order has it
    for strategy in instance_of(cold_chain, "strategy_live"):
        run_role(cold_chain, "strategy_live", instance=strategy)
    run_role(cold_chain, "signal_router", instance=instance_of(cold_chain, "signal_router")[0])

    role = "notifier"
    instance = instance_of(cold_chain, role)[0]
    run = run_role(
        cold_chain,
        role,
        instance=instance,
        credentials=credentials_root[instance],
    )

    assert run.refusal is None, run
    assert run.entered, run
    assert run.violations == [], run.violations
    control = cold_chain.runtime_root / "control"
    assert [path.name for path in control.glob(".page-control.sqlite3.*")] == []


def test_a_role_given_the_whole_runtime_root_is_not_what_this_file_measures(
    cold_chain: RouteAWorld,
) -> None:
    """The guard on the harness itself, with a write of its own to prove it.

    Review S-3: the first version of this guard only asserted "the granted set is not the
    whole root" and "the widened run has no violations", both of which stay true if
    `run_role` is widened. It was the unfixed `feature_live` write that made the widening
    mutation red -- so the day that write is fixed, the widening would have gone unnoticed.

    This makes the guard self-sufficient: a synthetic write into a directory no unit grants
    is refused under the role's own path set and admitted under the widened one. It needs
    no role to misbehave, so it keeps working when `KNOWN_OUT_OF_SANDBOX` is empty.
    """

    role = "feature_live"
    instance = instance_of(cold_chain, role)[0]
    granted = sandbox_of(role, instance=instance, runtime_root=cold_chain.runtime_root)
    assert granted["ReadWritePaths"] != (cold_chain.runtime_root,)
    assert all(
        path != cold_chain.runtime_root for path in granted["ReadWritePaths"]
    ), granted["ReadWritePaths"]

    #: `authorities/` is granted to no role's `ReadWritePaths` under this instance
    probe = cold_chain.runtime_root / "authorities" / "pkgl-sandbox-probe"

    with readonly_runtime(
        cold_chain.runtime_root,
        writable=granted["ReadWritePaths"],
    ) as narrow_violations, pytest.raises(OSError) as refused:
        probe.mkdir()
    assert refused.value.errno == errno.EROFS
    assert [violation.path for violation in narrow_violations] == [str(probe)]
    assert not probe.exists()

    with readonly_runtime(
        cold_chain.runtime_root,
        writable=(cold_chain.runtime_root,),
    ) as wide_violations:
        probe.mkdir()
    assert wide_violations == []
    assert probe.is_dir()
    probe.rmdir()


#: the two units that declare no `InaccessiblePaths` at all. `ProtectHome=read-only` still
#: makes `/home/lighthouse/rquant/.env` unwritable for them, but not unreadable, which the
#: other 23 units do make it. Recorded as it is, not corrected: `deploy/systemd` is the
#: owner's to change, and this pins the state so the next change is noticed.
UNITS_WITHOUT_INACCESSIBLE_PATHS = frozenset({"runtime_recovery", "runtime_recovery_rehearsal"})


def test_the_units_that_hide_the_dotenv_and_the_generation_secrets_are_23_of_25() -> None:
    """`InaccessiblePaths` is a denial the read-only default does not make on its own."""

    hides_dotenv: set[str] = set()
    hides_secrets: set[str] = set()
    for role, unit in ROLE_UNITS.items():
        text = (_UNIT_ROOT / unit).read_text(encoding="utf-8")
        declared = " ".join(re.findall(r"^InaccessiblePaths=(.*)$", text, flags=re.MULTILINE))
        if "/home/lighthouse/rquant/.env" in declared:
            hides_dotenv.add(role)
        if "data/runtime/current/secrets" in declared:
            hides_secrets.add(role)

    assert hides_dotenv == set(ROLE_UNITS) - UNITS_WITHOUT_INACCESSIBLE_PATHS
    assert hides_secrets == set(ROLE_UNITS) - UNITS_WITHOUT_INACCESSIBLE_PATHS
    for role in UNITS_WITHOUT_INACCESSIBLE_PATHS:
        text = (_UNIT_ROOT / ROLE_UNITS[role]).read_text(encoding="utf-8")
        assert not re.search(r"^InaccessiblePaths=", text, flags=re.MULTILINE), role
        assert "ProtectHome=read-only" in text, role


def test_the_inaccessible_paths_a_unit_declares_really_deny_a_read(
    cold_chain: RouteAWorld,
    tmp_path: Path,
) -> None:
    """The sandbox's third denial, exercised on its own.

    No role in this world reads `.env` or `current/secrets`, so the `InaccessiblePaths`
    list the runs above install protects nothing observable -- a sandbox clause with no
    case behind it is a clause that can be deleted without a test noticing. This is that
    case: the notifier's own list, and a read of one of the paths in it.
    """

    instance = instance_of(cold_chain, "notifier")[0]
    sandbox = sandbox_of("notifier", instance=instance, runtime_root=cold_chain.runtime_root)
    hidden = sandbox["InaccessiblePaths"]
    assert hidden, sandbox
    secrets = next(path for path in hidden if path.name == "secrets")
    secrets.parent.mkdir(parents=True, exist_ok=True)
    secrets.mkdir(exist_ok=True)
    (secrets / "sealed.json").write_text("{}", encoding="utf-8")

    with readonly_runtime(
        cold_chain.runtime_root,
        writable=sandbox["ReadWritePaths"],
        inaccessible=hidden,
    ) as violations:
        with pytest.raises(OSError) as raised:
            (secrets / "sealed.json").read_text(encoding="utf-8")
        #: and a path outside the list is still readable
        assert (cold_chain.runtime_root / "current").exists()

    assert raised.value.errno == errno.EACCES
    assert [violation.path for violation in violations] == [str(secrets / "sealed.json")]


def _role_evidence(runs: list[RoleRun]) -> str:  # pragma: no cover - report helper
    lines = ["role | instance | entered | code | status | out-of-sandbox writes | last_error"]
    for run in runs:
        lines.append(
            f"{run.role} | {run.instance[:12]}… | {run.entered} | {run.exit_code} | "
            f"{run.status} | {len(run.violations)} | "
            f"{run.last_error or run.refusal!r} | {run.violations}"
        )
    return "\n".join(lines)


def test_the_evidence_table_is_produced_for_the_report(
    minute_snapshot: bytes,
    cold_chain: RouteAWorld,
    relocated_minute_snapshot: None,
    credentials_root: dict[str, Path],
    provisioned_recovery: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """One pass over the roster that prints what each role did, for the package report."""

    runs = [
        run_role(
            cold_chain,
            role,
            instance=instance,
            credentials=credentials_root.get(instance),
        )
        for role in START_ORDER
        if role != "page_control"
        for instance in instance_of(cold_chain, role)
    ]
    with capsys.disabled():
        print("\n" + _role_evidence(runs))

    assert len(runs) == 28
    assert {run.role for run in runs if run.violations} == set(KNOWN_OUT_OF_SANDBOX)
    assert {run.role for run in runs if not run.entered} == set(CANNOT_BUILD)


def test_the_recovery_oneshots_reach_their_payload_inside_their_sandbox(
    cold_chain: RouteAWorld,
    provisioned_recovery: None,
) -> None:
    """Both recovery units, whose only writes are under `control/recovery/<svc>`."""

    for role in ("runtime_recovery", "runtime_recovery_rehearsal"):
        instance = instance_of(cold_chain, role)[0]
        run = run_role(cold_chain, role, instance=instance)
        assert run.refusal is None, run
        assert run.exit_code == 0, run
        assert run.violations == [], run.violations
        #: the one directory these two units may write, and the only one they touched
        assert run.changed_trees == (), run.changed_trees
