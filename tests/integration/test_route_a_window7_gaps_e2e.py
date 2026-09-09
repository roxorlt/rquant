"""#254 #253 #252 #255 acceptance: the four gaps the seventh Route A window opened.

The 2026-09-09 window installed bundle `1aebc325…` over `20d948d1…` (v0.33.5, authority
sequence 5) at 09:2x-09:4x, inside market hours, and four things that had nothing to do
with each other failed on the same day:

* `market-minute.source.v1` and `watchlist-quote.source.v1` DEGRADED every iteration with
  `snapshot authority is damaged: strategy candidate snapshot lock is missing or unsafe`,
  which stopped the whole live chain behind them (#254);
* `serving.publisher.v1` DEGRADED every iteration with `current pointer producer_commit
  does not match expected commit` (#253);
* two of the three `rquant-runtime-strategy@` units, started before the paper broker,
  exited 1 at construction on the broker's WAL ledger and pushed three real alerts (#252);
* `notifier.admin.shadow.v1` DEGRADED every iteration because the host's DuckDB refuses
  `/proc/self/fd/<n>` and the branch that ran instead hard-linked beside the database,
  into a directory its unit mounts read-only (#255).

This file is that window in package L's two-generation install world: the state each
failure needed is written by the same writers the roles use, and then the roles are
started through the wrapper's own argv inside each unit's own sandbox. The negative half
carries the same weight -- a damaged store, a foreign commit and a corrupt ledger are all
still refused.
"""

from __future__ import annotations

import gc
import sqlite3
import stat
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from rquant.feature_spool import FeatureBatchSpool
from rquant.paper_broker import PaperBrokerStore
from rquant.runtime_builder_paper import PaperBrokerSettings
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_service_entrypoint import RuntimeServiceKind
from rquant.runtime_serving_authority import ServingSourceAuthorityPublisher
from rquant.runtime_serving_snapshot import SignalDeliveryPayload, SourceReadResult
from rquant.serving_contracts import FreshnessStatus
from rquant.strategy_candidate_snapshot import StrategyCandidateSnapshotSpool
from tests.integration.test_route_a_all_roles_sandbox_e2e import (
    cold_chain,  # noqa: F401 -- the two-generation fixture, reused verbatim
    credentials_root,  # noqa: F401 -- three roles here carry a `LoadCredentialEncrypted=`
    instance_of,
    minute_snapshot,  # noqa: F401 -- required by `relocated_minute_snapshot`
    relocated_minute_snapshot,  # noqa: F401 -- the parquet `market_minute_source` opens
    run_role,
)
from tests.integration.test_route_a_generation_handover_e2e import (
    first_generation_manifest,
    manifests_of,
)
from tests.integration.test_route_a_legacy_binding_e2e import RouteAWorld
from tests.integration.test_route_a_live_chain_idle_e2e import FROZEN_NOW

pytestmark = pytest.mark.integration

CANDIDATE_ROLE = "candidate_publisher"
MINUTE_ROLE = "market_minute_source"
QUOTE_ROLE = "watchlist_quote_source"
SERVING_ROLE = "serving_publisher"
STRATEGY_ROLE = "strategy_live"
BROKER_ROLE = "paper_broker"
NOTIFIER_ROLE = "notifier"
TRADE_DATE = date(2026, 8, 3)
LOCK_REFUSAL = "snapshot lock is missing or unsafe"


# ---------------------------------------------------------------------------------------
# #254: the candidate root two source roles read, as the host had it
# ---------------------------------------------------------------------------------------


SESSION_NOW = datetime(2026, 8, 3, 1, 45, tzinfo=UTC)
"""09:45 Shanghai on the one date the bundle's calendar opens.

The window failed at 09:4x, and that matters: `market_minute_source` and
`watchlist_quote_source` only read the candidate stores while the session allows a fetch
(`decide_market_session`), so at the idle clock the rest of this world runs at they never
touch them at all. This is the instant at which they do.
"""


@pytest.fixture
def in_session(monkeypatch: pytest.MonkeyPatch) -> None:
    """Run the roles at `SESSION_NOW` instead of the world's idle clock."""

    import tests.integration.test_route_a_all_roles_sandbox_e2e as harness

    monkeypatch.setattr(harness, "FROZEN_NOW", SESSION_NOW)


def published_candidate_roots(route: RouteAWorld) -> tuple[Path, ...]:
    """Every candidate root with one bound generation of this generation's own publisher.

    Written by calling the writer the publisher calls, with the fingerprints out of the
    installed manifest, which is the seam package N's handover file states and keeps: what
    lands on disk is byte-for-byte what the role writes.
    """

    roots: list[Path] = []
    for manifest in manifests_of(route, RuntimeServiceKind.CANDIDATE_PUBLISHER):
        root = Path(str(manifest.settings["snapshot_root"]))
        StrategyCandidateSnapshotSpool(root).publish_strategy_records(
            strategy_id=str(manifest.settings["strategy_id"]),
            strategy_version="1",
            definition_fingerprint=str(manifest.settings["definition_fingerprint"]),
            executable_fingerprint=str(manifest.settings["executable_fingerprint"]),
            candidate_schema_fingerprint=str(manifest.settings["candidate_schema_fingerprint"]),
            static_feature_schema=dict(manifest.settings["static_feature_schema"]),
            source_snapshot_ids={"candidate_input": "1" * 64},
            trade_date=TRADE_DATE,
            captured_at=SESSION_NOW - timedelta(minutes=30),
            producer_commit=manifest.producer_commit,
            rows=(),
        )
        roots.append(root)
    assert roots
    return tuple(roots)


def without_their_locks(roots: tuple[Path, ...]) -> None:
    """The host's state: a candidate root with content and no `.publish.lock` beside it.

    `install_runtime_deployment_bundle` creates `live/candidates/<instance>/` at 0700 for
    every candidate publisher (`_ensure_owned_descendant`, the CANDIDATE_PUBLISHER branch)
    and puts nothing in it; until #254 the lock was created by a *publish* and by nothing
    else, so a publisher that has not published leaves every reader of its store a root
    they have to call damaged.
    """

    for root in roots:
        (root / ".publish.lock").unlink()


def source_runs(
    route: RouteAWorld,
    credentials: dict[str, Path],
) -> tuple[Any, ...]:
    """One pass of each of the two source roles that read the candidate stores."""

    runs = []
    for role in (MINUTE_ROLE, QUOTE_ROLE):
        instance = instance_of(route, role)[0]
        runs.append(
            run_role(route, role, instance=instance, credentials=credentials.get(instance))
        )
    return tuple(runs)


def test_the_candidate_root_without_its_lock_is_what_stopped_the_two_source_roles(
    cold_chain: RouteAWorld,  # noqa: F811
    credentials_root: dict[str, Path],  # noqa: F811
    relocated_minute_snapshot: None,  # noqa: F811
    in_session: None,
) -> None:
    """The premise, measured: this is the state, and this is the message it produced."""

    roots = published_candidate_roots(cold_chain)
    without_their_locks(roots)

    for run in source_runs(cold_chain, credentials_root):
        assert run.entered, run
        assert LOCK_REFUSAL in (run.last_error or ""), run


def test_the_owner_creates_the_lock_at_build_and_both_source_roles_read_again(
    cold_chain: RouteAWorld,  # noqa: F811
    credentials_root: dict[str, Path],  # noqa: F811
    relocated_minute_snapshot: None,  # noqa: F811
    in_session: None,
) -> None:
    """#254: the publisher's build is where the file every reader waits on comes from.

    The reader's requirement never moved -- `_locked` and `read_strategy_as_of` are
    byte-identical between v0.33.4 and v0.33.5. What moved is that the only build-time
    initializer package N added returns before creating the lock for an unbound root, so a
    publisher that had not managed a batch left its readers a root they had to refuse.
    """

    roots = published_candidate_roots(cold_chain)
    without_their_locks(roots)

    for instance in instance_of(cold_chain, CANDIDATE_ROLE):
        run_role(cold_chain, CANDIDATE_ROLE, instance=instance)
    for root in roots:
        lock = root / ".publish.lock"
        assert lock.is_file()
        observed = lock.lstat()
        assert stat.S_IMODE(observed.st_mode) == 0o600
        assert observed.st_nlink == 1

    #: the candidate universe is read, and what each role then does with it is its own
    #: business: the fixture publishes an empty generation, so the minute source has no
    #: codes to fetch. What must be gone is every trace of the store being refused.
    for run in source_runs(cold_chain, credentials_root):
        assert run.entered, run
        error = run.last_error or ""
        assert LOCK_REFUSAL not in error, run
        assert "snapshot authority" not in error, run
        assert "candidate universe is empty" not in error, run
        assert run.violations == [], run.violations


def test_a_damaged_lock_still_stops_the_publisher_and_its_readers(
    cold_chain: RouteAWorld,  # noqa: F811
    credentials_root: dict[str, Path],  # noqa: F811
    relocated_minute_snapshot: None,  # noqa: F811
    in_session: None,
) -> None:
    """The negative half: creating the lock is not accepting whatever sits at its name."""

    roots = published_candidate_roots(cold_chain)
    for root in roots:
        (root / ".publish.lock").chmod(0o666)

    for instance in instance_of(cold_chain, CANDIDATE_ROLE):
        run = run_role(cold_chain, CANDIDATE_ROLE, instance=instance)
        assert not run.entered, run
        assert "private regular file" in str(run.refusal), run

    for run in source_runs(cold_chain, credentials_root):
        assert "snapshot authority is damaged" in (run.last_error or ""), run


# ---------------------------------------------------------------------------------------
# #253: the signals authority pointer the previous generation left behind
# ---------------------------------------------------------------------------------------


def _signals_result() -> SourceReadResult:
    values: dict[str, object] = {
        "dataset_id": "signals",
        "sequence": 1,
        "event_time": FROZEN_NOW,
        "published_at": FROZEN_NOW,
        "status": FreshnessStatus.FRESH,
        "reason": None,
        "payload": SignalDeliveryPayload(),
    }
    values["generation_id"] = canonical_sha256(values)
    return SourceReadResult.model_validate(values)


def write_first_generation_signals_pointer(route: RouteAWorld) -> tuple[Path, str]:
    """`current.json` exactly as the previous generation's notifier published it."""

    notifier = manifests_of(route, RuntimeServiceKind.NOTIFIER)[0]
    previous = first_generation_manifest(route, notifier.service_id)
    assert previous.producer_commit != notifier.producer_commit
    root = Path(str(notifier.settings["serving_authority_root"]))
    ServingSourceAuthorityPublisher(
        root=root,
        producer_commit=previous.producer_commit,
        dataset_id="signals",
        payload_kind="signal_delivery",
        clock=lambda: FROZEN_NOW,
    ).publish(_signals_result())
    return root, previous.producer_commit


def test_the_serving_publisher_carries_the_previous_generations_signals_pointer(
    cold_chain: RouteAWorld,  # noqa: F811
) -> None:
    """#253: the fifth cross-generation shape, in the world that produces it.

    The signals authority belongs to the notifier. After a release the notifier has not
    published again yet, so its `current.json` still names the commit of the generation
    before this one -- and the serving publisher, which only reads it, refused every
    iteration. It cannot rewrite the pointer either: `rquant-runtime-serving@.service`
    mounts `control/` and `live/notifications/` read-only for it. So it carries the
    pointer, and the owner replaces it on its own next publish.
    """

    root, previous_commit = write_first_generation_signals_pointer(cold_chain)
    assert previous_commit in (root / "current.json").read_text()

    run = run_role(cold_chain, SERVING_ROLE, instance=instance_of(cold_chain, SERVING_ROLE)[0])

    assert run.entered, run
    assert "producer_commit does not match expected commit" not in (run.last_error or ""), run
    assert run.violations == [], run.violations


def test_a_signals_pointer_from_no_generation_of_ours_still_stops_serving(
    cold_chain: RouteAWorld,  # noqa: F811
) -> None:
    """The negative half: only our own past is carried, and only over its own document."""

    notifier = manifests_of(cold_chain, RuntimeServiceKind.NOTIFIER)[0]
    root = Path(str(notifier.settings["serving_authority_root"]))
    ServingSourceAuthorityPublisher(
        root=root,
        producer_commit="e" * 40,
        dataset_id="signals",
        payload_kind="signal_delivery",
        clock=lambda: FROZEN_NOW,
    ).publish(_signals_result())

    run = run_role(cold_chain, SERVING_ROLE, instance=instance_of(cold_chain, SERVING_ROLE)[0])

    assert run.entered, run
    assert "producer_commit does not match expected commit" in (run.last_error or ""), run


# ---------------------------------------------------------------------------------------
# #252: the strategies started before the paper broker
# ---------------------------------------------------------------------------------------


def broker_ledger(route: RouteAWorld) -> Path:
    return Path(
        str(manifests_of(route, RuntimeServiceKind.STRATEGY_LIVE)[0].settings["paper_broker_path"])
    )


def stopped_broker_ledger(route: RouteAWorld) -> Path:
    """The ledger its owner created, left the way a clean stop leaves it: no `-wal`, no `-shm`.

    The feature spool is created here too, because it is the peer a strategy waits on
    first on an idle world (`test_a_strategy_waits_by_name_while_the_feature_role_has_
    published_nothing`) and this file is about the one behind it. `install_runtime_
    deployment_bundle` creates `live/features` and the feature role's own builder turns it
    into a spool; the idle-chain fixture does exactly this and the strategies then succeed.
    """

    FeatureBatchSpool(
        Path(
            str(
                manifests_of(route, RuntimeServiceKind.STRATEGY_LIVE)[0].settings[
                    "feature_spool_root"
                ]
            )
        )
    )

    manifest = manifests_of(route, RuntimeServiceKind.PAPER_BROKER)[0]
    settings = PaperBrokerSettings.model_validate(dict(manifest.settings))
    #: the broker's own writer, with the broker's own manifest settings: what the role
    #: creates on its first start, which on the host had already happened
    PaperBrokerStore(
        settings.broker_path,
        account_id=settings.account_id,
        initial_cash=settings.initial_cash,
        cost_policy=settings.cost_policy(),
    )
    ledger = broker_ledger(route)
    assert ledger.is_file()
    gc.collect()
    connection = sqlite3.connect(ledger)
    try:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    finally:
        connection.close()
    gc.collect()
    assert not ledger.with_name(f"{ledger.name}-wal").exists()
    assert not ledger.with_name(f"{ledger.name}-shm").exists()
    return ledger


def test_the_strategies_start_before_the_broker_and_reach_their_loop(
    cold_chain: RouteAWorld,  # noqa: F811
) -> None:
    """#252: this exact order cost three real pushes, and it is the order after a reboot.

    `live/paper-brokers/` is read-only for a strategy, so a read-only open of the WAL
    ledger cannot create the `-shm` it needs and SQLite answers `unable to open database
    file` -- from the first statement, not from `connect()`. The ledger is not damaged:
    its owner is not running, which is a peer to wait for. The wait is invisible on an
    idle world, and that is the point: the strategy's step never needs the ledger until a
    signal does, so what the probe used to do was take the whole unit down for a file it
    was not going to read.
    """

    ledger = stopped_broker_ledger(cold_chain)
    before = sorted(path.name for path in ledger.parent.iterdir())
    ledger.parent.chmod(0o500)
    try:
        for instance in instance_of(cold_chain, STRATEGY_ROLE):
            run = run_role(cold_chain, STRATEGY_ROLE, instance=instance)
            assert run.entered, run
            assert run.refusal is None, run.traceback
            assert run.last_error is None, run
            assert run.violations == [], run.violations
    finally:
        ledger.parent.chmod(0o700)
    #: and it really was dormant throughout: no strategy created the wal-index beside it
    assert sorted(path.name for path in ledger.parent.iterdir()) == before


def test_the_ledger_is_deferred_by_name_the_way_every_other_peer_is(
    cold_chain: RouteAWorld,  # noqa: F811
) -> None:
    """What the heartbeat would say once a signal makes the step reach for the ledger."""

    from rquant.runtime_peer_artifacts import DeferredPeerArtifact, PeerArtifactUnavailableError
    from rquant.strategy_paper_lifecycle import PaperBrokerLifecycleReader

    ledger = stopped_broker_ledger(cold_chain)
    settings = PaperBrokerSettings.model_validate(
        dict(manifests_of(cold_chain, RuntimeServiceKind.PAPER_BROKER)[0].settings)
    )
    deferred: DeferredPeerArtifact[PaperBrokerLifecycleReader] = DeferredPeerArtifact(
        reader="strategy_live",
        artifact="paper broker ledger",
        path=ledger,
        open_artifact=lambda: PaperBrokerLifecycleReader(
            ledger,
            account_id=settings.account_id,
        ),
    )
    ledger.parent.chmod(0o500)
    try:
        assert deferred.probe() is None
        with pytest.raises(PeerArtifactUnavailableError) as raised:
            deferred.get()
    finally:
        ledger.parent.chmod(0o700)
    #: `waiting_for` in the heartbeat is `str(error.path)` (`runtime_service_control`)
    assert raised.value.path == ledger
    assert "wal" in str(raised.value).lower()


def test_the_same_strategies_run_once_the_broker_is_up(
    cold_chain: RouteAWorld,  # noqa: F811
) -> None:
    """And with the broker's connection open the sidecars are there and nothing defers."""

    from rquant.runtime_peer_artifacts import PeerArtifactUnavailableError
    from rquant.strategy_paper_lifecycle import PaperBrokerLifecycleReader

    ledger = stopped_broker_ledger(cold_chain)
    settings = PaperBrokerSettings.model_validate(
        dict(manifests_of(cold_chain, RuntimeServiceKind.PAPER_BROKER)[0].settings)
    )
    #: the paper broker starting: its own store keeps `-wal`/`-shm` beside the ledger
    broker = PaperBrokerStore(
        settings.broker_path,
        account_id=settings.account_id,
        initial_cash=settings.initial_cash,
        cost_policy=settings.cost_policy(),
    )
    assert ledger.with_name(f"{ledger.name}-shm").exists()

    ledger.parent.chmod(0o500)
    try:
        reader = PaperBrokerLifecycleReader(ledger, account_id=settings.account_id)
        assert reader.path == ledger
        for instance in instance_of(cold_chain, STRATEGY_ROLE):
            run = run_role(cold_chain, STRATEGY_ROLE, instance=instance)
            assert run.entered, run
            assert run.last_error is None, run
    except PeerArtifactUnavailableError as pending:  # pragma: no cover - diagnostics only
        raise AssertionError(f"a running broker must not defer: {pending}") from pending
    finally:
        ledger.parent.chmod(0o700)
        del broker


def test_a_corrupt_broker_ledger_still_stops_every_strategy(
    cold_chain: RouteAWorld,  # noqa: F811
) -> None:
    """The negative half: a ledger whose header is not SQLite's is a fault, not a wait."""

    ledger = stopped_broker_ledger(cold_chain)
    payload = bytearray(ledger.read_bytes())
    payload[:16] = b"NotSQLite fmt 3\x00"
    ledger.write_bytes(bytes(payload))
    ledger.parent.chmod(0o500)
    try:
        run = run_role(
            cold_chain,
            STRATEGY_ROLE,
            instance=instance_of(cold_chain, STRATEGY_ROLE)[0],
        )
    finally:
        ledger.parent.chmod(0o700)
    assert not run.entered, run
    assert run.waiting_for is None, run


# ---------------------------------------------------------------------------------------
# #255: the notifier's replica reader on an engine that refuses the descriptor
# ---------------------------------------------------------------------------------------


def test_the_notifier_never_writes_beside_the_projection_database(
    cold_chain: RouteAWorld,  # noqa: F811
    credentials_root: dict[str, Path],  # noqa: F811
) -> None:
    """#255: the branch that ran when the descriptor was refused wrote where it may not.

    Package L kept a hard link beside the database for engines that refuse
    `/proc/self/fd/<n>`, believing only macOS does. The build on the production host
    refuses it too, `data/` is read-only for this unit, and the link failed with
    `errno 30 EROFS` on every iteration. The reader now copies into
    `live/notifications/%i` -- the one directory this unit may write -- or, for a
    generation too large to copy, reads in place and checks the identity afterwards.
    """

    from tests.runtime_readonly_sandbox import tree_state

    notifier = manifests_of(cold_chain, RuntimeServiceKind.NOTIFIER)[0]
    database = Path(str(notifier.settings["page_projection_database_path"]))
    database.parent.mkdir(parents=True, exist_ok=True)
    import duckdb

    with duckdb.connect(str(database)) as connection:
        connection.execute("CREATE TABLE probe (a INTEGER)")
    database.chmod(0o600)
    before = tree_state(database.parent)

    #: the notifier opens the routed-signal spool the router creates, so the runbook's
    #: C-3 order runs first, exactly as the all-roles file does it
    for strategy in instance_of(cold_chain, STRATEGY_ROLE):
        run_role(cold_chain, STRATEGY_ROLE, instance=strategy)
    run_role(cold_chain, "signal_router", instance=instance_of(cold_chain, "signal_router")[0])

    instance = instance_of(cold_chain, NOTIFIER_ROLE)[0]
    run = run_role(
        cold_chain,
        NOTIFIER_ROLE,
        instance=instance,
        credentials=credentials_root[instance],
    )

    assert run.entered, run
    assert run.violations == [], run.violations
    #: the reader opened the generation and got as far as the schema audit, which is the
    #: proof the pinning path worked: this fixture's database is a real DuckDB with one
    #: unrelated table, so `_require_tables` is the *next* thing that can fail.
    assert "cannot be pinned" not in (run.last_error or ""), run
    assert "is missing screen_result" in (run.last_error or ""), run
    assert tree_state(database.parent) == before, "nothing may be created beside the database"
    assert [path.name for path in database.parent.glob(f".{database.name}.*")] == []
