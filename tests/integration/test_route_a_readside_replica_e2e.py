"""#250 acceptance: the read-side roles work while a writer holds the main database.

The premise is CLAUDE.md's single-writer rule and what package O's review found behind it.
`rquant-monitor` holds the write lock on `data/rquant.duckdb` from 09:25 to 15:00, and
DuckDB refuses *every* new connection to a locked file, `read_only=True` included. The
`auction_gap` candidate publisher may publish only inside 09:26-09:30 Asia/Shanghai
(`runtime_builder_candidate.py`), which is entirely inside that window, and its manifest
named the main database — so it never published, `market-minute.source.v1` and
`watchlist-quote.source.v1` failed every iteration on `required authority has no
not_visible snapshot`, and the live chain stopped at the pre-market.

So this file runs the window rather than describing it: two installed generations, a real
staged and published authority chain, the wrapper's own argv and child environment, a
market calendar that opens the session and the five sessions before it, a real
five-minute replica carrying those five sessions' `daily_bar` rows — and a **real second
DuckDB connection holding the main database in write mode for the whole test**, which is
what `rquant-monitor` is. Any role that reaches for the main file fails here for exactly
the reason it failed on the host.

The order is the chain's own: the publisher at 09:26 inside its window, then the two
sources at 09:35, in the morning phase, reading what it published.
"""

from __future__ import annotations

import hashlib
import os
import stat
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from unittest import mock
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd
import pytest

import rquant.runtime_service_builtin as builtin_module
import rquant.runtime_service_main as service_main
from rquant.auction_match_gateway import AuctionMatchGateway, AuctionMatchGatewayConfig
from rquant.live_spool import LiveBatchSpool
from rquant.reference_data_registry import (
    ReferenceDataset,
    ReferenceRecord,
    ReferenceRegistry,
)
from rquant.runtime_deployment_bundle import acknowledge_runtime_schema_rollout_preparation
from rquant.runtime_service_control import RuntimeServiceControl
from rquant.runtime_service_entrypoint import RuntimeServiceKind
from tests.integration.test_route_a_legacy_binding_e2e import (
    PRODUCTION_ROOT,
    RouteAWorld,
    _production_bundle,
    _StopAfterOneIteration,
)
from tests.unit.test_runtime_authority_publish import World

pytestmark = pytest.mark.integration

_SHANGHAI = ZoneInfo("Asia/Shanghai")

#: The session this window trades, and the five sessions the publisher reads before it.
#: Fixed dates rather than "tomorrow": every point-in-time comparison on this path is
#: against an explicit timestamp (the replica's mtime, the auction batch's `available_at`,
#: the reference generation's `published_at`), so the world can be placed where the
#: calendar the bundle installs actually opens.
OPEN_DATES = (
    date(2026, 8, 3),
    date(2026, 8, 4),
    date(2026, 8, 5),
    date(2026, 8, 6),
    date(2026, 8, 7),
    date(2026, 8, 10),
    date(2026, 8, 11),
)
TRADE_DATE = OPEN_DATES[-1]
PRIOR_DATES = OPEN_DATES[1:-1]

#: 09:26:30 Asia/Shanghai: inside the publisher's 09:26-09:30 window, a minute and a half
#: after `rquant-monitor` took the main database's write lock, and just after the
#: auction-match source captured the batch this publisher consumes (the gateway refuses
#: auction data received before 09:26, so 09:26 is the earliest either of them can act).
PUBLISH_AT = datetime.combine(TRADE_DATE, time(9, 26, 30), tzinfo=_SHANGHAI).astimezone(UTC)
#: 09:31, the morning phase — when the two sources actually load a candidate universe
#: (before 09:30 the session is `pre_open` and `may_fetch_market_minute` is false, so the
#: minute source returns without touching the universe at all).
CONSUME_AT = datetime.combine(TRADE_DATE, time(9, 31), tzinfo=_SHANGHAI).astimezone(UTC)
#: When the second install stamps its schema rollout window. A producer records its
#: dual-write with the *service's* clock, and `SchemaRolloutStore` refuses a record outside
#: `[started_at, deadline]`, so a world whose roles run at a market clock has to open that
#: window at the market clock; the installer takes it as a parameter for exactly this.
SCHEMA_ROLLOUT_STARTED_AT = PUBLISH_AT - timedelta(minutes=1)
#: When the replica-sync timer last replaced the replica: inside the five-minute bound and
#: before the publisher looks at it, because the publisher refuses future evidence.
REPLICA_SYNCED_AT = datetime.combine(TRADE_DATE, time(9, 23), tzinfo=_SHANGHAI).astimezone(UTC)
AUCTION_AVAILABLE_AT = datetime.combine(
    TRADE_DATE, time(9, 26, 5), tzinfo=_SHANGHAI
).astimezone(UTC)
REFERENCE_PUBLISHED_AT = datetime.combine(
    TRADE_DATE, time(9, 20), tzinfo=_SHANGHAI
).astimezone(UTC)
#: when the installer sealed the two document-driven strategies' candidate inputs
SEALED_CAPTURED_AT = datetime.combine(TRADE_DATE, time(9, 0), tzinfo=_SHANGHAI).astimezone(UTC)

#: the generation this window's bundle was installed over, as in the Route A windows
PREVIOUS_COMMIT = "1e2d3c4b5a69788796a5b4c3d2e1f00918273645"

CODE = "300001.SZ"
AUCTION_GAP_SERVICE_ID = "candidate.auction_gap.v1"
CANDIDATE_ROLE = "candidate_publisher"
MINUTE_ROLE = "market_minute_source"
QUOTE_ROLE = "watchlist_quote_source"


def _instance_name(service_id: str) -> str:
    return "svc-" + hashlib.sha256(service_id.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------------------
# The world
# ---------------------------------------------------------------------------------------


def _write_replica(path: Path, *, trade_dates: tuple[date, ...], synced_at: datetime) -> None:
    """The file `scripts/sync-readonly-replica.sh` leaves behind, with its mode and no WAL.

    The script copies the main database, checkpoints the copy, verifies it opens read-only
    as a single file, then `mv`s it into place and removes the WAL. So the replica is
    WAL-free, owned by the runtime user, and 0644 — which is why #249 relaxed the readers'
    mode rule instead of chmod-ing a file that is replaced every five minutes.
    """

    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()
    connection = duckdb.connect(str(path))
    try:
        connection.execute(
            "CREATE TABLE daily_bar(ts_code VARCHAR, trade_date DATE, vol DOUBLE)"
        )
        connection.executemany(
            "INSERT INTO daily_bar VALUES (?, ?, ?)",
            [(CODE, trade_date, 1_000.0) for trade_date in trade_dates],
        )
        connection.execute("CHECKPOINT")
    finally:
        connection.close()
    assert not Path(f"{path}.wal").exists()
    path.chmod(0o644)
    stamp = synced_at.timestamp()
    os.utime(path, (stamp, stamp))


def _publish_auction_batch(spool_root: Path, *, producer_commit: str) -> None:
    """One real auction-match batch in the source's spool, as `auction-match.source.v1` writes."""

    spool = LiveBatchSpool(spool_root)
    frame = pd.DataFrame(
        [
            {
                "ts_code": CODE,
                "trade_date": TRADE_DATE,
                "price": 10.5,
                "vol": 20_000.0,
                "amount": 210_000.0,
                "pre_close": 10.0,
                "turnover_rate": 0.2,
                "volume_ratio": 9.9,
            }
        ]
    )
    gateway = AuctionMatchGateway(
        spool=spool,
        fetcher=lambda _: frame,
        config=AuctionMatchGatewayConfig(
            producer_version="auction-match-source-v1",
            producer_commit=producer_commit,
            min_coverage_ratio=1.0,
        ),
    )
    capture = gateway.capture_once(
        trade_date=TRADE_DATE,
        received_at=AUCTION_AVAILABLE_AT,
        expected_codes=(CODE,),
    )
    assert capture.published is True


def _publish_reference_generation(path: Path) -> None:
    """The four datasets the auction-gap input asks for, in one published generation."""

    registry = ReferenceRegistry(path)
    effective_from = datetime.combine(TRADE_DATE, time(0, 0), tzinfo=_SHANGHAI).astimezone(UTC)
    payloads = (
        (ReferenceDataset.ST_STATUS, {"is_st": False}),
        (ReferenceDataset.SUSPENSION_STATUS, {"is_suspended": False}),
        (ReferenceDataset.LISTING_STATUS, {"status": "listed"}),
        (
            ReferenceDataset.PRICE_LIMIT_REGIME,
            {
                "limit_eligible": True,
                "limit_percent": 0.1,
                "limit_up_price": 11.0,
                "limit_down_price": 9.0,
            },
        ),
    )
    for dataset, payload in payloads:
        registry.append(
            ReferenceRecord(
                dataset_id=dataset,
                key=CODE,
                effective_from=effective_from,
                revision=1,
                source="test.reference",
                first_available_at=REFERENCE_PUBLISHED_AT,
                payload=payload,
            )
        )
    registry.publish(published_at=REFERENCE_PUBLISHED_AT)


def _seal_candidate_documents(inputs: Any, *, producer_commit: str) -> None:
    """The two sealed candidate documents the other two strategies publish from.

    `market-minute.source.v1` and `watchlist-quote.source.v1` bind all three strategies as
    *required* candidate authorities, so the session cannot be reached with only the
    auction-gap snapshot. The bytes come from the production generator's own
    `build_sealed_candidate_payload`, sealed for this session so the consumers accept them
    (their `trade_date` has to be the session's), and captured before the publisher runs.
    """

    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
    from build_runtime_production_inputs import build_sealed_candidate_payload

    for strategy_id, path in (
        ("n_shape", inputs.n_shape_candidate_input_path),
        ("growth_board_surge", inputs.growth_board_candidate_input_path),
    ):
        payload = build_sealed_candidate_payload(
            strategy_id=strategy_id,
            producer_commit=producer_commit,
            trade_date=TRADE_DATE,
            captured_at=SEALED_CAPTURED_AT,
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        path.chmod(0o600)


class ReplicaWorld(RouteAWorld):
    """A Route A world placed inside the session, with a writer holding the main database."""

    inputs: Any = None

    def manifest(self, service_id: str) -> Any:
        return next(item for item in self.profile.manifests if item.service_id == service_id)

    def setting(self, service_id: str, field: str) -> Path:
        return Path(str(self.manifest(service_id).settings[field]))

    def run(
        self,
        role: str,
        *,
        instance: str,
        now: datetime,
        adapter_factory: Any = None,
        watchlist_quote_provider_factory: Any = None,
    ) -> Any:
        """One role, one loop iteration, at `now`, with the wrapper's own argv."""

        resolved = self.world.resolve(role, instance)
        argv = list(resolved["module_argv"])
        index = argv.index("--control-root") + 1
        argv[index] = str(self.runtime_root / Path(argv[index]).relative_to(PRODUCTION_ROOT))
        arguments = service_main.build_parser().parse_args(argv)
        stop = _StopAfterOneIteration()
        real_event = service_main.Event
        real_registry = builtin_module.build_builtin_registry
        extra: dict[str, Any] = {}
        if adapter_factory is not None:
            extra["adapter_factory"] = adapter_factory
        if watchlist_quote_provider_factory is not None:
            extra["watchlist_quote_provider_factory"] = watchlist_quote_provider_factory
        service_main.Event = lambda: stop  # type: ignore[assignment]
        builtin_module.build_builtin_registry = (  # type: ignore[assignment]
            lambda **kwargs: real_registry(clock=lambda: now, **{**extra, **kwargs})
        )
        try:
            with mock.patch.dict(os.environ, dict(resolved["environment"]), clear=True):
                code = service_main.run(arguments)
        finally:
            service_main.Event = real_event  # type: ignore[assignment]
            builtin_module.build_builtin_registry = real_registry  # type: ignore[assignment]
        assert stop.iterations == 1, f"{role} never entered its service loop"
        control_root = Path(argv[argv.index("--control-root") + 1])
        manifest = next(
            item
            for item in self.profile.manifests
            if _instance_name(item.service_id) == instance
        )
        heartbeat = RuntimeServiceControl.read_heartbeat(control_root, manifest.service_spec)
        return code, heartbeat


@pytest.fixture
def session_world(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> ReplicaWorld:
    """Two installed generations, the session's evidence, and a locked main database."""

    world = World(tmp_path / "root", monkeypatch).build()
    runtime_root = tmp_path / "host" / "data" / "runtime"
    _production_bundle(
        tmp_path / "previous",
        monkeypatch,
        producer_commit=PREVIOUS_COMMIT,
        runtime_root=runtime_root,
        schema_bootstrap_reason="#250 acceptance bootstrap",
        market_calendar_open_dates=OPEN_DATES,
    )
    inputs, profile, receipt, sealed = _production_bundle(
        tmp_path / "target",
        monkeypatch,
        producer_commit=world.commit,
        runtime_root=runtime_root,
        #: only the first install into an empty root may carry a bootstrap reason, and the
        #: second install without one is what prepares the rollout plans
        schema_bootstrap_reason=None,
        #: one registry root cannot hold two commits' definitions (#225)
        definition_registry_root=runtime_root.parent / f"definitions-{world.commit[:7]}",
        market_calendar_open_dates=OPEN_DATES,
        schema_rollout_started_at=SCHEMA_ROLLOUT_STARTED_AT,
    )
    assert receipt.previous_generation_hash is not None

    route = ReplicaWorld(world, inputs.runtime_root)
    route.inputs = inputs
    route.profile = profile
    route.receipt = receipt
    route.sealed_credentials = sealed
    #: the PREPARE round the installer's own command acknowledges (the runbook step
    #: between B-7 and C-3), inside the window the install just opened. It is what moves
    #: every plan to DUAL_WRITE, which is why the roles below never acknowledge anything
    #: themselves — they only record dual-writes, at their own clock.
    acknowledge_runtime_schema_rollout_preparation(route.runtime_root, now=PUBLISH_AT)
    route.stage_and_publish()

    _write_replica(
        inputs.readonly_replica_database_path,
        trade_dates=PRIOR_DATES,
        synced_at=REPLICA_SYNCED_AT,
    )
    _publish_auction_batch(
        route.setting(AUCTION_GAP_SERVICE_ID, "auction_spool_root"),
        producer_commit=world.commit,
    )
    _publish_reference_generation(
        route.setting(AUCTION_GAP_SERVICE_ID, "reference_registry_path")
    )
    _seal_candidate_documents(inputs, producer_commit=world.commit)
    return route


@pytest.fixture
def locked_main_database(session_world: ReplicaWorld) -> Any:
    """`rquant-monitor`, as a second DuckDB connection holding the main file in write mode.

    Not a stand-in for the lock: it is the lock. While this connection is open, DuckDB
    refuses every new connection to that path in this process and in any other, which is
    the whole of #250.
    """

    path = session_world.inputs.operational_database_path
    path.parent.mkdir(parents=True, exist_ok=True)
    writer = duckdb.connect(str(path))
    writer.execute("CREATE TABLE IF NOT EXISTS monitor_event(id BIGINT)")
    writer.execute("INSERT INTO monitor_event VALUES (1)")
    try:
        yield writer
    finally:
        writer.close()


# ---------------------------------------------------------------------------------------
# The premise: the lock is real
# ---------------------------------------------------------------------------------------


def test_the_writer_really_refuses_a_read_only_connection_to_the_main_database(
    session_world: ReplicaWorld,
    locked_main_database: Any,
) -> None:
    """CLAUDE.md's rule, asserted rather than assumed, before anything is built on it."""

    with pytest.raises(duckdb.Error):
        duckdb.connect(str(session_world.inputs.operational_database_path), read_only=True)

    #: and the replica, at the same instant, opens
    replica = duckdb.connect(
        str(session_world.inputs.readonly_replica_database_path), read_only=True
    )
    try:
        assert replica.execute("SELECT count(*) FROM daily_bar").fetchone()[0] == len(
            PRIOR_DATES
        )
    finally:
        replica.close()


def test_the_replica_has_the_mode_and_the_absence_of_a_wal_the_readers_require(
    session_world: ReplicaWorld,
) -> None:
    replica = session_world.inputs.readonly_replica_database_path
    observed = replica.lstat()

    assert stat.S_IMODE(observed.st_mode) == 0o644
    assert observed.st_uid == os.geteuid()
    assert observed.st_nlink == 1
    assert not Path(f"{replica}.wal").exists()


# ---------------------------------------------------------------------------------------
# The three roles, in the session, over the locked main database
# ---------------------------------------------------------------------------------------


def test_the_auction_gap_publisher_publishes_inside_its_window(
    session_world: ReplicaWorld,
    locked_main_database: Any,
) -> None:
    """09:26, one minute after the monitor took the lock: it reads the replica and publishes."""

    instance = _instance_name(AUCTION_GAP_SERVICE_ID)
    code, heartbeat = session_world.run(CANDIDATE_ROLE, instance=instance, now=PUBLISH_AT)

    assert code == 0
    assert heartbeat is not None, "the publisher wrote no heartbeat"
    assert heartbeat.last_error is None
    #: not "it did not crash": the step returns an empty result outside 09:26-09:30 and a
    #: `auction_gap_input_unavailable` degradation when it cannot read its evidence, and
    #: both of those also exit 0
    assert heartbeat.degraded_reasons == ()
    assert heartbeat.processed_count == 1
    assert heartbeat.output_sequence >= 0
    assert set(heartbeat.source_generations) >= {"candidate_input", "strategy_candidate"}


class _MinuteAdapter:
    """`market-minute.source.v1`'s Tushare adapter, with the network taken out of it."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []

    def rt_min(self, codes: list[str], freq: str = "1min") -> pd.DataFrame:
        self.calls.append(tuple(codes))
        return pd.DataFrame(
            [
                {
                    "ts_code": code,
                    "trade_time": f"{TRADE_DATE.isoformat()} 09:30:00",
                    "open": 10.0,
                    "high": 10.2,
                    "low": 9.9,
                    "close": 10.1,
                    "vol": 1_000.0,
                    "amount": 10_100.0,
                }
                for code in codes
            ]
        )


class _QuoteProvider:
    """`watchlist-quote.source.v1`'s provider, likewise."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []

    def __call__(
        self,
        codes: tuple[str, ...],
        *,
        timeout_seconds: float,
        on_started: Any,
    ) -> pd.DataFrame:
        self.calls.append(tuple(codes))
        on_started(CONSUME_AT)
        return pd.DataFrame(
            [
                {
                    "ts_code": code,
                    "observed_at": CONSUME_AT,
                    "price": 10.1,
                    "open": 10.0,
                    "high": 10.2,
                    "low": 9.9,
                    "volume": 1_000.0,
                    "amount": 10_100.0,
                }
                for code in codes
            ]
        )


def _publish_every_candidate_authority(session_world: ReplicaWorld) -> None:
    """The three candidate publishers, at 09:26:30, in the order the profile lists them."""

    for manifest in session_world.profile.manifests:
        if manifest.service_kind is not RuntimeServiceKind.CANDIDATE_PUBLISHER:
            continue
        code, heartbeat = session_world.run(
            CANDIDATE_ROLE,
            instance=_instance_name(manifest.service_id),
            now=PUBLISH_AT,
        )
        assert code == 0, manifest.service_id
        assert heartbeat is not None and heartbeat.last_error is None, manifest.service_id
        assert heartbeat.degraded_reasons == (), manifest.service_id


def test_market_minute_reads_the_snapshot_the_publisher_left(
    session_world: ReplicaWorld,
    locked_main_database: Any,
) -> None:
    """The failure #250 is measured by: `required authority has no not_visible snapshot`."""

    _publish_every_candidate_authority(session_world)
    adapter = _MinuteAdapter()

    code, heartbeat = session_world.run(
        MINUTE_ROLE,
        instance=_instance_name("market-minute.source.v1"),
        now=CONSUME_AT,
        adapter_factory=lambda: adapter,
    )

    assert code == 0
    assert heartbeat is not None
    assert heartbeat.last_error is None
    assert heartbeat.degraded_reasons == ()
    #: the universe it fetched is the one the auction-gap publisher put in the snapshot
    assert adapter.calls == [(CODE,)]
    assert "candidate_universe" in heartbeat.source_generations
    assert heartbeat.processed_count == 1


def test_watchlist_quote_reads_the_snapshot_the_publisher_left(
    session_world: ReplicaWorld,
    locked_main_database: Any,
) -> None:
    _publish_every_candidate_authority(session_world)
    provider = _QuoteProvider()

    code, heartbeat = session_world.run(
        QUOTE_ROLE,
        instance=_instance_name("watchlist-quote.source.v1"),
        now=CONSUME_AT,
        watchlist_quote_provider_factory=lambda: provider,
    )

    assert code == 0
    assert heartbeat is not None
    assert heartbeat.last_error is None
    assert heartbeat.degraded_reasons == ()
    assert provider.calls == [(CODE,)]
    assert heartbeat.processed_count == 1


# ---------------------------------------------------------------------------------------
# The counterfactual, and the five-minute replacement
# ---------------------------------------------------------------------------------------


def test_the_same_read_against_the_main_database_fails_at_the_same_instant(
    session_world: ReplicaWorld,
    locked_main_database: Any,
) -> None:
    """What the manifest used to say, run against the same locked file, in the same world.

    This is the causation #250 names, not an argument about it: the publisher's own input
    assembler, given the main database instead of the replica while `rquant-monitor` holds
    it, cannot get a connection — which is why the 09:26-09:30 window never produced
    anything on the host.
    """

    from rquant.auction_gap_candidate_input import (
        AuctionGapCandidateInputError,
        assemble_auction_gap_candidate_batch,
    )
    from rquant.reference_data_registry import ReadonlyReferenceRegistry
    from rquant.runtime_market_session import load_market_calendar_authority

    manifest = session_world.manifest(AUCTION_GAP_SERVICE_ID)
    calendar = load_market_calendar_authority(
        Path(str(manifest.settings["calendar_path"])),
        expected_commit=str(manifest.settings["calendar_expected_commit"]),
    )
    arguments = {
        "auction_spool": LiveBatchSpool(
            session_world.setting(AUCTION_GAP_SERVICE_ID, "auction_spool_root")
        ),
        "reference_registry": ReadonlyReferenceRegistry(
            session_world.setting(AUCTION_GAP_SERVICE_ID, "reference_registry_path")
        ),
        "calendar": calendar,
        "trade_date": TRADE_DATE,
        "observed_at": PUBLISH_AT,
        "producer_commit": manifest.producer_commit,
    }

    with pytest.raises(AuctionGapCandidateInputError):
        assemble_auction_gap_candidate_batch(
            daily_database_path=session_world.inputs.operational_database_path,
            **arguments,
        )

    #: and the binding the profile actually installs, at the same instant, succeeds
    batch = assemble_auction_gap_candidate_batch(
        daily_database_path=session_world.inputs.readonly_replica_database_path,
        **arguments,
    )
    assert batch.authority.trade_date == TRADE_DATE


def test_the_publisher_survives_an_atomic_replica_replacement_between_iterations(
    session_world: ReplicaWorld,
    locked_main_database: Any,
) -> None:
    """`sync-readonly-replica.sh` `mv`s a new file over the replica every five minutes.

    A reader that held a connection across that would keep reading an unlinked inode until
    it was restarted. This role opens, reads and closes inside one iteration, so the
    replacement lands between two of them and the next one reads the new file.
    """

    replica = session_world.inputs.readonly_replica_database_path
    instance = _instance_name(AUCTION_GAP_SERVICE_ID)

    first_code, first = session_world.run(CANDIDATE_ROLE, instance=instance, now=PUBLISH_AT)
    assert first_code == 0
    assert first is not None and first.degraded_reasons == ()
    before_inode = replica.lstat().st_ino

    #: exactly what the sync script does: build the next generation beside the replica and
    #: `mv` it over — one `rename(2)`, so a reader either sees the old inode or the new one
    staging = replica.with_name(f"{replica.name}.tmp.{os.getpid()}")
    _write_replica(
        staging,
        trade_dates=PRIOR_DATES,
        #: the next five-minute generation, still behind the instant the second iteration
        #: observes — the publisher refuses a snapshot stamped after `observed_at`
        synced_at=REPLICA_SYNCED_AT + timedelta(minutes=4),
    )
    os.replace(staging, replica)
    assert replica.lstat().st_ino != before_inode

    second_code, second = session_world.run(
        CANDIDATE_ROLE,
        instance=instance,
        now=PUBLISH_AT + timedelta(seconds=60),
    )

    assert second_code == 0
    assert second is not None
    assert second.last_error is None
    assert second.degraded_reasons == ()
    assert second.processed_count == 1


def test_the_generator_refuses_a_read_side_role_on_the_main_database(
    session_world: ReplicaWorld,
) -> None:
    """The rule that keeps this window from being re-lost, over this world's own inputs.

    Two shapes, because the operator can arrive at the main database two ways: by naming
    it in a role's field, and by pointing the replica's name at it. Both are refused with
    the role and the field named, before anything is installed.
    """

    from rquant.runtime_production_profile import (
        _validate_read_side_database_bindings,
        build_production_runtime_profile,
    )

    inputs = session_world.inputs
    manifest = session_world.manifest(AUCTION_GAP_SERVICE_ID)
    doctored = manifest.model_copy(
        update={
            "settings": {
                **dict(manifest.settings),
                "daily_database_path": str(inputs.operational_database_path),
            }
        }
    )
    with pytest.raises(ValueError) as error:
        _validate_read_side_database_bindings(
            (doctored,),
            operational_database_path=inputs.operational_database_path,
            readonly_replica_database_path=inputs.readonly_replica_database_path,
        )
    assert AUCTION_GAP_SERVICE_ID in str(error.value)
    assert "daily_database_path" in str(error.value)

    replica = inputs.readonly_replica_database_path
    replica.unlink()
    replica.symlink_to(inputs.operational_database_path)
    with pytest.raises(ValueError, match="main database"):
        build_production_runtime_profile(inputs)
