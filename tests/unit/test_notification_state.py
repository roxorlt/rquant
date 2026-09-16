from __future__ import annotations

import inspect
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rquant.delivery_contracts import DeliveryChannel, DeliveryTarget
from rquant.notification_state import (
    NotificationProjectionAuthoritySnapshot,
    NotificationProjectionSourceReceipt,
    NotificationReplicationError,
    NotificationStateStore,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.serving_read_models import ServingProjectionPayload
from rquant.signal_bus import SignalBusRoutedRecord, SignalBusStore
from rquant.signal_contracts import SignalAction, SignalEnvelope
from rquant.signal_route_spool import (
    ReadonlySignalRouteSpool,
    SignalRouteSpool,
    publish_signal_bus_prefix,
)
from rquant.signal_router_runtime import (
    RouteSourceDescriptor,
    RoutingDecision,
    RunnerSignalBatch,
    SignalRouteCursorStore,
    SourceSnapshot,
    route_runner_signals,
)
from rquant.strategy_runner import RunnerSignalRecord

NOW = datetime(2026, 7, 31, 2, 30, tzinfo=UTC)
POLICY = "a" * 64


def test_notification_serving_delivery_fetch_is_sql_bounded() -> None:
    source = inspect.getsource(NotificationStateStore.serving_snapshot)

    delivery_query = source.split("SELECT * FROM delivery_outbox", maxsplit=1)[1]
    assert "LIMIT ?" in delivery_query.split("fetchall()", maxsplit=1)[0]


def _page_projections(available_at: datetime = NOW) -> tuple[ServingProjectionPayload, ...]:
    rows = {
        "screen_result": (),
        "pool2_watch": (),
        "monitor_event": (),
        "surge_event": (),
        "market_snapshot": (),
        "market_overview": (),
        "intraday_kline": (),
        "screen_bounds": (),
        "minute_coverage": (),
        "canvas_diagnostic": (),
        "canvas_latest_trade_date": (),
        "canvas_hit": (),
        "canvas_definition": (),
    }
    return tuple(
        ServingProjectionPayload(table_name=table_name, available_at=available_at, rows=values)
        for table_name, values in rows.items()
    )


def _signal(seed: str = "b", candidate_id: str = "600000.SH") -> SignalEnvelope:
    return SignalEnvelope(
        schema_version=1,
        strategy_id="n-shape",
        strategy_version="1",
        parameter_fingerprint=seed * 64,
        dataset_snapshot_id="c" * 64,
        feature_snapshot_id="d" * 64,
        event_time=NOW - timedelta(seconds=1),
        available_at=NOW,
        candidate_id=candidate_id,
        action=SignalAction.WATCH,
        reason_codes=("notification-state-test",),
        evidence={},
        expires_at=NOW + timedelta(minutes=5),
        producer_commit="e" * 40,
    )


class _Source:
    def __init__(self, signals: tuple[SignalEnvelope, ...]) -> None:
        self.signals = signals

    def read_batch(self, *, after_sequence: int, limit: int) -> RunnerSignalBatch:
        records = tuple(
            RunnerSignalRecord(sequence=index, signal=signal)
            for index, signal in enumerate(self.signals, start=1)
            if index > after_sequence
        )
        return RunnerSignalBatch(
            snapshot=SourceSnapshot(
                descriptor=RouteSourceDescriptor(
                    source_id="n-shape-v1",
                    generation_id="f" * 64,
                    strategy_spec_fingerprint="1" * 64,
                    first_sequence=1,
                    high_watermark=len(self.signals),
                )
            ),
            after_sequence=after_sequence,
            limit=limit,
            records=records[:limit],
        )


def _published_source(
    tmp_path: Path,
    *,
    signals: tuple[SignalEnvelope, ...] | None = None,
) -> ReadonlySignalRouteSpool:
    signals = signals or (_signal(),)
    bus = SignalBusStore(tmp_path / "signal-bus.sqlite3")
    route_runner_signals(
        source_id="n-shape-v1",
        source=_Source(signals),
        bus=bus,
        cursors=SignalRouteCursorStore(
            tmp_path / "cursor.sqlite3",
            routing_policy_fingerprint=POLICY,
        ),
        routed_at=NOW,
        target_resolver=lambda _signal: RoutingDecision.route(
            routing_policy_fingerprint=POLICY,
            targets=(
                DeliveryTarget(
                    recipient_id="admin",
                    channel=DeliveryChannel.PUSHDEER,
                ),
            ),
        ),
        limit=len(signals),
    )
    root = tmp_path / "signal-spool"
    publish_signal_bus_prefix(bus=bus, spool=SignalRouteSpool(root), limit=10)
    return ReadonlySignalRouteSpool(root)


def test_notification_state_replicates_routed_prefix_into_owned_outbox(
    tmp_path: Path,
) -> None:
    source = _published_source(tmp_path)
    store = NotificationStateStore(tmp_path / "notification-state.sqlite3")
    descriptor = source.source_descriptor()
    records = source.routed_after_global_sequence(
        after_sequence=0,
        through_sequence=descriptor.high_watermark,
        limit=10,
    )

    first = store.replicate(descriptor, records, observed_at=NOW)
    replay = store.replicate(descriptor, (), observed_at=NOW + timedelta(seconds=1))

    assert first.started_after_sequence == 0
    assert first.ended_at_sequence == 1
    assert first.replicated_count == 1
    assert replay.replicated_count == 0
    assert replay.ended_at_sequence == 1
    assert store.signal(1) == _signal()
    assert store.outbox_records()[0].target == DeliveryTarget(
        recipient_id="admin",
        channel=DeliveryChannel.PUSHDEER,
    )


def test_a_second_batch_moves_the_cursor_and_the_column_the_gate_compares_is_the_end(
    tmp_path: Path,
) -> None:
    """Review SF-2/SF-3: the gate has a "write it" half, and it compares the right column.

    Every fixture in this file replicated a single signal, which made
    `first_global_sequence == last_global_sequence == 1` -- so a gate that compared the
    *start* of the source instead of the end, and a gate cut down to "write once and
    never again", both stayed green. Four signals in two batches separate all three.
    """

    source = _published_source(
        tmp_path,
        signals=tuple(_signal("0123456789abcdef"[index]) for index in range(1, 5)),
    )
    store = NotificationStateStore(tmp_path / "notification-state.sqlite3")
    descriptor = source.source_descriptor()
    first_batch = source.routed_after_global_sequence(
        after_sequence=0,
        through_sequence=2,
        limit=10,
    )
    second_batch = source.routed_after_global_sequence(
        after_sequence=2,
        through_sequence=descriptor.high_watermark,
        limit=10,
    )

    store.replicate(descriptor, first_batch, observed_at=NOW)
    after_first = store.replication_cursor()
    store.replicate(
        descriptor,
        second_batch,
        observed_at=NOW + timedelta(seconds=2),
    )
    after_second = store.replication_cursor()

    assert descriptor.high_watermark == 4
    # The start never moves, so a gate comparing it would never see the second batch.
    assert after_first.first_global_sequence == after_second.first_global_sequence == 1
    assert after_first.last_global_sequence == 2
    assert after_second.last_global_sequence == 4
    assert after_second.last_signal_id != after_first.last_signal_id
    assert after_second.updated_at == NOW + timedelta(seconds=2)


def test_an_idle_replication_leaves_the_cursor_row_exactly_as_it_was(
    tmp_path: Path,
) -> None:
    """#271: the second unconditional write in the notifier's two-second loop.

    The cursor row was rewritten on every `replicate` call because `updated_at` carried
    the iteration clock, so an idle notifier dirtied a page, committed it and fsynced it
    every two seconds with nothing to replicate. `updated_at` now says when the cursor
    last advanced.

    **Three signals, not one** (review SF-3): with one, the cursor's start and end are
    both 1, so a gate comparing `first_global_sequence` instead of `last_global_sequence`
    reads as "unchanged" here and writes nothing -- the mutation that broke the fix would
    have stayed green. With three they are 1 and 3, and that gate writes on every one of
    the idle rounds below.
    """

    database = tmp_path / "notification-state.sqlite3"
    source = _published_source(
        tmp_path,
        signals=tuple(_signal("0123456789abcdef"[index]) for index in range(1, 4)),
    )
    store = NotificationStateStore(database)
    descriptor = source.source_descriptor()
    records = source.routed_after_global_sequence(
        after_sequence=0,
        through_sequence=descriptor.high_watermark,
        limit=10,
    )

    store.replicate(descriptor, records, observed_at=NOW)
    advanced = store.replication_cursor()
    watcher = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
    try:
        before = _committed_stamp(watcher)
        for index in range(1, 61):
            store.replicate(descriptor, (), observed_at=NOW + timedelta(seconds=2 * index))
        after = _committed_stamp(watcher)
    finally:
        watcher.close()
    idle = store.replication_cursor()

    assert advanced.first_global_sequence == 1
    assert advanced.last_global_sequence == 3
    assert after == before
    assert idle == advanced
    assert idle.updated_at == advanced.updated_at


def _committed_stamp(watcher: sqlite3.Connection) -> tuple[object, ...]:
    """What this one held-open connection can see of anybody else's commits.

    `PRAGMA data_version` is SQLite's own "has another connection committed since I last
    looked", and it is comparable only within one connection -- hence a connection held
    open across the loop rather than reopened per sample. The alternatives do not answer
    the question: `total_changes` counts what one connection *did* and the store opens its
    own per call; the main file's mtime moves when a checkpoint gets around to running,
    which depends on whether a reader is open; and the `-wal` is created and truncated by
    opening a write connection whether or not it commits. The row counts and the cursor
    row come with it, because a row replaced in place changes no count.
    """

    return (
        watcher.execute("PRAGMA data_version").fetchone()[0],
        tuple(
            watcher.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in (
                "notification_replication_source",
                "signal_envelope",
                "delivery_outbox",
                "notification_projection_authority",
            )
        ),
        tuple(
            watcher.execute(
                "SELECT * FROM notification_replication_source WHERE singleton = 1"
            ).fetchone()
            or ()
        ),
    )


def test_notification_state_rolls_back_signal_outbox_and_cursor_together(
    tmp_path: Path,
) -> None:
    source = _published_source(tmp_path)
    descriptor = source.source_descriptor()
    records = source.routed_after_global_sequence(
        after_sequence=0,
        through_sequence=1,
        limit=10,
    )

    class FaultyStore(NotificationStateStore):
        def _after_replicated_signal(self) -> None:
            raise RuntimeError("injected replication failure")

    store = FaultyStore(tmp_path / "notification-state.sqlite3")
    with pytest.raises(RuntimeError, match="injected"):
        store.replicate(descriptor, records, observed_at=NOW)

    assert store.replication_cursor().last_global_sequence == 0
    assert store.outbox_records() == ()
    assert store.signal(1) is None
    assert store.serving_snapshot(observed_at=NOW, history_limit=10).payload.routes == ()


def test_notification_state_replay_preserves_exact_immutable_route_receipt(
    tmp_path: Path,
) -> None:
    source = _published_source(tmp_path)
    descriptor = source.source_descriptor()
    records = source.routed_after_global_sequence(
        after_sequence=0,
        through_sequence=descriptor.high_watermark,
        limit=10,
    )
    store = NotificationStateStore(tmp_path / "notification-state.sqlite3")

    first = store.replicate(descriptor, records, observed_at=NOW)
    replay = store.replicate(
        descriptor,
        records,
        observed_at=NOW + timedelta(seconds=1),
    )

    assert first.replicated_count == 1
    assert replay.replicated_count == 0
    snapshot = store.serving_snapshot(observed_at=NOW, history_limit=10)
    assert snapshot.payload.routes == (records[0].receipt,)

    conflicting = SignalBusRoutedRecord.model_validate(
        records[0].model_copy(
            update={
                "receipt": records[0].receipt.model_copy(update={"decision_fingerprint": "9" * 64})
            }
        )
    )
    with pytest.raises(NotificationReplicationError, match="route receipt conflicts"):
        store.replicate(
            descriptor,
            (conflicting,),
            observed_at=NOW + timedelta(seconds=2),
        )

    with sqlite3.connect(store.path) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                """
                UPDATE notification_source_route_receipt
                SET receipt_json = '{}'
                WHERE global_sequence = 1
                """
            )
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "DELETE FROM notification_source_route_receipt WHERE global_sequence = 1"
            )


def test_notification_state_serving_snapshot_is_atomic_closed_and_explicitly_truncated(
    tmp_path: Path,
) -> None:
    signals = (
        _signal("2", "600000.SH"),
        _signal("3", "000001.SZ"),
    )
    source = _published_source(tmp_path, signals=signals)
    descriptor = source.source_descriptor()
    records = source.routed_after_global_sequence(
        after_sequence=0,
        through_sequence=descriptor.high_watermark,
        limit=10,
    )
    store = NotificationStateStore(tmp_path / "notification-state.sqlite3")
    store.replicate(descriptor, records, observed_at=NOW)

    snapshot = store.serving_snapshot(observed_at=NOW, history_limit=1)

    assert snapshot.visible_signal_count == 2
    assert snapshot.returned_signal_count == 1
    assert snapshot.omitted_signal_count == 1
    assert snapshot.truncated is True
    assert tuple(record.global_sequence for record in snapshot.payload.signals) == (2,)
    assert tuple(route.signal_id for route in snapshot.payload.routes) == (signals[1].signal_id,)
    assert tuple(delivery.signal_id for delivery in snapshot.payload.deliveries) == (
        signals[1].signal_id,
    )
    assert all(record.signal.available_at <= NOW for record in snapshot.payload.signals)
    assert all(route.routed_at <= NOW for route in snapshot.payload.routes)
    assert all(delivery.updated_at <= NOW for delivery in snapshot.payload.deliveries)


def test_notification_serving_snapshot_limits_visible_history_in_sql(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    signals = tuple(_signal(str((index % 8) + 2), f"{index:06d}.SZ") for index in range(1, 31))
    source = _published_source(tmp_path, signals=signals)
    descriptor = source.source_descriptor()
    records = source.routed_after_global_sequence(
        after_sequence=0,
        through_sequence=descriptor.high_watermark,
        limit=100,
    )
    store = NotificationStateStore(tmp_path / "notification-state.sqlite3")
    store.replicate(descriptor, records, observed_at=NOW)
    statements: list[str] = []
    original = store._connect_readonly

    def traced_connection() -> sqlite3.Connection:
        connection = original()
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(store, "_connect_readonly", traced_connection)

    snapshot = store.serving_snapshot(observed_at=NOW, history_limit=3)

    route_queries = tuple(
        statement
        for statement in statements
        if "FROM notification_source_route_receipt AS receipt" in statement
    )
    assert snapshot.returned_signal_count == 3
    assert route_queries
    assert any("ORDER BY signal.global_sequence DESC" in query for query in route_queries)
    assert any("LIMIT 3" in query for query in route_queries)


def test_notification_state_serving_snapshot_excludes_future_mutable_delivery_state(
    tmp_path: Path,
) -> None:
    source = _published_source(tmp_path)
    descriptor = source.source_descriptor()
    records = source.routed_after_global_sequence(
        after_sequence=0,
        through_sequence=descriptor.high_watermark,
        limit=10,
    )
    store = NotificationStateStore(tmp_path / "notification-state.sqlite3")
    store.replicate(descriptor, records, observed_at=NOW)
    store.claim_due(
        worker_id="future-worker",
        now=NOW + timedelta(seconds=1),
        lease_for=timedelta(seconds=30),
        limit=1,
    )

    snapshot = store.serving_snapshot(observed_at=NOW, history_limit=10)

    assert len(snapshot.payload.signals) == 1
    assert len(snapshot.payload.routes) == 1
    assert snapshot.payload.deliveries == ()


def test_notification_projection_authority_is_pit_bound_and_persisted_atomically(
    tmp_path: Path,
) -> None:
    store = NotificationStateStore(tmp_path / "notification-state.sqlite3")
    authority = NotificationProjectionAuthoritySnapshot.create(
        observed_at=NOW,
        available_at=NOW,
        source_receipts={"market-minute": "1" * 64, "candidate": "2" * 64},
        projections=_page_projections(),
    )

    first = store.publish_projection_authority(authority)
    repeated = store.publish_projection_authority(authority)
    snapshot = store.serving_snapshot(observed_at=NOW, history_limit=10)

    assert first.generation_id == repeated.generation_id == authority.generation_id
    assert first.written
    assert not repeated.written
    assert snapshot.payload.projections == authority.projections
    assert snapshot.projection_generation_id == authority.generation_id
    assert snapshot.projection_source_receipts == authority.source_receipts


def test_the_projection_content_id_names_the_content_and_not_the_iteration_clock() -> None:
    """#271: two iterations that see the same projection compute the same `content_id`.

    `notifier.admin.shadow.v1` calls this every two seconds with a fresh `observed_at`
    and a replica that is replaced every five minutes, so if a clock reached the identity
    the gate compares, the same content would be a new publication hundreds of times over.
    `generation_id` is the other half of the split and is per publication on purpose: it
    is the row key, and two publications of one content have to be two rows (review SF-1).
    """

    first = NotificationProjectionAuthoritySnapshot.create(
        observed_at=NOW,
        available_at=NOW,
        source_receipts={"market-minute": "1" * 64},
        projections=_page_projections(),
    )
    later = NotificationProjectionAuthoritySnapshot.create(
        #: both clocks move, because both are stamped from the iteration:
        #: `create_from_sources` takes `available_at` from the receipts' `published_at`,
        #: which the producer stamps with the same `observed_at` it is called with
        observed_at=NOW + timedelta(hours=3),
        available_at=NOW + timedelta(hours=1),
        source_receipts={"market-minute": "1" * 64},
        projections=_page_projections(),
    )
    changed = NotificationProjectionAuthoritySnapshot.create(
        observed_at=NOW,
        available_at=NOW,
        source_receipts={"market-minute": "2" * 64},
        projections=_page_projections(),
    )

    assert first.content_id == later.content_id
    assert first.observed_at != later.observed_at
    assert first.available_at != later.available_at
    assert first.generation_id != later.generation_id
    assert changed.content_id != first.content_id
    assert changed.generation_id != first.generation_id


def test_the_source_receipt_identity_drops_the_publication_clock() -> None:
    """The receipt id is carried into the authority, so it has to be content too."""

    def receipt(published_at: datetime) -> NotificationProjectionSourceReceipt:
        return NotificationProjectionSourceReceipt.create(
            dataset_id="signal-page-projections",
            generation_id="3" * 64,
            sequence=9,
            event_time=NOW - timedelta(seconds=2),
            published_at=published_at,
            projections=_page_projections(NOW - timedelta(seconds=2)),
        )

    assert receipt(NOW).receipt_id == receipt(NOW + timedelta(hours=3)).receipt_id


def test_republishing_one_projection_content_writes_the_database_once(
    tmp_path: Path,
) -> None:
    """Sixty idle iterations, one row, one write -- the whole of #271 in one assertion."""

    database = tmp_path / "notification-state.sqlite3"
    store = NotificationStateStore(database)

    written = []
    served = set()
    idle_generation = ""
    for index in range(60):
        authority = NotificationProjectionAuthoritySnapshot.create(
            observed_at=NOW + timedelta(seconds=2 * index),
            available_at=NOW + timedelta(seconds=2 * index),
            source_receipts={"market-minute": "1" * 64},
            projections=_page_projections(),
        )
        publication = store.publish_projection_authority(authority)
        if not index:
            idle_generation = authority.generation_id
        written.append(publication.written)
        served.add(publication.generation_id)

    revised_at = NOW + timedelta(minutes=5)
    revised = NotificationProjectionAuthoritySnapshot.create(
        observed_at=revised_at,
        available_at=revised_at,
        source_receipts={"market-minute": "2" * 64},
        projections=_page_projections(revised_at),
    )
    revised_publication = store.publish_projection_authority(revised)

    connection = sqlite3.connect(database)
    try:
        rows = connection.execute(
            "SELECT generation_id, observed_at FROM notification_projection_authority "
            "ORDER BY observed_at"
        ).fetchall()
    finally:
        connection.close()

    assert written == [True] + [False] * 59
    assert revised_publication.written
    # Every one of the fifty-nine iterations that wrote nothing was told which generation
    # is being served -- the one row that is there, not the one it just computed.
    assert served == {idle_generation}
    assert [row[0] for row in rows] == [idle_generation, revised.generation_id]
    # The row keeps the `observed_at` of the iteration that first saw this content, and
    # the fifty-nine that saw it again left it alone.
    assert rows[0][1].startswith("2026-07-31T02:30:00")


def test_an_already_published_projection_never_asks_for_the_write_lock(
    tmp_path: Path,
) -> None:
    """The gate runs on the read-only connection, which is the point of it being there.

    `notifier.admin.shadow.v1` shares this database with nothing, but it does share the
    host with the 17:00 daily and the backups, and a `BEGIN IMMEDIATE` every two seconds
    for a transaction that will find its own row and return is work for no answer. With
    the lock held by somebody else, a republish of published content still succeeds and a
    new content still has to wait for it -- which is how this test tells the two apart.
    """

    database = tmp_path / "notification-state.sqlite3"
    store = NotificationStateStore(database, busy_timeout_ms=50)
    authority = NotificationProjectionAuthoritySnapshot.create(
        observed_at=NOW,
        available_at=NOW,
        source_receipts={"market-minute": "1" * 64},
        projections=_page_projections(),
    )
    store.publish_projection_authority(authority)
    revised_at = NOW + timedelta(minutes=5)
    revised = NotificationProjectionAuthoritySnapshot.create(
        observed_at=revised_at,
        available_at=revised_at,
        source_receipts={"market-minute": "2" * 64},
        projections=_page_projections(revised_at),
    )

    holder = sqlite3.connect(database, isolation_level=None)
    try:
        holder.execute("BEGIN IMMEDIATE")
        repeated = store.publish_projection_authority(authority)
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            store.publish_projection_authority(revised)
    finally:
        holder.execute("ROLLBACK")
        holder.close()

    assert repeated.generation_id == authority.generation_id
    assert not repeated.written


def test_a_projection_that_reverts_to_an_earlier_form_is_published_again(
    tmp_path: Path,
) -> None:
    """Review SF-1: "already published once" is not "is what is being served".

    Publish content A, then B, then A again. The first cut of this fix keyed the table by
    the content alone, so the third publication found A's own earlier row, wrote nothing,
    and `serving_snapshot` -- which orders by `available_at DESC, observed_at DESC` --
    went on serving **B**, silently, for as long as the revert lasted. The gate now
    compares against the latest row, so the revert is a third row and the answer is A.
    """

    store = NotificationStateStore(tmp_path / "notification-state.sqlite3")

    def content(receipt: str, at: datetime) -> NotificationProjectionAuthoritySnapshot:
        return NotificationProjectionAuthoritySnapshot.create(
            observed_at=at,
            available_at=at,
            source_receipts={"market-minute": receipt * 64},
            projections=_page_projections(),
        )

    first = content("1", NOW)
    other = content("2", NOW + timedelta(minutes=1))
    reverted = content("1", NOW + timedelta(minutes=2))

    published = [
        store.publish_projection_authority(first),
        store.publish_projection_authority(other),
        store.publish_projection_authority(reverted),
    ]
    served = store.serving_snapshot(
        observed_at=NOW + timedelta(minutes=2),
        history_limit=10,
    )

    assert first.content_id == reverted.content_id
    assert first.generation_id != reverted.generation_id
    assert [item.written for item in published] == [True, True, True]
    assert served.projection_generation_id == reverted.generation_id
    assert served.projection_source_receipts == {"market-minute": "1" * 64}
    # And the revert, once published, is idle again.
    assert not store.publish_projection_authority(
        content("1", NOW + timedelta(minutes=3))
    ).written


def test_the_gate_never_drags_the_payload_through_its_sort(tmp_path: Path) -> None:
    """Review DF-1: the gate's query runs every two seconds against a growing table.

    There is no index on `available_at`, so this is a scan plus a temporary B-tree for the
    sort. Selecting `payload_json` puts a 20 KB blob per row through that sorter: 29 ms at
    5k rows and 400-650 ms at 80k, about twenty-five times what the same query costs
    without it. The legacy branch fetches the payload by primary key instead, for the one
    row that needs it.
    """

    store = NotificationStateStore(tmp_path / "notification-state.sqlite3")
    store.publish_projection_authority(
        NotificationProjectionAuthoritySnapshot.create(
            observed_at=NOW,
            available_at=NOW,
            source_receipts={"market-minute": "1" * 64},
            projections=_page_projections(),
        )
    )
    connection = sqlite3.connect(
        f"file:{tmp_path / 'notification-state.sqlite3'}?mode=ro",
        uri=True,
    )
    try:
        cursor = connection.execute(
            NotificationStateStore._LATEST_PROJECTION_AUTHORITY_SQL,
            (
                NOW.isoformat(timespec="microseconds"),
                NOW.isoformat(timespec="microseconds"),
            ),
        )
        columns = tuple(description[0] for description in cursor.description)
        plan = connection.execute(
            "EXPLAIN QUERY PLAN " + NotificationStateStore._LATEST_PROJECTION_AUTHORITY_SQL,
            (
                NOW.isoformat(timespec="microseconds"),
                NOW.isoformat(timespec="microseconds"),
            ),
        ).fetchall()
    finally:
        connection.close()

    assert columns == ("generation_id", "content_id")
    assert "payload_json" not in NotificationStateStore._LATEST_PROJECTION_AUTHORITY_SQL
    # The sort is what makes the projected columns matter; if this ever stops being a
    # temp-B-tree sort the column list is free again and this test can go.
    assert any("TEMP B-TREE" in str(step[3]).upper() for step in plan), plan
    # The payload is reachable by primary key, which is how the legacy branch reads it.
    assert "WHERE generation_id = ?" in NotificationStateStore._PROJECTION_AUTHORITY_PAYLOAD_SQL


def test_a_row_stamped_ahead_of_the_clock_cannot_make_the_gate_skip(tmp_path: Path) -> None:
    """Review DF-3: the gate and the reader must answer at the same instant.

    `serving_snapshot` takes the latest row `available_at <= now AND observed_at <= now`.
    Without that filter the gate took the latest row *overall*, so a row stamped ahead of
    the clock -- which takes a clock that went backwards to produce -- would have made the
    gate skip a publication the reader could not see, and the reader would have gone on
    serving the older content. SF-1's shape by another route.
    """

    store = NotificationStateStore(tmp_path / "notification-state.sqlite3")

    def content(receipt: str, at: datetime) -> NotificationProjectionAuthoritySnapshot:
        return NotificationProjectionAuthoritySnapshot.create(
            observed_at=at,
            available_at=at,
            source_receipts={"market-minute": receipt * 64},
            projections=_page_projections(),
        )

    store.publish_projection_authority(content("1", NOW))
    ahead = content("2", NOW + timedelta(days=1))
    store.publish_projection_authority(ahead)

    now = NOW + timedelta(minutes=1)
    published = store.publish_projection_authority(content("2", now))
    served = store.serving_snapshot(observed_at=now, history_limit=10)

    # The reader could not see the row a day ahead, so the gate must not have used it.
    assert published.written
    assert served.projection_generation_id == published.generation_id
    assert served.projection_source_receipts == {"market-minute": "2" * 64}
    assert served.projection_generation_id != ahead.generation_id


def test_replaying_a_snapshot_that_is_no_longer_the_latest_is_refused(
    tmp_path: Path,
) -> None:
    """Review DF-2: the `IntegrityError` branch is reachable, so it is tested.

    Two contents published at the same instant are two rows the order separates only by
    `generation_id`; re-publishing the one that lost that tie is a snapshot whose content
    is not what the latest row holds and whose row is already there. The notifier's loop
    cannot do this -- it builds a new snapshot from the clock every iteration -- but a
    caller replaying a stored snapshot object can.
    """

    store = NotificationStateStore(tmp_path / "notification-state.sqlite3")
    candidates = sorted(
        (
            NotificationProjectionAuthoritySnapshot.create(
                observed_at=NOW,
                available_at=NOW,
                source_receipts={"market-minute": receipt * 64},
                projections=_page_projections(),
            )
            for receipt in ("1", "2")
        ),
        key=lambda item: item.generation_id,
    )
    loses_the_tie, wins_the_tie = candidates

    assert store.publish_projection_authority(loses_the_tie).written
    assert store.publish_projection_authority(wins_the_tie).written

    with pytest.raises(NotificationReplicationError, match="already published"):
        store.publish_projection_authority(loses_the_tie)

    # The refusal rolled back cleanly and the store still works.
    assert not store.publish_projection_authority(wins_the_tie).written


def test_a_projection_authority_written_before_the_content_gate_is_still_read(
    tmp_path: Path,
) -> None:
    """Production's state database is older than this change and outlives a deployment.

    Rows written before v0.33.13 hash `observed_at` into `generation_id` and have no
    `content_id` at all. They are read here exactly as they were written; nothing
    recomputes or rewrites them. And while such a row is still the latest, the gate has to
    derive its content id from the payload -- a primary-key read of the one row that needs
    it (review DF-1) -- or the first iteration after an install would publish a row for
    content that is already there.
    """

    store = NotificationStateStore(tmp_path / "notification-state.sqlite3")
    content = NotificationProjectionAuthoritySnapshot.create(
        observed_at=NOW,
        available_at=NOW,
        source_receipts={"market-minute": "1" * 64},
        projections=_page_projections(),
    )
    payload = content.model_dump(mode="python", exclude={"generation_id", "content_id"})
    legacy = NotificationProjectionAuthoritySnapshot.model_validate(
        {**payload, "generation_id": canonical_sha256(payload)}
    )

    assert legacy.content_id is None
    assert legacy.generation_id != content.generation_id
    assert store.publish_projection_authority(legacy).written

    snapshot = store.serving_snapshot(observed_at=NOW, history_limit=10)
    # The same content again, in the new form: the legacy row's content id is derived and
    # matches, so nothing is written.
    unchanged = store.publish_projection_authority(
        NotificationProjectionAuthoritySnapshot.create(
            observed_at=NOW + timedelta(minutes=1),
            available_at=NOW + timedelta(minutes=1),
            source_receipts={"market-minute": "1" * 64},
            projections=_page_projections(),
        )
    )
    changed = store.publish_projection_authority(
        NotificationProjectionAuthoritySnapshot.create(
            observed_at=NOW + timedelta(minutes=2),
            available_at=NOW + timedelta(minutes=2),
            source_receipts={"market-minute": "2" * 64},
            projections=_page_projections(),
        )
    )

    assert snapshot.projection_generation_id == legacy.generation_id
    assert not unchanged.written
    assert unchanged.generation_id == legacy.generation_id
    assert changed.written


def test_a_projection_generation_that_matches_neither_identity_is_refused() -> None:
    content = NotificationProjectionAuthoritySnapshot.create(
        observed_at=NOW,
        available_at=NOW,
        source_receipts={"market-minute": "1" * 64},
        projections=_page_projections(),
    )

    with pytest.raises(ValueError, match="does not match content"):
        NotificationProjectionAuthoritySnapshot.model_validate(
            {
                **content.model_dump(mode="python", exclude={"generation_id"}),
                "generation_id": "f" * 64,
            }
        )
    with pytest.raises(ValueError, match="content does not match its identity"):
        NotificationProjectionAuthoritySnapshot.model_validate(
            {**content.model_dump(mode="python"), "content_id": "e" * 64}
        )


def test_notification_projection_authority_is_assembled_from_verified_pit_receipts() -> None:
    source = NotificationProjectionSourceReceipt.create(
        dataset_id="market-live-authority",
        generation_id="3" * 64,
        sequence=9,
        event_time=NOW - timedelta(seconds=2),
        published_at=NOW - timedelta(seconds=1),
        projections=_page_projections(NOW - timedelta(seconds=2)),
    )

    authority = NotificationProjectionAuthoritySnapshot.create_from_sources(
        observed_at=NOW,
        sources=(source,),
    )

    assert authority.available_at == source.published_at
    assert authority.source_receipts == {source.dataset_id: source.receipt_id}
    assert authority.projections == source.projections

    with pytest.raises(ValueError, match="future"):
        NotificationProjectionAuthoritySnapshot.create_from_sources(
            observed_at=NOW - timedelta(seconds=2),
            sources=(source,),
        )


def test_notification_projection_authority_keeps_old_pit_snapshot_visible(
    tmp_path: Path,
) -> None:
    store = NotificationStateStore(tmp_path / "notification-state.sqlite3")
    old = NotificationProjectionAuthoritySnapshot.create(
        observed_at=NOW,
        available_at=NOW,
        source_receipts={"market-minute": "1" * 64},
        projections=_page_projections(),
    )
    revised_at = NOW + timedelta(minutes=1)
    revised = NotificationProjectionAuthoritySnapshot.create(
        observed_at=revised_at,
        available_at=revised_at,
        source_receipts={"market-minute": "2" * 64},
        projections=_page_projections(revised_at),
    )
    store.publish_projection_authority(old)
    store.publish_projection_authority(revised)

    historical = store.serving_snapshot(observed_at=NOW, history_limit=10)
    current = store.serving_snapshot(observed_at=revised_at, history_limit=10)

    assert historical.projection_generation_id == old.generation_id
    assert current.projection_generation_id == revised.generation_id


def test_notification_projection_authority_requires_all_owned_projections() -> None:
    with pytest.raises(ValueError, match="exactly the notification projections"):
        NotificationProjectionAuthoritySnapshot.create(
            observed_at=NOW,
            available_at=NOW,
            source_receipts={"market-minute": "1" * 64},
            projections=_page_projections()[:-1],
        )
