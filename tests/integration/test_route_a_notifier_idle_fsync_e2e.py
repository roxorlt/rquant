"""#271: what `notifier.admin.shadow.v1` costs the host when it has nothing to do.

Production samples this role at 26 writes/s and about 150 KB/s while the market is shut
and its outbox is empty. Two things produced that, both in its two-second loop and both
independent of what the loop was given to do:

* every iteration INSERTed a page projection authority row, because the row's id was
  hashed over `observed_at` -- the notifier's own clock -- so the lookup that makes the
  publish idempotent never matched its own previous row; and
* every iteration rewrote the replication cursor, because `updated_at` was the same clock.

Both are `BEGIN IMMEDIATE ... COMMIT` on a `journal_mode=WAL`, `synchronous=FULL`
database, so each was a real fsync. This drives the built notifier -- the same
`notifier_builder` the production manifest is handed to -- for sixty idle iterations and
asks the database file whether anything was committed at all.
"""

from __future__ import annotations

import os
import sqlite3
from datetime import timedelta
from pathlib import Path

import duckdb

from rquant.delivery_contracts import DeliveryChannel
from rquant.runtime_builder_signal import notifier_builder
from tests.unit.test_runtime_builder_signal import (
    NOW,
    _notifier_manifest,
    _page_projection_replica,
    _Provider,
    _seed_outbox,
)

#: Two seconds, the production interval of `notifier.admin.shadow.v1`.
INTERVAL = timedelta(seconds=2)
IDLE_ITERATIONS = 60


def _state_stamp(database: Path) -> tuple[object, ...]:
    """Everything that moves when this database is committed to.

    The **main file's** size and mtime are the commit evidence. The store opens and
    closes a connection per call, so every commit is checkpointed into this file before
    the call returns, and a file whose mtime has not moved is a database nothing was
    committed to. The `-wal` is deliberately not looked at: opening a write connection
    creates and truncates it whether or not that connection goes on to commit, so its
    mtime moves on an iteration that wrote nothing at all. `total_changes` is no use
    either -- it counts what *one* connection did -- and `/proc/self/io` `syscw` is
    Linux-only, so this runs on both lanes. The row counts and the cursor row come with
    it because a row rewritten in place moves no count, and the point is to catch both.
    """

    connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
    try:
        counts = tuple(
            int(connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
            for table in (
                "notification_projection_authority",
                "notification_replication_source",
                "signal_envelope",
                "delivery_outbox",
                "delivery_attempt",
            )
        )
        cursor_row = connection.execute(
            "SELECT * FROM notification_replication_source WHERE singleton = 1"
        ).fetchone()
    finally:
        connection.close()
    observed = database.stat()
    return (
        (observed.st_size, observed.st_mtime_ns),
        counts,
        tuple(cursor_row) if cursor_row else None,
    )


def _projection_generations(database: Path) -> tuple[str, ...]:
    connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
    try:
        return tuple(
            str(row[0])
            for row in connection.execute(
                "SELECT generation_id FROM notification_projection_authority "
                "ORDER BY observed_at"
            ).fetchall()
        )
    finally:
        connection.close()


def test_an_idle_notifier_commits_nothing_and_one_change_commits_once(
    tmp_path: Path,
) -> None:
    """Sixty idle iterations, zero commits; one changed projection, exactly one row."""

    _seed_outbox(tmp_path)
    replica = _page_projection_replica(tmp_path, synced_at=NOW - timedelta(minutes=1))
    state = tmp_path / "notification-state.sqlite3"
    clock = NOW
    step = notifier_builder(
        provider_loader=lambda: {DeliveryChannel.PUSHDEER: _Provider()},
        clock=lambda: clock,
    )(
        _notifier_manifest(
            tmp_path,
            serving_authority_root=str((tmp_path / "serving-signals").resolve()),
            page_projection_database_path=str(replica),
        )
    )

    # Two iterations to drain what the seed put in the outbox, so what follows is a
    # notifier with genuinely nothing to do -- which is the whole of its day outside the
    # session, and most of its day inside one.
    settle = [step(), step()]
    assert [result.projection_published for result in settle] == [True, False]

    before = _state_stamp(state)
    idle = []
    for index in range(IDLE_ITERATIONS):
        clock = NOW + INTERVAL * (index + 1)
        idle.append(step())
    after = _state_stamp(state)

    assert after == before, "an idle notifier committed to its state database"
    assert [result.projection_published for result in idle] == [False] * IDLE_ITERATIONS
    assert [result.processed_count for result in idle] == [0] * IDLE_ITERATIONS
    assert len(_projection_generations(state)) == 1

    # Now the projection actually changes: a new replica generation, and a clock past the
    # fifteen-minute floor so the role is allowed to open it.
    connection = duckdb.connect(str(replica))
    try:
        connection.execute(
            """
            INSERT INTO screen_result VALUES
              ('2026-07-31', 'n-shape-pool2', '600519.SH', 'MT', 1680.0, 2.5, '{}',
               '2026-07-31 10:06:00');
            CHECKPOINT;
            """
        )
    finally:
        connection.close()
    synced_at = (NOW + timedelta(minutes=20)).timestamp()
    os.utime(replica, (synced_at, synced_at))

    clock = NOW + timedelta(minutes=21)
    changed = step()
    clock = NOW + timedelta(minutes=21) + INTERVAL
    again = step()

    generations = _projection_generations(state)

    assert changed.projection_published is True
    assert again.projection_published is False
    assert len(generations) == 2
    assert generations[0] != generations[1]
