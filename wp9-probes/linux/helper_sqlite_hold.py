"""WP9 v4 probe helper: reproduce the ``_heartbeat_outbox`` write shape and stop
inside it while holding the SQLite write lock.

Connection shape mirrors ``src/rquant/source_broker_v2.py:3647-3658``
(``timeout=busy_ms/1000``, ``isolation_level=None``, foreign_keys / busy_timeout
/ synchronous=FULL) and the UPDATE mirrors ``:4296-4308``.

``python -I helper_sqlite_hold.py DB READY_W BUSY_MS MODE``
MODE ``after-update``  stop between UPDATE and commit (durable tail, lock held)
MODE ``after-commit``  stop after commit, before close
"""

import os
import sqlite3
import sys
import time

OPERATION_ID = "wp9-probe-op"
OWNER_TOKEN = "wp9-probe-owner"
GENERATION = 1


def main():
    db = sys.argv[1]
    ready_w = int(sys.argv[2])
    busy_ms = int(sys.argv[3])
    mode = sys.argv[4]

    connection = sqlite3.connect(db, timeout=busy_ms / 1_000, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute(f"PRAGMA busy_timeout = {busy_ms}")
    connection.execute("PRAGMA synchronous = FULL")
    connection.execute("BEGIN IMMEDIATE")
    updated = connection.execute(
        "UPDATE outbox SET executor_heartbeat_at = ?, executor_lease_expires_at = ? "
        "WHERE operation_id = ? AND status = 'pending' AND executor_owner_token = ? "
        "AND executor_generation = ?",
        ("CHILD-WROTE", "CHILD-WROTE", OPERATION_ID, OWNER_TOKEN, GENERATION),
    ).rowcount
    if updated != 1:
        os.write(ready_w, b"badrowcount\n")
        return 2
    if mode == "after-commit":
        connection.commit()
    os.write(ready_w, b"held\n")
    deadline = time.monotonic() + 120.0
    while time.monotonic() < deadline:
        time.sleep(0.05)
    return 0


if __name__ == "__main__":
    sys.exit(main())
