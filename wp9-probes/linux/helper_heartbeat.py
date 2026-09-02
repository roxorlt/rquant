"""WP9 v4 probe helper: the long-lived heartbeat helper of the P1-05 three-layer
parent-death test.

``python -I helper_heartbeat.py DB OP_ID INTERVAL STATUS_W STOP_R LIVE_W BUSY_MS``

* STOP_R  read end of the stop pipe; the *only* writer is the helper-parent, so
  the helper-parent's death closes it and this process sees EOF.
* LIVE_W  write end of a liveness pipe whose only holder is this process; the
  supervisor watches its read end and therefore learns of this process's exit
  **without any pid-reuse assumption**.
"""

import fcntl
import json
import os
import select
import signal
import sqlite3
import sys
import time


def fd_inventory():
    """(fd, access mode, st_ino) for every open fd, using only fcntl+fstat."""
    rows = []
    for fd in range(0, 64):
        try:
            flags = fcntl.fcntl(fd, fcntl.F_GETFL)
            info = os.fstat(fd)
        except OSError:
            continue
        accmode = flags & os.O_ACCMODE
        rows.append(
            {
                "fd": fd,
                "accmode": {os.O_RDONLY: "r", os.O_WRONLY: "w", os.O_RDWR: "rw"}.get(
                    accmode, str(accmode)
                ),
                "ino": info.st_ino,
                "dev": info.st_dev,
            }
        )
    return rows


def heartbeat_once(db, operation_id, busy_ms):
    connection = sqlite3.connect(db, timeout=busy_ms / 1_000, isolation_level=None)
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(f"PRAGMA busy_timeout = {busy_ms}")
        connection.execute("PRAGMA synchronous = FULL")
        connection.execute("BEGIN IMMEDIATE")
        try:
            stamp = f"{time.time():.6f}"
            connection.execute(
                "UPDATE outbox SET executor_heartbeat_at = ?, executor_lease_expires_at = ? "
                "WHERE operation_id = ? AND status = 'pending'",
                (stamp, stamp, operation_id),
            )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
    finally:
        connection.close()


def main():
    db = sys.argv[1]
    operation_id = sys.argv[2]
    interval = float(sys.argv[3])
    status_w = int(sys.argv[4])
    stop_r = int(sys.argv[5])
    live_w = int(sys.argv[6])
    busy_ms = int(sys.argv[7])
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    stop_ino = os.fstat(stop_r).st_ino
    ready = {
        "type": "ready",
        "pid": os.getpid(),
        "stop_r": stop_r,
        "stop_ino": stop_ino,
        "fds": fd_inventory(),
    }
    os.write(status_w, (json.dumps(ready) + "\n").encode("utf-8"))

    while True:
        readable, _, _ = select.select([stop_r], [], [], interval)
        if readable:
            if os.read(stop_r, 4096) == b"":
                return 0
            continue
        heartbeat_once(db, operation_id, busy_ms)


if __name__ == "__main__":
    sys.exit(main())
