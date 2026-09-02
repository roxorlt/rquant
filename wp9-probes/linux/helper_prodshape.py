"""WP9 v4.1 probe helper: the heartbeat helper in **exactly the production
shape** frozen by design §3.1/§3.3 — three fds and nothing else.

    python -I helper_prodshape.py --control-fd N --status-fd M --stop-fd K

No LIVE_W, no fd-inventory frame: this file exists to prove that T9's new
acceptance criteria (V4-M2) work against the helper the design actually
specifies, not against a probe-only variant.

Protocol: newline-delimited JSON.  One ``config`` frame in, ``ready`` out, then
a heartbeat every ``interval_seconds`` until the stop pipe reaches EOF.
"""

import json
import os
import select
import signal
import sqlite3
import sys
import time

BUF = b""


def send(fd, frame):
    os.write(fd, (json.dumps(frame, separators=(",", ":")) + "\n").encode("utf-8"))


def read_frame(fd):
    global BUF
    while b"\n" not in BUF:
        chunk = os.read(fd, 65536)
        if not chunk:
            return None
        BUF += chunk
    line, _, BUF = BUF.partition(b"\n")
    return json.loads(line)


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


def parse_args(argv):
    out = {}
    for index in range(0, len(argv), 2):
        out[argv[index].lstrip("-").replace("-", "_")] = argv[index + 1]
    return out


def main():
    args = parse_args(sys.argv[1:])
    ctrl_r = int(args["control_fd"])
    status_w = int(args["status_fd"])
    stop_r = int(args["stop_fd"])
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    config = read_frame(ctrl_r)
    if config is None:
        return 0
    send(status_w, {"t": "ready", "protocol": 1, "pid": os.getpid()})

    db = config["db"]
    operation_id = config["operation_id"]
    busy_ms = int(config["busy_timeout_ms"])
    interval = float(config["interval_seconds"])
    ticks = 0
    while True:
        readable, _, _ = select.select([stop_r, ctrl_r], [], [], interval)
        if stop_r in readable and os.read(stop_r, 4096) == b"":
            return 0
        if ctrl_r in readable and read_frame(ctrl_r) is None:
            return 0
        if not readable:
            heartbeat_once(db, operation_id, busy_ms)
            ticks += 1
            try:
                os.write(status_w, (json.dumps({"t": "tick", "n": ticks}) + "\n").encode())
            except BlockingIOError:
                pass


if __name__ == "__main__":
    sys.exit(main())
