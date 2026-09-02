"""Fault-injecting stand-in for the production outbox heartbeat helper.

Started the same way the real one is - `sys.executable -I <this file>
--control-fd N --status-fd M --stop-fd K` - because the only thing that crosses
the process boundary is a command line, so a case can substitute a helper by
substituting one argv and nothing else.  It speaks the same frame protocol.

Three constraints, all of them load bearing:

* **Single file and stdlib only.**  `-I` keeps the script's directory out of
  `sys.path`, so this file cannot import its siblings, and it deliberately does
  not import `rquant` either: a fault helper with a smaller capability surface
  than the thing it stands in for is the right way round.  The digest below is
  therefore a second implementation of `stable_row_digest`, and
  `test_wp9_fault_helper_digest_matches_the_production_digest` pins the two
  together so the copy cannot drift.
* **Configuration arrives on the control pipe, exactly as in production.**  The
  only additions to argv are `--fault` and `--fault-marker`, and neither is a
  secret: a fault name and a path a case just created.  The database path and
  the owner token stay in the config frame, where `ps` cannot read them.
* **`python_files = ["test_*.py"]`**, so pytest never collects this module even
  though it lives under `tests/unit`.

The stalling faults block on a real `flock` the case holds, not on a sleep, so
what the shutdown has to survive is a genuine uninterruptible wait rather than
a timer someone could argue about.  The marker file is how a case learns the
helper has arrived: it is created with `open(path, "x")` immediately *before*
the blocking call, so its existence means "already there and now stuck".  Every
branch must be given a path of its own - the exclusive create is what makes the
signal unambiguous, and a leftover file from a previous parametrisation would
either release a case early or fail the helper outright.  A `FileExistsError`
here is a hard failure, never ignored.
"""

from __future__ import annotations

import argparse
import errno
import fcntl
import hashlib
import json
import os
import select
import signal
import sqlite3
import struct
import sys
import time
from datetime import UTC, datetime, timedelta
from typing import Any

PROTOCOL_VERSION = 1
IDLE_POLL_SECONDS = 1.0
DIGEST_EXCLUDED_COLUMNS = ("executor_heartbeat_at", "executor_lease_expires_at")

SELECT_SQL = "SELECT * FROM source_broker_v2_outbox WHERE operation_id = ?"
UPDATE_SQL = (
    "UPDATE source_broker_v2_outbox SET executor_heartbeat_at = ?, "
    "executor_lease_expires_at = ? WHERE operation_id = ? "
    "AND status = 'pending' AND executor_owner_token = ? "
    "AND executor_generation = ?"
)
# The column the self-healing fault moves and then puts back.  A plain string
# column that the helper-side structural checks accept unchanged, so the only
# thing that notices is the digest.
TAMPER_COLUMN = "payload_hash"

FAULT_MODES = (
    "exit-immediately",
    "no-ready",
    "ready-wrong-pid",
    "ready-wrong-protocol",
    "ready-garbage",
    "no-ack",
    "exit-after-ack",
    "fail-renewal",
    "stall-before-connect",
    "stall-before-commit",
    "stall-after-commit",
    "tamper-self-healing",
)


def stable_row_digest(row: sqlite3.Row, description: Any) -> str:
    """A second implementation of the production digest, pinned by a test."""

    digest = hashlib.sha256()
    for index, column in enumerate(description):
        if column[0] in DIGEST_EXCLUDED_COLUMNS:
            continue
        value = row[index]
        if value is None:
            payload = b"N"
        elif isinstance(value, bool):
            payload = b"I" + str(int(value)).encode("ascii")
        elif isinstance(value, int):
            payload = b"I" + str(value).encode("ascii")
        elif isinstance(value, float):
            payload = b"F" + repr(value).encode("ascii")
        elif isinstance(value, str):
            payload = b"S" + value.encode("utf-8")
        else:
            payload = b"B" + bytes(value)
        digest.update(struct.pack("!I", len(payload)))
        digest.update(payload)
    return digest.hexdigest()


def _encode(frame: dict[str, Any]) -> bytes:
    return json.dumps(frame, allow_nan=False, separators=(",", ":")).encode("utf-8") + b"\n"


def _write(fd: int, frame: dict[str, Any]) -> bool:
    payload = _encode(frame)
    while payload:
        try:
            payload = payload[os.write(fd, payload) :]
        except OSError:
            return False
    return True


class _Reader:
    def __init__(self, fd: int) -> None:
        self._fd = fd
        self._buffer = b""
        self.eof = False

    def take(self) -> dict[str, Any] | None:
        line, separator, rest = self._buffer.partition(b"\n")
        if not separator:
            return None
        self._buffer = rest
        return json.loads(line.decode("utf-8"))

    def fill(self) -> bool:
        try:
            chunk = os.read(self._fd, 65536)
        except OSError as exc:
            if exc.errno in {errno.EAGAIN, errno.EWOULDBLOCK}:
                return True
            raise
        if not chunk:
            self.eof = True
            return False
        self._buffer += chunk
        return True

    @property
    def at_eof(self) -> bool:
        return self.eof and not self._buffer


def _arrive_and_block(marker: str) -> None:
    """Announce arrival, then wait on a lock the case holds.  Never returns.

    The order matters: the marker is created first and exclusively, so a case
    that sees it knows the next thing this process did was block.  `flock` on a
    file another process holds `LOCK_EX` on is a real uninterruptible wait, not
    a sleep with a number in it.
    """

    handle = os.open(marker + ".lock", os.O_RDWR | os.O_CREAT, 0o600)
    descriptor = os.open(marker, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.close(descriptor)
    fcntl.flock(handle, fcntl.LOCK_EX)
    # Only reachable if the case released the lock, which no case does; the
    # helper is meant to be killed here.
    while True:
        time.sleep(3600)


def _connect(config: dict[str, Any]) -> sqlite3.Connection:
    connection = sqlite3.connect(
        str(config["db"]),
        timeout=int(config["busy_timeout_ms"]) / 1_000,
        isolation_level=None,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute(f"PRAGMA busy_timeout = {int(config['busy_timeout_ms'])}")
    connection.execute("PRAGMA synchronous = FULL")
    return connection


def _renew(
    config: dict[str, Any],
    session: dict[str, Any],
    *,
    stop_before_commit: bool = False,
) -> tuple[str, sqlite3.Connection]:
    """One real renewal.  Returns the row digest and the open connection."""

    now = datetime.now(UTC)
    expires = now + timedelta(seconds=float(config["lease_seconds"]))
    connection = _connect(config)
    connection.execute("BEGIN IMMEDIATE")
    try:
        cursor = connection.execute(SELECT_SQL, (session["operation_id"],))
        row = cursor.fetchone()
        if row is None:
            raise sqlite3.OperationalError("required outbox effect is missing")
        digest = stable_row_digest(row, cursor.description)
        updated = connection.execute(
            UPDATE_SQL,
            (
                now.isoformat(),
                expires.isoformat(),
                session["operation_id"],
                str(config["owner_token"]),
                session["owner_generation"],
            ),
        ).rowcount
        if updated != 1:
            raise sqlite3.OperationalError("outbox executor lost ownership before heartbeat")
        if stop_before_commit:
            # Left open on purpose: the write lock is held and no commit
            # record exists, which is the durable-tail window.
            return digest, connection
        connection.commit()
    except BaseException:
        connection.rollback()
        connection.close()
        raise
    return digest, connection


def _tamper(config: dict[str, Any], session: dict[str, Any], value: str) -> None:
    connection = _connect(config)
    try:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            f"UPDATE source_broker_v2_outbox SET {TAMPER_COLUMN} = ? WHERE operation_id = ?",
            (value, session["operation_id"]),
        )
        connection.commit()
    finally:
        connection.close()


def _original_column(config: dict[str, Any], session: dict[str, Any]) -> str:
    connection = _connect(config)
    try:
        row = connection.execute(
            f"SELECT {TAMPER_COLUMN} FROM source_broker_v2_outbox WHERE operation_id = ?",
            (session["operation_id"],),
        ).fetchone()
    finally:
        connection.close()
    return str(row[0])


def _emit_tick(status_fd: int, session: dict[str, Any], digest: str) -> None:
    session["ticks"] += 1
    if session["first_digest"] is None:
        session["first_digest"] = digest
    session["last_digest"] = digest
    if digest != session["first_digest"]:
        session["digest_changed"] = True
    _write(status_fd, {"t": "tick", "n": session["ticks"], "stage": "commit", "digest": digest})


def _end_ack(session: dict[str, Any] | None, token: Any) -> dict[str, Any]:
    if session is None or session["token"] != token:
        return {
            "t": "end-ack",
            "token": token if isinstance(token, str) else "",
            "ticks": 0,
            "last_stage": "idle",
            "last_outcome": "unknown-token",
            "first_digest": None,
            "last_digest": None,
            "digest_changed": False,
        }
    return {
        "t": "end-ack",
        "token": session["token"],
        "ticks": session["ticks"],
        "last_stage": "commit" if session["ticks"] else "idle",
        "last_outcome": session["outcome"],
        "first_digest": session["first_digest"],
        "last_digest": session["last_digest"],
        "digest_changed": session["digest_changed"],
    }


def _new_session(frame: dict[str, Any]) -> dict[str, Any]:
    return {
        "token": frame["token"],
        "phase": frame["phase"],
        "operation_id": frame["operation_id"],
        "owner_generation": frame["owner_generation"],
        "interval": float(frame["interval_seconds"]),
        "open": True,
        "ticks": 0,
        "first_digest": None,
        "last_digest": None,
        "digest_changed": False,
        "outcome": "ok",
    }


def _run_self_healing_tamper(
    config: dict[str, Any],
    session: dict[str, Any],
    status_fd: int,
    marker: str,
) -> None:
    """Sample, move a column, sample, put it back, sample.

    Ordered entirely inside this process.  Nothing here waits to be told what
    to do next, so the interleaving is a property of this function rather than
    of two processes racing, and the case only has to wait for the marker that
    says the whole sequence is done.
    """

    original = _original_column(config, session)
    digest, connection = _renew(config, session)
    connection.close()
    _emit_tick(status_fd, session, digest)
    _tamper(config, session, "f" * 64)
    digest, connection = _renew(config, session)
    connection.close()
    _emit_tick(status_fd, session, digest)
    _tamper(config, session, original)
    digest, connection = _renew(config, session)
    connection.close()
    _emit_tick(status_fd, session, digest)
    descriptor = os.open(marker, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.close(descriptor)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--control-fd", type=int, required=True)
    parser.add_argument("--status-fd", type=int, required=True)
    parser.add_argument("--stop-fd", type=int, required=True)
    parser.add_argument("--fault", required=True, choices=FAULT_MODES)
    parser.add_argument("--fault-marker", default=None)
    args = parser.parse_args(argv)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    fault = args.fault
    if fault == "exit-immediately":
        return 0
    control_fd, status_fd, stop_fd = args.control_fd, args.status_fd, args.stop_fd
    reader = _Reader(control_fd)
    while True:
        frame = reader.take()
        if frame is not None:
            break
        if not reader.fill():
            return 2
    config = frame
    if fault == "ready-garbage":
        os.write(status_fd, b"this is not a frame\n")
    elif fault == "ready-wrong-protocol":
        _write(status_fd, {"t": "ready", "protocol": 999, "pid": os.getpid()})
    elif fault == "ready-wrong-pid":
        _write(status_fd, {"t": "ready", "protocol": PROTOCOL_VERSION, "pid": os.getpid() + 1})
    elif fault != "no-ready":
        _write(status_fd, {"t": "ready", "protocol": PROTOCOL_VERSION, "pid": os.getpid()})

    session: dict[str, Any] | None = None
    next_tick_at = 0.0
    while True:
        now = time.monotonic()
        if session is not None and session["open"]:
            timeout = max(0.0, next_tick_at - now)
        else:
            timeout = IDLE_POLL_SECONDS
        ready, _, _ = select.select([control_fd, stop_fd], [], [], timeout)
        if stop_fd in ready:
            return 0
        if control_fd in ready:
            reader.fill()
            if reader.at_eof:
                return 0
        while True:
            frame = reader.take()
            if frame is None:
                break
            if frame["t"] == "session":
                session = _new_session(frame)
                next_tick_at = time.monotonic() + session["interval"]
                if fault == "no-ack":
                    continue
                _write(status_fd, {"t": "session-ack", "token": session["token"]})
                if fault == "exit-after-ack":
                    return 0
                if fault == "tamper-self-healing":
                    assert args.fault_marker is not None
                    _run_self_healing_tamper(config, session, status_fd, args.fault_marker)
            elif frame["t"] == "end":
                _write(status_fd, _end_ack(session, frame.get("token")))
                session = None
        if session is None or not session["open"] or time.monotonic() < next_tick_at:
            continue
        if fault == "fail-renewal":
            session["open"] = False
            session["outcome"] = "failed"
            _write(
                status_fd,
                {
                    "t": "failed",
                    "token": session["token"],
                    "errorcode": sqlite3.SQLITE_BUSY,
                    "type": "OperationalError",
                    "detail": "injected renewal failure",
                },
            )
            continue
        if fault == "stall-before-connect":
            assert args.fault_marker is not None
            _arrive_and_block(args.fault_marker)
        if fault == "stall-before-commit":
            digest, connection = _renew(config, session, stop_before_commit=True)
            assert args.fault_marker is not None
            _arrive_and_block(args.fault_marker)
        if fault == "stall-after-commit":
            digest, connection = _renew(config, session)
            _emit_tick(status_fd, session, digest)
            assert args.fault_marker is not None
            # Still holding the committed connection, so the stall is inside
            # the window where `close` would run its passive checkpoint.
            _arrive_and_block(args.fault_marker)
        digest, connection = _renew(config, session)
        connection.close()
        _emit_tick(status_fd, session, digest)
        next_tick_at = time.monotonic() + session["interval"]


if __name__ == "__main__":
    sys.exit(main())
