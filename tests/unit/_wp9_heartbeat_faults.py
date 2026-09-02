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
a timer someone could argue about.  They also ignore SIGTERM, which is what
makes a case walk the escalation all the way down to SIGKILL - and which is why
the stall carries a deadline of its own (`--stall-seconds`): a run whose
SIGKILL never arrives, exactly what a mutation of the escalation produces,
would otherwise leave a signal-proof process on the machine indefinitely.

The marker file is how a case learns the helper has arrived: it is created with
`open(path, "x")` immediately *before* the blocking call, so its existence
means "already there and now stuck". Every branch must be given a path of its
own - the exclusive create is what makes the signal unambiguous, and a leftover
file from a previous parametrisation would either release a case early or fail
the helper outright. A `FileExistsError` here is a hard failure, never ignored.
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
# How long a stalling fault stays stuck before removing itself.  Far longer
# than any case needs - every one of them kills this process within a couple of
# seconds - and short enough that a mutation which breaks the escalation cannot
# leave a SIGTERM-ignoring process on a developer's machine for an hour.
DEFAULT_STALL_SECONDS = 120.0
# The busy_timeout the foreign-lock fault opens its own connection with.  It has
# to be far longer than any shutdown budget a case configures, so that the wait
# ending by itself is never the reason the shutdown ended.
_FOREIGN_LOCK_TIMEOUT_SECONDS = 60.0
_STALL_DEADLINE_EXIT_CODE = 70
_GO_MARKER_TIMEOUT_SECONDS = 60.0
DIGEST_EXCLUDED_COLUMNS = ("executor_heartbeat_at", "executor_lease_expires_at")

SELECT_SQL = "SELECT * FROM source_broker_v2_outbox WHERE operation_id = ?"
UPDATE_SQL = (
    "UPDATE source_broker_v2_outbox SET executor_heartbeat_at = ?, "
    "executor_lease_expires_at = ? WHERE operation_id = ? "
    "AND status = 'pending' AND executor_owner_token = ? "
    "AND executor_generation = ?"
)
# Faults whose whole point is that the process cannot be asked to leave.  A
# helper that honours SIGTERM exits at the first escalation step, which would
# leave the SIGKILL stage untested - and that stage is the only reason a stuck
# renewal has a bound at all.  Ignoring SIGTERM here is what makes the case go
# all the way down the chain.
_SIGTERM_IGNORING_FAULTS = (
    "stall-before-connect",
    "stall-before-commit",
    "stall-after-commit",
    "stall-in-foreign-lock-wait",
)

# Faults that act during a renewal rather than during the handshake, and so
# must wait for the case to open the window first.
_GATED_FAULTS = (
    "fail-renewal",
    "stall-before-connect",
    "stall-before-commit",
    "stall-after-commit",
    "stall-in-foreign-lock-wait",
    "pin-in-lock-wait",
)

FAULT_MODES = (
    "exit-immediately",
    "no-ready",
    "ready-wrong-pid",
    "ready-wrong-protocol",
    "ready-garbage",
    "no-ack",
    "exit-after-ack",
    "fail-renewal",
    "never-renews",
    "pin-in-lock-wait",
    "stall-before-connect",
    "stall-before-commit",
    "stall-after-commit",
    "stall-in-foreign-lock-wait",
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


def _wait_for_go(marker: str) -> None:
    """Block until the case says the window is open.

    Every tick-time fault waits here first.  The parent runs one synchronous,
    owner-guarded renewal between the session acknowledgement and the
    invocation, and a fault that fired before that renewal would be racing it
    rather than testing anything.  The case creates ``<marker>.go`` from inside
    its own invocation, so the fault lands strictly inside the window.
    """

    deadline = time.monotonic() + _GO_MARKER_TIMEOUT_SECONDS
    while not os.path.exists(marker + ".go"):
        if time.monotonic() > deadline:
            raise RuntimeError("fault helper waited for its go marker and never got one")
        time.sleep(0.002)


def _announce(marker: str) -> None:
    """Exclusive create: the case is watching for exactly this file."""

    descriptor = os.open(marker, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.close(descriptor)


def _arrive_and_block(marker: str, stall_seconds: float) -> None:
    """Announce arrival, then wait on a lock the case holds.  Then give up.

    The order matters: the marker is created first and exclusively, so a case
    that sees it knows the next thing this process did was block.  `flock` on a
    file another process holds `LOCK_EX` on is a real uninterruptible wait, not
    a sleep with a number in it.

    The deadline is housekeeping, not part of what is under test.  These faults
    also ignore SIGTERM - that is what makes the case walk the whole escalation
    down to SIGKILL - so a run whose SIGKILL never arrives, which is exactly
    what a mutation of the escalation produces, would otherwise leave this
    process on the machine indefinitely.

    It has to be a timer rather than a check after the wait: the wait is the
    whole point and it does not return.  `SIGALRM` reaches a process blocked in
    `flock`, and the handler leaves through `os._exit` rather than returning,
    because returning would let PEP 475 restart the interrupted call.
    """

    handle = os.open(marker + ".lock", os.O_RDWR | os.O_CREAT, 0o600)

    def give_up(_signal: int, _frame: object) -> None:
        os._exit(_STALL_DEADLINE_EXIT_CODE)

    signal.signal(signal.SIGALRM, give_up)
    signal.setitimer(signal.ITIMER_REAL, stall_seconds)
    _announce(marker)
    fcntl.flock(handle, fcntl.LOCK_EX)
    # Only reachable if the case released the lock, which no case does; the
    # helper is meant to be killed while it is in the call above.
    while True:
        time.sleep(1.0)


def _block_on_a_foreign_write_lock(marker: str, stall_seconds: float) -> None:
    """Wait for a write lock on a database that is not the saga's.  Never returns.

    The point is a lock wait whose window is decided by *somebody else's*
    connection: this one is opened with a `busy_timeout` far longer than the
    saga's shutdown budget, so the caller's bound cannot be the lock wait
    ending on its own.  Only the kill can end it.

    Same deadline discipline as the flock stalls, and for the same reason -
    this fault ignores SIGTERM.
    """

    connection = sqlite3.connect(
        marker + ".contended",
        timeout=_FOREIGN_LOCK_TIMEOUT_SECONDS,
        isolation_level=None,
    )
    connection.execute(f"PRAGMA busy_timeout = {int(_FOREIGN_LOCK_TIMEOUT_SECONDS * 1000)}")

    def give_up(_signal: int, _frame: object) -> None:
        os._exit(_STALL_DEADLINE_EXIT_CODE)

    signal.signal(signal.SIGALRM, give_up)
    signal.setitimer(signal.ITIMER_REAL, stall_seconds)
    _announce(marker)
    connection.execute("BEGIN IMMEDIATE")
    while True:
        time.sleep(1.0)


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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--control-fd", type=int, required=True)
    parser.add_argument("--status-fd", type=int, required=True)
    parser.add_argument("--stop-fd", type=int, required=True)
    parser.add_argument("--fault", required=True, choices=FAULT_MODES)
    parser.add_argument("--fault-marker", default=None)
    parser.add_argument("--stall-seconds", type=float, default=DEFAULT_STALL_SECONDS)
    args = parser.parse_args(argv)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    fault = args.fault
    if fault in _SIGTERM_IGNORING_FAULTS:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
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
    # Every gated fault fires exactly once and then gets out of the way, so a
    # case can follow "fail this renewal" with "and now serve the next session
    # normally" on the same long-lived process.
    fired: set[str] = set()
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
                    # Models a session frame that reached a buffer nobody
                    # processes: no acknowledgement, and no renewals either.
                    session["open"] = False
                    continue
                _write(status_fd, {"t": "session-ack", "token": session["token"]})
                if fault == "exit-after-ack":
                    return 0
            elif frame["t"] == "end":
                _write(status_fd, _end_ack(session, frame.get("token")))
                session = None
        if session is None or not session["open"] or time.monotonic() < next_tick_at:
            continue
        if fault in _GATED_FAULTS and fault in fired:
            fault_now = None
        elif fault in _GATED_FAULTS:
            assert args.fault_marker is not None
            _wait_for_go(args.fault_marker)
            fired.add(fault)
            fault_now = fault
        else:
            fault_now = fault
        if fault_now == "never-renews":
            # Protocol-correct and completely idle: the session is
            # acknowledged and the end frame will be answered, but this helper
            # never touches the database.  That hands the renewal schedule back
            # to the case, which is what the direct-call migrations need.
            session["open"] = False
            continue
        if fault_now == "stall-in-foreign-lock-wait":
            assert args.fault_marker is not None
            _block_on_a_foreign_write_lock(args.fault_marker, args.stall_seconds)
        if fault_now == "pin-in-lock-wait":
            # Announce first, then run a real renewal that will sit in
            # `BEGIN IMMEDIATE` behind the lock the case is holding.  Unlike
            # the stalls this one is meant to finish once the case lets go.
            assert args.fault_marker is not None
            _announce(args.fault_marker)
        if fault_now == "fail-renewal":
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
            assert args.fault_marker is not None
            _announce(args.fault_marker)
            continue
        if fault_now == "stall-before-connect":
            assert args.fault_marker is not None
            _arrive_and_block(args.fault_marker, args.stall_seconds)
        if fault_now == "stall-before-commit":
            digest, connection = _renew(config, session, stop_before_commit=True)
            assert args.fault_marker is not None
            _arrive_and_block(args.fault_marker, args.stall_seconds)
        if fault_now == "stall-after-commit":
            digest, connection = _renew(config, session)
            _emit_tick(status_fd, session, digest)
            assert args.fault_marker is not None
            # Still holding the committed connection, so the stall is inside
            # the window where `close` would run its passive checkpoint.
            _arrive_and_block(args.fault_marker, args.stall_seconds)
        digest, connection = _renew(config, session)
        connection.close()
        _emit_tick(status_fd, session, digest)
        next_tick_at = time.monotonic() + session["interval"]


if __name__ == "__main__":
    sys.exit(main())
