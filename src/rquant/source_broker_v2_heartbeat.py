"""Outbox heartbeat renewal, isolated in a process a shutdown can kill.

CPython cannot end a thread, and the three windows a heartbeat can be inside -
``sqlite3.connect``, ``Connection.close``'s passive checkpoint, and the
``synchronous = FULL`` fsync behind a commit - are blocking C calls that no
interrupt, signal or asynchronous exception reaches.  A caller that waits for
such a thread therefore has either no bound (an untimed ``join``) or a live
heartbeat left behind (a timed one).  A separate process breaks that: after
``SIGKILL`` it executes no further bytecode, whatever it was inside, so the
shutdown is bounded *and* leaves nothing renewing.

This module is that process, and it is deliberately the whole of it.  It is
stdlib-only and imports no other ``rquant`` module: the helper's address space
therefore contains no transport, quota, authority, lineage, config or logging
code at all, which is why "the heartbeat makes no external call" is structural
here rather than promised.  ``source_broker_v2`` imports *this* module rather
than the other way round, so the renewal SQL, the connection pragmas and the
window digest are one object each, shared by the in-process synchronous
validation points and by the helper.

Nothing here writes to stdout or stderr - the parent runs it with all three
standard streams on ``/dev/null`` and reads its state from status frames.
"""

from __future__ import annotations

import argparse
import errno
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

HEARTBEAT_PROTOCOL_VERSION = 1

# The renewal statement and its guard, byte for byte as ``_heartbeat_outbox``
# has always run them.  Both the in-process synchronous renewals and the helper
# execute *this* object; a second copy anywhere in ``src/`` is what the
# single-source test forbids, because two copies can drift apart and the owner
# guard is the whole of the lease's fencing.
HEARTBEAT_SELECT_SQL = "SELECT * FROM source_broker_v2_outbox WHERE operation_id = ?"
HEARTBEAT_UPDATE_SQL = (
    "UPDATE source_broker_v2_outbox SET executor_heartbeat_at = ?, "
    "executor_lease_expires_at = ? WHERE operation_id = ? "
    "AND status = 'pending' AND executor_owner_token = ? "
    "AND executor_generation = ?"
)

# The two columns a renewal is *supposed* to move.  Everything else in the row
# has to hold still for the length of one invocation, which is what the digest
# below samples.
DIGEST_EXCLUDED_COLUMNS = ("executor_heartbeat_at", "executor_lease_expires_at")

MAX_FRAME_BYTES = 64 * 1024
_IDLE_POLL_SECONDS = 1.0
_MAX_DETAIL_CHARS = 1000

# Digest type tags.  A length prefix precedes each column so no concatenation
# of two rows can produce the bytes of a third.
_NULL_TAG = b"N"
_INT_TAG = b"I"
_FLOAT_TAG = b"F"
_TEXT_TAG = b"S"
_BLOB_TAG = b"B"


class HeartbeatProtocolError(Exception):
    """A frame was malformed, oversized, or not the one the reader expected."""


def open_saga_connection(path: str, busy_timeout_ms: int) -> sqlite3.Connection:
    """Open the saga database with the one connection shape this system uses.

    Every saga connection - schema init, the synchronous validation points, the
    lease writes and this helper - goes through here, so ``busy_timeout`` (the
    only tolerance any of them states for one SQLite operation on this file)
    and ``synchronous = FULL`` (which is what makes a committed renewal
    survivable) cannot be set in one place and forgotten in another.
    """

    connection = sqlite3.connect(path, timeout=busy_timeout_ms / 1_000, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute(f"PRAGMA busy_timeout = {busy_timeout_ms}")
    connection.execute("PRAGMA synchronous = FULL")
    return connection


def stable_row_digest(row: sqlite3.Row, description: Any) -> str:
    """Hash the columns that must not move while one invocation runs.

    Column order comes from ``cursor.description`` rather than from a literal
    list: parent and helper read the same database file, so ``SELECT *`` hands
    both the same order, and a schema this function does not know about still
    hashes deterministically.  Values are tagged by SQLite storage class and
    length-prefixed, so ``"1"`` and ``1``, or ``"ab" + "c"`` and ``"a" + "bc"``,
    are different digests.

    The digest says *that* the row moved, not what about it became invalid -
    the semantic checks stay in the parent, which runs the full validation
    chain on both sides of the window.
    """

    digest = hashlib.sha256()
    for index, column in enumerate(description):
        name = column[0]
        if name in DIGEST_EXCLUDED_COLUMNS:
            continue
        value = row[index]
        if value is None:
            payload = _NULL_TAG
        elif isinstance(value, bool):
            # ``bool`` is an ``int`` subclass and SQLite stores it as one; tag
            # it identically so a round trip through the file cannot change the
            # digest of a row nobody touched.
            payload = _INT_TAG + str(int(value)).encode("ascii")
        elif isinstance(value, int):
            payload = _INT_TAG + str(value).encode("ascii")
        elif isinstance(value, float):
            payload = _FLOAT_TAG + repr(value).encode("ascii")
        elif isinstance(value, str):
            payload = _TEXT_TAG + value.encode("utf-8")
        elif isinstance(value, (bytes, bytearray, memoryview)):
            payload = _BLOB_TAG + bytes(value)
        else:  # pragma: no cover - sqlite3 returns only the five above
            raise TypeError(f"outbox column {name!r} has an unhashable SQLite type")
        digest.update(struct.pack("!I", len(payload)))
        digest.update(payload)
    return digest.hexdigest()


def validate_heartbeat_row(
    row: sqlite3.Row,
    *,
    saga_id: str,
    operation_id: str,
    phase: str,
) -> None:
    """Structural and ownership checks a stdlib-only process can make.

    This is the subset of ``_validate_outbox_row`` that needs no canonical JSON,
    no hashing of payloads and no pydantic models: SQLite storage classes, the
    executor columns, parseable timestamps, and the three identity assertions
    that the row in front of this process is the row this session is about.
    The remaining semantic checks run in the parent at both edges of the
    invocation window; between them the row is sampled by the digest.
    """

    for key in (
        "operation_id",
        "saga_id",
        "phase",
        "payload_json",
        "payload_hash",
        "idempotency_hash",
        "status",
    ):
        if type(row[key]) is not str:
            raise HeartbeatProtocolError("outbox SQLite types are invalid")
    if (
        type(row["executor_generation"]) is not int
        or int(row["executor_generation"]) < 0
        or type(row["invoke_started"]) is not int
        or int(row["invoke_started"]) not in {0, 1}
    ):
        raise HeartbeatProtocolError("outbox executor SQLite types are invalid")
    owner = row["executor_owner_token"]
    if owner is not None and (type(owner) is not str or not owner):
        raise HeartbeatProtocolError("outbox executor owner is malformed")
    for key in (
        "executor_lease_expires_at",
        "executor_heartbeat_at",
        "dispatch_started_at",
        "max_external_deadline",
        "not_before_takeover_at",
    ):
        value = row[key]
        if value is None:
            continue
        if type(value) is not str:
            raise HeartbeatProtocolError(f"outbox {key} is malformed")
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError as exc:
            raise HeartbeatProtocolError(f"outbox {key} is malformed") from exc
        if parsed.tzinfo is None:
            raise HeartbeatProtocolError(f"outbox {key} is not time-zone aware")
    if row["status"] == "pending" and row["result_json"] is not None:
        raise HeartbeatProtocolError("pending outbox effect cannot carry a result")
    if row["saga_id"] != saga_id or row["operation_id"] != operation_id or row["phase"] != phase:
        raise HeartbeatProtocolError("outbox row identity does not match this session")


def _no_stage(stage: str) -> None:
    del stage


def heartbeat_write(
    connection: sqlite3.Connection,
    *,
    now_iso: str,
    expires_iso: str,
    operation_id: str,
    owner_token: str,
    owner_generation: int,
    phase: str,
    saga_id: str,
    validate: Any = None,
    mark_stage: Any = _no_stage,
) -> str:
    """Renew one lease inside one write transaction; return the row digest.

    Statement for statement what ``_heartbeat_outbox`` has always run: begin
    immediate, read and validate, update under the owner and generation guard,
    refuse a ``rowcount`` other than one, commit, and roll back on any
    exception.  ``rowcount == 0`` means somebody else owns the row now - the
    caller turns that into the same conflict it always has.

    The digest is taken inside this transaction, so the bytes it covers are the
    bytes this renewal saw under the write lock; a digest read afterwards could
    have been changed in between by the very writer it is meant to detect.
    """

    mark_stage("lock-wait")
    connection.execute("BEGIN IMMEDIATE")
    try:
        mark_stage("read")
        cursor = connection.execute(HEARTBEAT_SELECT_SQL, (operation_id,))
        row = cursor.fetchone()
        if row is None:
            raise HeartbeatProtocolError("required outbox effect is missing")
        if validate is None:
            validate_heartbeat_row(
                row, saga_id=saga_id, operation_id=operation_id, phase=phase
            )
        else:
            validate(row)
        digest = stable_row_digest(row, cursor.description)
        mark_stage("update")
        updated = connection.execute(
            HEARTBEAT_UPDATE_SQL,
            (now_iso, expires_iso, operation_id, owner_token, owner_generation),
        ).rowcount
        if updated != 1:
            raise HeartbeatOwnershipError("outbox executor lost ownership before heartbeat")
        mark_stage("commit")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    mark_stage("close")
    return digest


class HeartbeatOwnershipError(Exception):
    """The renewal guard matched no row: this executor no longer owns it."""


# --------------------------------------------------------------------------
# Frame codec.  Newline-delimited JSON, primitives only, no pickle anywhere.
# --------------------------------------------------------------------------


def encode_frame(frame: dict[str, Any]) -> bytes:
    """Serialize one frame.

    ``allow_nan=False`` is load bearing rather than tidy: ``json.dumps`` would
    otherwise emit the bare token ``NaN``, which is not JSON, and a non-finite
    interval would reach ``select``'s timeout.  Refusing it here raises in the
    parent *before* the external invocation, so a rejected value costs zero
    external calls.
    """

    encoded = json.dumps(frame, allow_nan=False, separators=(",", ":")).encode("utf-8")
    if len(encoded) + 1 > MAX_FRAME_BYTES:
        raise HeartbeatProtocolError("heartbeat frame exceeds the protocol size limit")
    return encoded + b"\n"


def decode_frame(line: bytes) -> dict[str, Any]:
    """Parse one frame and check its shape before any field is used."""

    if len(line) > MAX_FRAME_BYTES:
        raise HeartbeatProtocolError("heartbeat frame exceeds the protocol size limit")
    try:
        frame = json.loads(line.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise HeartbeatProtocolError("heartbeat frame is not valid JSON") from exc
    if type(frame) is not dict or type(frame.get("t")) is not str:
        raise HeartbeatProtocolError("heartbeat frame has no frame type")
    return frame


class FrameReader:
    """Buffered newline-framed reader over a raw file descriptor.

    Reads only when the caller says the descriptor is ready, and reports end of
    file as a value rather than an exception - on a bare pipe ``os.read``
    returns ``b""``, and the teardown that consumes this has one fewer thing to
    suppress.
    """

    __slots__ = ("_buffer", "_eof", "_fd")

    def __init__(self, fd: int) -> None:
        self._fd = fd
        self._buffer = b""
        self._eof = False

    @property
    def at_eof(self) -> bool:
        return self._eof and not self._buffer

    def take_buffered(self) -> dict[str, Any] | None:
        line, separator, rest = self._buffer.partition(b"\n")
        if not separator:
            if len(self._buffer) > MAX_FRAME_BYTES:
                raise HeartbeatProtocolError("heartbeat frame exceeds the protocol size limit")
            return None
        self._buffer = rest
        return decode_frame(line)

    def fill(self) -> bool:
        """Read one chunk; return ``False`` at end of file."""

        try:
            chunk = os.read(self._fd, 65536)
        except InterruptedError:  # pragma: no cover - PEP 475 retries for us
            return True
        except OSError as exc:
            if exc.errno in {errno.EAGAIN, errno.EWOULDBLOCK}:
                return True
            raise
        if not chunk:
            self._eof = True
            return False
        self._buffer += chunk
        return True


# POSIX guarantees a write of at most ``PIPE_BUF`` bytes is atomic, and 512 is
# the floor every platform meets.  A tick frame is well under it, which is what
# lets one be dropped whole on ``EAGAIN`` instead of leaving half a line in the
# stream.
_PIPE_ATOMIC_BYTES = 512


def _write_frame(fd: int, frame: dict[str, Any]) -> bool:
    """Write one frame to completion.  Never raises; reports failure instead."""

    payload = encode_frame(frame)
    while payload:
        try:
            payload = payload[os.write(fd, payload) :]
        except OSError:
            return False
    return True


def _write_tick_frame(fd: int, frame: dict[str, Any]) -> bool:
    """Write a tick frame without ever blocking the renewal loop on the parent.

    ``tick`` is the one frame that can be frequent, and the parent does not read
    the status pipe while an invocation runs.  A blocking write here would let a
    full pipe stop the heartbeat itself, so a tick that does not fit is dropped;
    the authoritative counters and digests ride on ``end-ack`` instead.
    """

    payload = encode_frame(frame)
    if len(payload) > _PIPE_ATOMIC_BYTES:  # pragma: no cover - a tick is ~100B
        return False
    os.set_blocking(fd, False)
    try:
        os.write(fd, payload)
    except OSError:
        return False
    finally:
        os.set_blocking(fd, True)
    return True


# --------------------------------------------------------------------------
# Helper entry point.
# --------------------------------------------------------------------------



class _Session:
    """Everything one invocation window owns, discarded when it ends.

    Session-scoped on purpose: a long-lived helper that kept ``owner_generation``
    or ``ticks`` across sessions would renew the next invocation under the
    previous one's fencing, which is exactly the guard this system relies on.
    """

    __slots__ = (
        "digest_changed",
        "first_digest",
        "interval",
        "last_digest",
        "last_outcome",
        "last_stage",
        "open",
        "operation_id",
        "owner_generation",
        "phase",
        "ticks",
        "token",
    )

    def __init__(self, frame: dict[str, Any]) -> None:
        token = frame.get("token")
        phase = frame.get("phase")
        operation_id = frame.get("operation_id")
        owner_generation = frame.get("owner_generation")
        interval = frame.get("interval_seconds")
        if (
            type(token) is not str
            or type(phase) is not str
            or type(operation_id) is not str
            or type(owner_generation) is not int
            or type(interval) not in {int, float}
        ):
            raise HeartbeatProtocolError("session frame is malformed")
        interval = float(interval)
        # ``select``'s timeout is given this number directly and its behaviour
        # on a NaN is not defined by anything this process controls.  The
        # parent refuses one earlier still - at construction, and again when
        # the frame is encoded - so this is the third of three.
        if interval != interval or interval in {float("inf"), float("-inf")} or interval <= 0:
            raise HeartbeatProtocolError("session interval must be finite and positive")
        self.token = token
        self.phase = phase
        self.operation_id = operation_id
        self.owner_generation = owner_generation
        self.interval = interval
        self.open = True
        self.ticks = 0
        self.first_digest: str | None = None
        self.last_digest: str | None = None
        self.digest_changed = False
        self.last_outcome = "ok"
        self.last_stage = "idle"


def _read_config(reader: FrameReader) -> dict[str, Any]:
    """Read the one config frame, which is where every secret arrives.

    Nothing here comes from ``argv`` or the environment: a command line is
    world-readable through ``ps`` and ``/proc/<pid>/cmdline``, and the owner
    token and database path are neither.
    """

    while True:
        frame = reader.take_buffered()
        if frame is not None:
            break
        if not reader.fill():
            raise HeartbeatProtocolError("control pipe closed before a config frame arrived")
    if frame["t"] != "config":
        raise HeartbeatProtocolError("first control frame was not a config frame")
    if frame.get("protocol") != HEARTBEAT_PROTOCOL_VERSION:
        raise HeartbeatProtocolError("config frame declares an unsupported protocol")
    for key, kind in (
        ("db", str),
        ("saga_id", str),
        ("owner_token", str),
        ("busy_timeout_ms", int),
    ):
        if type(frame.get(key)) is not kind:
            raise HeartbeatProtocolError(f"config frame field {key!r} is malformed")
    for key in ("lease_seconds", "idle_exit_seconds"):
        value = frame.get(key)
        if type(value) not in {int, float} or value != value or value <= 0:
            raise HeartbeatProtocolError(f"config frame field {key!r} is malformed")
    return frame


def _describe_failure(exc: BaseException) -> dict[str, Any]:
    """Reduce an exception to primitives.

    No exception object crosses the pipe.  The parent gets SQLite's own
    ``sqlite_errorcode`` to branch on plus a truncated message for a human to
    read; matching on message text is precisely what the code field replaces.
    """

    code = getattr(exc, "sqlite_errorcode", None)
    return {
        "errorcode": code if type(code) is int else None,
        "type": type(exc).__name__,
        "detail": str(exc)[:_MAX_DETAIL_CHARS],
    }


def _close_quietly(connection: sqlite3.Connection) -> None:
    """Close without letting the close itself become the reported failure."""

    try:
        connection.close()
    except sqlite3.Error:
        return


def _run_tick(config: dict[str, Any], session: _Session, status_fd: int) -> bool:
    """Renew once.  Returns ``False`` when the session must close."""

    now = datetime.now(UTC)
    expires = now + timedelta(seconds=float(config["lease_seconds"]))
    stage = ["connect"]

    def mark(value: str) -> None:
        stage[0] = value

    connection = None
    try:
        connection = open_saga_connection(str(config["db"]), int(config["busy_timeout_ms"]))
        digest = heartbeat_write(
            connection,
            now_iso=now.isoformat(),
            expires_iso=expires.isoformat(),
            operation_id=session.operation_id,
            owner_token=str(config["owner_token"]),
            owner_generation=session.owner_generation,
            phase=session.phase,
            saga_id=str(config["saga_id"]),
            mark_stage=mark,
        )
    except BaseException as exc:
        session.last_outcome = "failed"
        session.last_stage = stage[0]
        # The session closes, this process does not.  A renewal that fails is
        # an ordinary outcome the parent turns into one typed error; killing a
        # healthy helper over it would cost a terminate, a kill and a restart.
        session.open = False
        frame: dict[str, Any] = {"t": "failed", "token": session.token}
        frame.update(_describe_failure(exc))
        _write_frame(status_fd, frame)
        return False
    finally:
        if connection is not None:
            _close_quietly(connection)
    session.ticks += 1
    session.last_stage = stage[0]
    if session.first_digest is None:
        session.first_digest = digest
    session.last_digest = digest
    if digest != session.first_digest:
        # Sticky.  A window tamper that is undone before the next sample still
        # leaves this set - restoring the bytes is what the sampling is for.
        session.digest_changed = True
    _write_tick_frame(
        status_fd,
        {"t": "tick", "n": session.ticks, "stage": session.last_stage, "digest": digest},
    )
    return True


def _end_ack(session: _Session | None, token: Any) -> dict[str, Any]:
    if session is None or session.token != token:
        return {
            "t": "end-ack",
            "token": token if type(token) is str else "",
            "ticks": 0,
            "last_stage": "idle",
            "last_outcome": "unknown-token",
            "first_digest": None,
            "last_digest": None,
            "digest_changed": False,
        }
    return {
        "t": "end-ack",
        "token": session.token,
        "ticks": session.ticks,
        "last_stage": session.last_stage,
        "last_outcome": session.last_outcome,
        "first_digest": session.first_digest,
        "last_digest": session.last_digest,
        "digest_changed": session.digest_changed,
    }


def main(argv: list[str] | None = None) -> int:
    """Serve one saga's renewals until the parent stops, or dies, or goes idle.

    ``select`` on the control and stop descriptors is the whole scheduler: it
    is the sleep between renewals, the command inbox, and the parent-death
    detector at once.  Nothing here starts a thread and nothing polls.
    """

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--control-fd", type=int, required=True)
    parser.add_argument("--status-fd", type=int, required=True)
    parser.add_argument("--stop-fd", type=int, required=True)
    args = parser.parse_args(argv)
    # ``SIGTERM`` keeps its default so the parent's escalation lands at once.
    # ``SIGINT`` is ignored: a Ctrl-C in the parent's terminal reaches this
    # session too, and when this process stops is the parent's decision, taken
    # by closing the stop pipe.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    control_fd = args.control_fd
    status_fd = args.status_fd
    stop_fd = args.stop_fd
    reader = FrameReader(control_fd)
    try:
        config = _read_config(reader)
    except HeartbeatProtocolError:
        return 2
    if not _write_frame(
        status_fd,
        {"t": "ready", "protocol": HEARTBEAT_PROTOCOL_VERSION, "pid": os.getpid()},
    ):
        return 3
    idle_exit = float(config["idle_exit_seconds"])
    idle_since = time.monotonic()
    session: _Session | None = None
    next_tick_at = 0.0
    while True:
        now = time.monotonic()
        if session is not None and session.open:
            # I-1: while a session is open the idle-exit branch below is
            # unreachable, and the parent only enters its invocation after the
            # ack written when the session opened.  So the ack is proof that
            # no invocation can be running against a helper on its way out.
            timeout = max(0.0, next_tick_at - now)
        else:
            remaining = idle_exit - (now - idle_since)
            if remaining <= 0:
                return 0
            timeout = min(remaining, _IDLE_POLL_SECONDS)
        try:
            ready, _, _ = select.select([control_fd, stop_fd], [], [], timeout)
        except InterruptedError:  # pragma: no cover - PEP 475 retries for us
            continue
        if stop_fd in ready:
            # The parent never writes to this pipe, so readable means its write
            # end is gone: it closed it, or it died and the kernel closed it.
            # This is the portable stand-in for ``PR_SET_PDEATHSIG``.
            return 0
        if control_fd in ready:
            reader.fill()
            if reader.at_eof:
                return 0
        while True:
            try:
                frame = reader.take_buffered()
            except HeartbeatProtocolError:
                return 4
            if frame is None:
                break
            kind = frame["t"]
            if kind == "session":
                try:
                    session = _Session(frame)
                except HeartbeatProtocolError as exc:
                    failure: dict[str, Any] = {"t": "failed", "token": frame.get("token", "")}
                    failure.update(_describe_failure(exc))
                    _write_frame(status_fd, failure)
                    session = None
                    idle_since = time.monotonic()
                    continue
                next_tick_at = time.monotonic() + session.interval
                # Open first, answer second.  The ack the parent waits on is
                # therefore evidence the idle-exit branch is already closed.
                _write_frame(status_fd, {"t": "session-ack", "token": session.token})
            elif kind == "end":
                # I-2: answered unconditionally - whether this session is open,
                # already closed by a failed renewal, or not this token at all.
                # Without it an ordinary renewal failure would cost the parent
                # a full shutdown budget, a terminate and a kill.
                _write_frame(status_fd, _end_ack(session, frame.get("token")))
                session = None
                idle_since = time.monotonic()
            else:
                return 5
        if session is not None and session.open and time.monotonic() >= next_tick_at:
            if _run_tick(config, session, status_fd):
                next_tick_at = time.monotonic() + session.interval
            else:
                idle_since = time.monotonic()


if __name__ == "__main__":  # pragma: no cover - exercised as a subprocess
    sys.exit(main())
