"""Making a long DuckDB/SQLite read answer SIGTERM (#268).

On 2026-09-14, the first trading day with all twenty runtime units resident, the
coordinator stopped the nine heaviest roles at 09:33. Seven of them were inside a read of
the ten-gigabyte read-only replica, and a process in an uninterruptible read does not run
a Python signal handler: `TimeoutStopSec=60` expired, systemd sent `SIGKILL`, the unit was
reported `Result=timeout`, and `OnFailure` turned an operator's own `systemctl stop` into
an alert. Lengthening the unit's timeout would hide the symptom and is the owner's
decision anyway; this module removes the cause, which is that nothing told the engine to
give up.

**Why a thread and a pipe rather than a signal handler.** CPython runs a Python-level
signal handler in the *main* thread, between bytecodes. A role blocked in
`DuckDBPyConnection.execute()` is inside C with the GIL released and executes no bytecode
at all until the query returns, so `stop_event.set()` written as a handler is not late --
it has not run. What *does* run immediately, in whichever thread the kernel picks, is
CPython's own C handler, and `signal.set_wakeup_fd` makes that handler write one byte,
the signal number, to a pipe. `StopSignalWatcher` reads that byte on a thread of its own,
so `interrupt()` reaches the engine while the main thread is still inside the query.
DuckDB then raises `duckdb.InterruptException` out of `execute()` within about a second
(measured: 0.508 s on the pinned 1.5.2), the role unwinds through its normal error paths,
and `run_service_loop` recognises the abandoned read as the stop it is.

**An interrupted read is not a shorter read.** `ReplicaReadGate.read()` already treats a
loader that raised as "opened the database, kept nothing" and forgets its cache, so an
interrupted round leaves no half answer behind; the next generation is read whole.

Two facts about `interrupt()` that shape the design, both measured on duckdb 1.5.2:

* it aborts the query running on that connection *now*, and the connection stays usable;
* issued while the connection is idle it is a **no-op** -- it does not arm the next query.

The second is why `register()` refuses outright once a stop has been requested: a read
that has not started yet cannot be interrupted into stopping, so it must not start.
"""

from __future__ import annotations

import errno
import os
import select
import signal
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from typing import Protocol

#: What `run_service_loop` records as the stop reason when a read was abandoned.
READ_INTERRUPT_STOP_REASON = "stop requested during a database read"

#: How deep `is_read_interrupt` follows `__cause__` / `__context__`. A role that wraps the
#: engine's error in its own sentence -- `reference source database query failed`, and
#: three more like it -- must still be recognised as a stop rather than a fault, and the
#: bound keeps a pathological chain from turning the check into a walk.
_MAX_CAUSE_DEPTH = 8

#: How long the watcher may sit in `select` before it looks at its own shutdown flag. The
#: pipe wakes it immediately when a signal arrives; this only bounds how long `__exit__`
#: waits for the thread to notice it is finished.
_WATCH_POLL_SECONDS = 0.2

#: What SQLite says when `Connection.interrupt()` aborted the statement. DuckDB raises a
#: dedicated class; SQLite raises `OperationalError` and only the message distinguishes it.
_SQLITE_INTERRUPT_MARKER = "interrupted"


class ReadInterruptedError(RuntimeError):
    """A database read was abandoned because this process was asked to stop.

    Raised by `ReadInterruptRegistry.register` when a stop has already been requested, so
    that a read which has not begun does not begin. It is deliberately **not** a
    `duckdb.Error`: the four read-side roles convert `duckdb.Error` into their own
    integrity errors, and a stop is not an integrity failure.
    """


class InterruptibleConnection(Protocol):
    """What this module needs of a connection: the ability to abandon its current query."""

    def interrupt(self) -> None: ...  # pragma: no cover - structural typing only


def is_read_interrupt(error: BaseException) -> bool:
    """Whether this failure is "the read was abandoned on the way out", not a fault.

    True for `ReadInterruptedError`, for DuckDB's `InterruptException`, and for the
    `sqlite3.OperationalError` an interrupted SQLite statement raises. The whole
    `__cause__` / `__context__` chain is followed because every one of these roles reports
    an engine error in its own words -- `auction_universe_source` and
    `auction_gap_candidate_input` both catch `duckdb.Error`, and `InterruptException` is
    one. The read paths this package touches re-raise an interrupt before they convert
    anything; following the chain is what keeps a path it did not touch honest too.
    """

    seen: set[int] = set()
    current: BaseException | None = error
    for _ in range(_MAX_CAUSE_DEPTH):
        if current is None or id(current) in seen:
            return False
        seen.add(id(current))
        if _is_direct_read_interrupt(current):
            return True
        current = current.__cause__ or current.__context__
    return False


def _is_direct_read_interrupt(error: BaseException) -> bool:
    if isinstance(error, ReadInterruptedError):
        return True
    import sqlite3

    if isinstance(error, sqlite3.OperationalError):
        return _SQLITE_INTERRUPT_MARKER in str(error).lower()
    interrupt_class = _duckdb_interrupt_class()
    return interrupt_class is not None and isinstance(error, interrupt_class)


def _duckdb_interrupt_class() -> type[BaseException] | None:
    """DuckDB's interrupt exception, or None where duckdb is not importable.

    Imported lazily and tolerantly on purpose: this module is on the stop path of every
    role, including the sixteen that never open a DuckDB database at all, and a stop must
    not be the first thing that imports a several-hundred-megabyte engine.
    """

    try:
        import duckdb
    except Exception:  # noqa: BLE001 - a stop path may not depend on an optional engine
        return None
    candidate = getattr(duckdb, "InterruptException", None)
    if isinstance(candidate, type) and issubclass(candidate, BaseException):
        return candidate
    return None


class ReadInterruptRegistry:
    """The database connections this process is reading through, and one switch for them.

    `register` / `release` are paired by `interruptible_read`; `request` abandons every
    read open at that moment and latches, so that a read starting afterwards is refused
    rather than begun. `reset` unlatches, and exists for the tests and for a process that
    outlives one stop -- nothing in production calls it after a stop.
    """

    def __init__(self) -> None:
        #: re-entrant because `request()` is also reachable from the Python-level signal
        #: handler, which runs on the main thread and may land in the middle of this
        #: object's own critical section. A plain `Lock` there is a deadlock.
        self._lock = threading.RLock()
        self._handles: dict[int, InterruptibleConnection] = {}
        self._next_token = 1
        self._requested = False

    @property
    def requested(self) -> bool:
        with self._lock:
            return self._requested

    @property
    def open_reads(self) -> int:
        """How many reads are registered right now, for the tests and the heartbeat."""

        with self._lock:
            return len(self._handles)

    def register(self, connection: InterruptibleConnection) -> int:
        """Take responsibility for abandoning this connection's query, and return a token.

        Refuses with `ReadInterruptedError` once a stop has been requested: an idle
        `interrupt()` does not arm the next query on duckdb 1.5.2, so a read begun after
        the stop would run to completion and be exactly the read that outlives
        `TimeoutStopSec`.
        """

        if not callable(getattr(connection, "interrupt", None)):
            raise TypeError("an interruptible read needs a connection with interrupt()")
        with self._lock:
            if self._requested:
                raise ReadInterruptedError(
                    "a database read may not start after a stop has been requested"
                )
            token = self._next_token
            self._next_token += 1
            self._handles[token] = connection
            return token

    def release(self, token: int) -> None:
        with self._lock:
            self._handles.pop(token, None)

    def request(self) -> int:
        """Abandon every read open now, latch, and say how many connections were told."""

        with self._lock:
            self._requested = True
            handles = tuple(self._handles.values())
        told = 0
        for handle in handles:
            try:
                handle.interrupt()
            except Exception:  # noqa: BLE001 - a closed connection is not a stop failure
                continue
            told += 1
        return told

    def reset(self) -> None:
        with self._lock:
            self._requested = False
            self._handles.clear()
            self._next_token = 1

    @contextmanager
    def interruptible(self, connection: InterruptibleConnection) -> Iterator[None]:
        token = self.register(connection)
        try:
            yield
        finally:
            self.release(token)


#: The registry the runtime's roles and its entrypoint share. One process runs one role.
READ_INTERRUPTS = ReadInterruptRegistry()


@contextmanager
def interruptible_read(connection: InterruptibleConnection) -> Iterator[None]:
    """Make this connection's query abandonable for as long as the block runs."""

    with READ_INTERRUPTS.interruptible(connection):
        yield


def request_read_interrupt() -> int:
    """Abandon every read this process has open. Safe to call more than once."""

    return READ_INTERRUPTS.request()


def read_interrupt_requested() -> bool:
    return READ_INTERRUPTS.requested


def reset_read_interrupts() -> None:
    READ_INTERRUPTS.reset()


class StopSignalWatcher:
    """Hear SIGTERM on a thread of this process's own, not in the main thread's eval loop.

    `signal.set_wakeup_fd` is the only thing CPython offers that acts while the main
    thread is inside a C call: the C handler writes the signal number to the pipe from
    whichever thread the kernel delivered the signal to, immediately. This watcher reads
    that byte and does two things -- abandons the open reads, and calls `on_stop`, which is
    what sets the loop's `stop_event`, because the Python-level handler that normally sets
    it will not run until the read it is waiting on returns.

    **It refuses to install over somebody else's wakeup fd.** `asyncio` uses the same slot
    on the main thread; taking it would break that loop's signal handling, and forwarding
    bytes on to it correctly is more fragility than this is worth. `active` says which
    happened, so a caller can report the degradation rather than assume the mechanism.
    """

    def __init__(
        self,
        *,
        signums: tuple[int, ...] = (signal.SIGINT, signal.SIGTERM),
        registry: ReadInterruptRegistry | None = None,
        on_stop: Callable[[], None] | None = None,
    ) -> None:
        if not signums:
            raise ValueError("a stop watcher needs at least one signal to watch")
        self.signums = tuple(sorted({int(item) for item in signums}))
        self.registry = READ_INTERRUPTS if registry is None else registry
        self._on_stop = on_stop
        self.active = False
        #: signal numbers this watcher actually saw, in arrival order, for the tests
        self.observed: list[int] = []
        self._read_fd = -1
        self._write_fd = -1
        self._previous_fd: int | None = None
        self._thread: threading.Thread | None = None
        self._finished = threading.Event()

    def __enter__(self) -> StopSignalWatcher:
        read_fd, write_fd = os.pipe()
        os.set_blocking(read_fd, False)
        os.set_blocking(write_fd, False)
        try:
            previous = signal.set_wakeup_fd(write_fd, warn_on_full_buffer=False)
        except (ValueError, OSError):
            #: not the main thread, or no signal support here. The role still stops, just
            #: no faster than the read it is inside -- which is the behaviour before this.
            os.close(read_fd)
            os.close(write_fd)
            return self
        if previous is not None and previous >= 0:
            signal.set_wakeup_fd(previous, warn_on_full_buffer=False)
            os.close(read_fd)
            os.close(write_fd)
            return self
        self._previous_fd = previous
        self._read_fd = read_fd
        self._write_fd = write_fd
        self._thread = threading.Thread(
            target=self._watch,
            name="rquant-read-interrupt",
            daemon=True,
        )
        self.active = True
        self._thread.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        if not self.active:
            return
        self._finished.set()
        if self._previous_fd is not None:
            with suppress(ValueError, OSError):  # pragma: no cover - cannot fail here
                signal.set_wakeup_fd(self._previous_fd, warn_on_full_buffer=False)
        thread = self._thread
        if thread is not None:
            thread.join(timeout=_WATCH_POLL_SECONDS * 10)
        for descriptor in (self._write_fd, self._read_fd):
            if descriptor >= 0:
                with suppress(OSError):  # pragma: no cover - already closed
                    os.close(descriptor)
        self._write_fd = -1
        self._read_fd = -1
        self._thread = None
        self.active = False

    def _watch(self) -> None:
        while not self._finished.is_set():
            try:
                ready, _, _ = select.select([self._read_fd], [], [], _WATCH_POLL_SECONDS)
            except (OSError, ValueError):  # pragma: no cover - the fd closed under us
                return
            if not ready:
                continue
            try:
                payload = os.read(self._read_fd, 4096)
            except OSError as error:  # pragma: no cover - a closed or drained pipe
                if error.errno in {errno.EAGAIN, errno.EWOULDBLOCK}:
                    continue
                return
            if not payload:  # pragma: no cover - the write end closed
                return
            if not self._handle(payload):
                continue
            return

    def _handle(self, payload: bytes) -> bool:
        """Act on one batch of wakeup bytes. True when a watched signal was among them."""

        watched = [number for number in payload if number in self.signums]
        if not watched:
            return False
        self.observed.extend(watched)
        self.registry.request()
        if self._on_stop is not None:
            self._on_stop()
        return True


__all__ = [
    "READ_INTERRUPTS",
    "READ_INTERRUPT_STOP_REASON",
    "InterruptibleConnection",
    "ReadInterruptRegistry",
    "ReadInterruptedError",
    "StopSignalWatcher",
    "interruptible_read",
    "is_read_interrupt",
    "read_interrupt_requested",
    "request_read_interrupt",
    "reset_read_interrupts",
]
