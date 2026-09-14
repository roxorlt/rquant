"""#268: a database read that answers SIGTERM instead of outliving `TimeoutStopSec`."""

from __future__ import annotations

import os
import signal
import sqlite3
import threading
import time
from pathlib import Path

import duckdb
import pytest

from rquant.runtime_read_interrupt import (
    READ_INTERRUPT_STOP_REASON,
    ReadInterruptedError,
    ReadInterruptRegistry,
    StopSignalWatcher,
    interruptible_read,
    is_read_interrupt,
    read_interrupt_requested,
    request_read_interrupt,
    reset_read_interrupts,
)

#: long enough that finishing on its own inside any assertion here is not possible
_ENDLESS = "SELECT count(*) FROM range(400000000000) WHERE range % 7 = 0"

#: what the acceptance criterion allows between the signal and the exception
_BUDGET_SECONDS = 5.0


class _Recording:
    """A connection that only records that it was told to give up."""

    def __init__(self) -> None:
        self.interrupts = 0

    def interrupt(self) -> None:
        self.interrupts += 1


class _Refusing(_Recording):
    def interrupt(self) -> None:
        super().interrupt()
        raise RuntimeError("this connection is closed")


@pytest.fixture(autouse=True)
def _clean_registry() -> None:
    reset_read_interrupts()
    yield
    reset_read_interrupts()


# ---------------------------------------------------------------------------------------
# the registry
# ---------------------------------------------------------------------------------------


def test_a_requested_stop_interrupts_every_open_read_once() -> None:
    registry = ReadInterruptRegistry()
    first, second = _Recording(), _Recording()
    with registry.interruptible(first), registry.interruptible(second):
        assert registry.open_reads == 2
        assert registry.request() == 2
    assert (first.interrupts, second.interrupts) == (1, 1)
    assert registry.open_reads == 0
    assert registry.requested


def test_a_read_that_has_already_finished_is_not_interrupted() -> None:
    registry = ReadInterruptRegistry()
    finished = _Recording()
    with registry.interruptible(finished):
        pass
    assert registry.request() == 0
    assert finished.interrupts == 0


def test_a_read_may_not_start_once_a_stop_has_been_requested() -> None:
    """An idle `interrupt()` does not arm the next query on duckdb 1.5.2 (measured).

    So a read begun after the stop would run to completion, which is exactly the read that
    outlives `TimeoutStopSec`. It is refused instead.
    """

    registry = ReadInterruptRegistry()
    registry.request()
    with pytest.raises(ReadInterruptedError), registry.interruptible(_Recording()):
        pytest.fail("the read must not begin")


def test_a_connection_that_refuses_to_be_interrupted_does_not_break_the_stop() -> None:
    registry = ReadInterruptRegistry()
    refusing, willing = _Refusing(), _Recording()
    with registry.interruptible(refusing), registry.interruptible(willing):
        assert registry.request() == 1
    assert refusing.interrupts == 1
    assert willing.interrupts == 1


def test_a_thing_without_interrupt_is_refused_rather_than_silently_untracked() -> None:
    registry = ReadInterruptRegistry()
    with pytest.raises(TypeError):
        registry.register(object())  # type: ignore[arg-type]


def test_the_process_registry_is_what_the_module_level_helpers_drive() -> None:
    connection = _Recording()
    with interruptible_read(connection):
        assert not read_interrupt_requested()
        assert request_read_interrupt() == 1
        assert read_interrupt_requested()
    assert connection.interrupts == 1
    reset_read_interrupts()
    assert not read_interrupt_requested()


# ---------------------------------------------------------------------------------------
# recognising one
# ---------------------------------------------------------------------------------------


def test_duckdbs_interrupt_is_recognised_and_an_ordinary_failure_is_not() -> None:
    connection = duckdb.connect(":memory:")
    raised: list[BaseException] = []

    def interrupt_soon() -> None:
        time.sleep(0.3)
        connection.interrupt()

    thread = threading.Thread(target=interrupt_soon)
    thread.start()
    started = time.monotonic()
    try:
        connection.execute(_ENDLESS).fetchall()
    except BaseException as error:  # noqa: BLE001 - the exception is the assertion
        raised.append(error)
    thread.join()

    assert raised, "the query finished, so nothing was interrupted"
    assert time.monotonic() - started < _BUDGET_SECONDS
    assert is_read_interrupt(raised[0])
    assert not is_read_interrupt(duckdb.Error("something else went wrong"))
    assert not is_read_interrupt(RuntimeError("unrelated"))


def test_an_interrupt_is_recognised_through_a_roles_own_wording() -> None:
    """Four read paths convert `duckdb.Error` into their own sentence, and it is one."""

    class _RoleError(RuntimeError):
        pass

    try:
        try:
            raise duckdb.InterruptException("INTERRUPT Error: Interrupted!")
        except duckdb.Error as cause:
            raise _RoleError("daily snapshot query failed") from cause
    except _RoleError as error:
        assert is_read_interrupt(error)


def test_sqlites_interrupted_statement_is_recognised(tmp_path: Path) -> None:
    connection = sqlite3.connect(tmp_path / "state.sqlite3")
    connection.execute("CREATE TABLE t(a INTEGER)")
    connection.executemany("INSERT INTO t VALUES (?)", [(index,) for index in range(200_000)])
    raised: list[BaseException] = []

    def interrupt_soon() -> None:
        time.sleep(0.05)
        connection.interrupt()

    thread = threading.Thread(target=interrupt_soon)
    thread.start()
    try:
        connection.execute(
            "SELECT count(*) FROM t AS a, t AS b, t AS c WHERE a.a + b.a + c.a > 0"
        ).fetchall()
    except BaseException as error:  # noqa: BLE001 - the exception is the assertion
        raised.append(error)
    thread.join()
    connection.close()

    assert raised, "the statement finished, so nothing was interrupted"
    assert is_read_interrupt(raised[0])


def test_a_cycle_in_the_cause_chain_terminates() -> None:
    first = RuntimeError("one")
    second = RuntimeError("two")
    first.__cause__ = second
    second.__cause__ = first
    assert not is_read_interrupt(first)


# ---------------------------------------------------------------------------------------
# the watcher: the part a signal handler cannot do
# ---------------------------------------------------------------------------------------


def test_a_signal_arriving_during_a_query_abandons_it_within_the_budget() -> None:
    """The claim ruling 30 makes, with the watcher as the only thing that can deliver it.

    The handler installed here sets the stop event and **does not** ask for the interrupt,
    which is what isolates the watcher: DuckDB does run pending Python handlers while a
    query is in flight (measured: 0.40 s into a 1.85 s query), so a handler that asked
    would hide whether the watcher works at all. Here nothing but the watcher's thread can
    reach the engine, and if it does not, the query runs for hours and this test times out
    rather than passing quietly.
    """

    stopped = threading.Event()
    previous = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, lambda *_: stopped.set())
    connection = duckdb.connect(":memory:")
    raised: list[BaseException] = []
    try:
        with StopSignalWatcher(on_stop=stopped.set) as watcher:
            assert watcher.active, "the wakeup fd was not available to this process"
            threading.Thread(
                target=lambda: (time.sleep(0.3), os.kill(os.getpid(), signal.SIGTERM)),
                daemon=True,
            ).start()
            started = time.monotonic()
            try:
                with interruptible_read(connection):
                    connection.execute(_ENDLESS).fetchall()
            except BaseException as error:  # noqa: BLE001 - the exception is the assertion
                raised.append(error)
            elapsed = time.monotonic() - started
            assert watcher.observed == [signal.SIGTERM]
    finally:
        signal.signal(signal.SIGTERM, previous)
        connection.close()

    assert raised, "the query ran to completion, so the signal never reached the engine"
    assert is_read_interrupt(raised[0])
    assert elapsed < _BUDGET_SECONDS, f"the read was abandoned after {elapsed:.2f}s"
    assert stopped.is_set(), "the watcher must also set the loop's stop event"


def test_only_the_watcher_can_abandon_a_long_sqlite_statement(tmp_path: Path) -> None:
    """The half of #268 a signal handler cannot do anything about, whatever it is written to do.

    Measured on this CPython: a Python-level handler runs 0.40 s into a 1.85 s DuckDB query
    and only at the *end* of a 20.5 s SQLite statement -- `sqlite3` releases the GIL for
    the whole statement and never yields to pending handlers. So here the handler asks for
    the interrupt and it makes no difference; the statement is abandoned by the watcher's
    thread or by nothing. This is why the watcher exists rather than just the handler.
    """

    connection = sqlite3.connect(tmp_path / "slow.sqlite3")
    connection.execute("CREATE TABLE t(a INTEGER)")
    connection.executemany("INSERT INTO t VALUES (?)", [(index,) for index in range(1_000)])
    handler_ran = threading.Event()
    previous = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, lambda *_: (handler_ran.set(), request_read_interrupt()))
    raised: list[BaseException] = []
    try:
        with StopSignalWatcher() as watcher:
            assert watcher.active
            threading.Thread(
                target=lambda: (time.sleep(0.3), os.kill(os.getpid(), signal.SIGTERM)),
                daemon=True,
            ).start()
            started = time.monotonic()
            try:
                with interruptible_read(connection):
                    connection.execute(
                        "SELECT count(*) FROM t AS a, t AS b, t AS c WHERE a.a + b.a + c.a > 0"
                    ).fetchall()
            except BaseException as error:  # noqa: BLE001 - the exception is the assertion
                raised.append(error)
            elapsed = time.monotonic() - started
    finally:
        signal.signal(signal.SIGTERM, previous)
        connection.close()

    assert raised, "the statement ran to completion, so nothing reached the engine"
    assert is_read_interrupt(raised[0])
    assert elapsed < _BUDGET_SECONDS, f"the statement was abandoned after {elapsed:.2f}s"


def test_the_watcher_restores_the_wakeup_fd_it_borrowed() -> None:
    with StopSignalWatcher() as watcher:
        assert watcher.active
    #: -1 is "nobody is using it", which is where it must be left
    assert signal.set_wakeup_fd(-1) == -1


def test_the_watcher_refuses_to_take_a_wakeup_fd_somebody_else_holds() -> None:
    """`asyncio` uses the same slot; taking it would break that loop's signal handling."""

    read_fd, write_fd = os.pipe()
    os.set_blocking(write_fd, False)
    signal.set_wakeup_fd(write_fd, warn_on_full_buffer=False)
    try:
        with StopSignalWatcher() as watcher:
            assert not watcher.active
        assert signal.set_wakeup_fd(-1) == write_fd
    finally:
        signal.set_wakeup_fd(-1)
        os.close(read_fd)
        os.close(write_fd)


def test_a_signal_the_watcher_does_not_watch_is_left_alone() -> None:
    registry = ReadInterruptRegistry()
    watcher = StopSignalWatcher(signums=(signal.SIGTERM,), registry=registry)
    connection = _Recording()
    token = registry.register(connection)
    assert watcher._handle(bytes([signal.SIGUSR1])) is False
    assert connection.interrupts == 0
    assert watcher._handle(bytes([signal.SIGUSR1, signal.SIGTERM])) is True
    assert connection.interrupts == 1
    registry.release(token)


def test_the_stop_reason_is_a_sentence_an_operator_can_read() -> None:
    assert READ_INTERRUPT_STOP_REASON == "stop requested during a database read"
