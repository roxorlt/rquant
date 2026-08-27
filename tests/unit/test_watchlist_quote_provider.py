from __future__ import annotations

import multiprocessing
import os
import signal
import threading
import time
from datetime import UTC, datetime
from multiprocessing.connection import Connection
from pathlib import Path

import pandas as pd
import pytest

from rquant.watchlist_quote_provider import AkshareSinaWatchlistQuoteProvider


def _snapshot() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "代码": "600000",
                "最新价": 10.1,
                "今开": 10.0,
                "最高": 10.2,
                "最低": 9.9,
                "成交量": 1_000.0,
                "成交额": 10_100.0,
            },
            {
                "代码": "000001",
                "最新价": 12.1,
                "今开": 12.0,
                "最高": 12.2,
                "最低": 11.9,
                "成交量": 2_000.0,
                "成交额": 24_200.0,
            },
        ]
    )


def _record_pid() -> None:
    path = Path(os.environ["RQUANT_WATCHLIST_QUOTE_TEST_PID_PATH"])
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.write(descriptor, f"{os.getpid()}\n".encode())
    finally:
        os.close(descriptor)


def _permanently_blocked_snapshot() -> pd.DataFrame:
    _record_pid()
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    while True:
        time.sleep(1)


def _terminable_blocked_snapshot() -> pd.DataFrame:
    _record_pid()
    while True:
        time.sleep(1)


def _snapshot_that_must_not_start_before_go() -> pd.DataFrame:
    _record_pid()
    return _snapshot()


def _blocked_snapshot_with_descendant() -> pd.DataFrame:
    _record_pid()
    child = os.fork()
    if child == 0:
        _record_pid()
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        while True:
            time.sleep(1)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    while True:
        time.sleep(1)


def _run_blocked_provider(pid_path: str) -> None:
    os.environ["RQUANT_WATCHLIST_QUOTE_TEST_PID_PATH"] = pid_path
    AkshareSinaWatchlistQuoteProvider(
        snapshot_loader=_blocked_snapshot_with_descendant,
        process_start_method="spawn",
        termination_grace_seconds=0.1,
    )(("600000.SH",), timeout_seconds=30)


def _run_provider_with_unconsumed_ready(worker_pid_path: str, loader_pid_path: str) -> None:
    os.environ["RQUANT_WATCHLIST_QUOTE_TEST_PID_PATH"] = loader_pid_path
    provider = AkshareSinaWatchlistQuoteProvider(
        snapshot_loader=_snapshot_that_must_not_start_before_go,
        process_start_method="spawn",
        termination_grace_seconds=0.1,
    )
    receive = provider._receive

    def hold_ready(
        receiver: Connection,
        *args: object,
        **kwargs: object,
    ) -> tuple[object, ...]:
        assert receiver.poll(5)
        assert provider.last_worker_pid is not None
        Path(worker_pid_path).write_text(str(provider.last_worker_pid), encoding="utf-8")
        time.sleep(30)
        return receive(receiver, *args, **kwargs)  # type: ignore[arg-type]

    provider._receive = hold_ready  # type: ignore[method-assign]
    provider(("600000.SH",), timeout_seconds=60)


def _run_blocked_provider_with_runtime_handler(pid_path: str, sender: Connection) -> None:
    os.environ["RQUANT_WATCHLIST_QUOTE_TEST_PID_PATH"] = pid_path
    stop_requested = False

    def request_stop(_signum: int, _frame: object) -> None:
        nonlocal stop_requested
        stop_requested = True

    signal.signal(signal.SIGTERM, request_stop)
    try:
        AkshareSinaWatchlistQuoteProvider(
            snapshot_loader=_blocked_snapshot_with_descendant,
            process_start_method="spawn",
            termination_grace_seconds=0.1,
        )(("600000.SH",), timeout_seconds=30)
    except InterruptedError:
        sender.send(("stopped", stop_requested))
    finally:
        sender.close()


def _wait_for_pids(path: Path, *, count: int, timeout: float = 5) -> tuple[int, ...]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            pids = tuple(int(value) for value in path.read_text(encoding="utf-8").splitlines())
            if len(pids) >= count:
                return pids
        time.sleep(0.01)
    raise AssertionError(f"expected {count} provider process ids")


def _assert_pids_reaped(pids: tuple[int, ...], *, timeout: float = 5) -> None:
    deadline = time.monotonic() + timeout
    remaining = set(pids)
    while remaining and time.monotonic() < deadline:
        for pid in tuple(remaining):
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                remaining.remove(pid)
        time.sleep(0.01)
    assert remaining == set()


@pytest.mark.parametrize(
    "start_method",
    [method for method in ("spawn", "fork") if method in multiprocessing.get_all_start_methods()],
)
def test_provider_filters_one_snapshot_under_spawn_and_fork(start_method: str) -> None:
    provider = AkshareSinaWatchlistQuoteProvider(
        snapshot_loader=_snapshot,
        process_start_method=start_method,
    )

    actual_starts: list[datetime] = []
    result = provider(
        ("600000.SH",),
        timeout_seconds=2,
        on_started=actual_starts.append,
    )

    assert result["ts_code"].tolist() == ["600000.SH"]
    assert result["price"].tolist() == [10.1]
    assert "observed_at" not in result.columns
    assert len(actual_starts) == 1
    assert actual_starts[0].tzinfo is UTC
    assert provider.last_actual_requested_at == actual_starts[0]
    assert provider.last_worker_pid is not None
    _assert_pids_reaped((provider.last_worker_pid,))


def test_missing_started_message_fails_protocol_and_reaps_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = AkshareSinaWatchlistQuoteProvider(
        snapshot_loader=_snapshot,
        process_start_method="spawn",
        termination_grace_seconds=0.05,
    )
    receive = provider._receive
    receive_count = 0

    def drop_started(*args: object, **kwargs: object) -> tuple[object, ...]:
        nonlocal receive_count
        receive_count += 1
        message = receive(*args, **kwargs)  # type: ignore[arg-type]
        if receive_count == 2:
            assert message[0] == "started"
            return receive(*args, **kwargs)  # type: ignore[arg-type]
        return message

    monkeypatch.setattr(provider, "_receive", drop_started)

    with pytest.raises(RuntimeError, match="started handshake"):
        provider(("600000.SH",), timeout_seconds=2, on_started=lambda _value: None)

    assert provider.last_worker_pid is not None
    _assert_pids_reaped((provider.last_worker_pid,))


def test_malformed_started_timestamp_fails_protocol_and_reaps_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = AkshareSinaWatchlistQuoteProvider(
        snapshot_loader=_snapshot,
        process_start_method="spawn",
        termination_grace_seconds=0.05,
    )
    receive = provider._receive
    receive_count = 0

    def corrupt_started(*args: object, **kwargs: object) -> tuple[object, ...]:
        nonlocal receive_count
        receive_count += 1
        message = receive(*args, **kwargs)  # type: ignore[arg-type]
        if receive_count == 2:
            assert message[0] == "started"
            return ("started", "not-a-timestamp")
        return message

    monkeypatch.setattr(provider, "_receive", corrupt_started)

    with pytest.raises(RuntimeError, match="started handshake"):
        provider(("600000.SH",), timeout_seconds=2, on_started=lambda _value: None)

    assert provider.last_worker_pid is not None
    _assert_pids_reaped((provider.last_worker_pid,))


def test_duplicate_started_message_is_not_accepted_as_result_and_reaps_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = AkshareSinaWatchlistQuoteProvider(
        snapshot_loader=_snapshot,
        process_start_method="spawn",
        termination_grace_seconds=0.05,
    )
    receive = provider._receive
    receive_count = 0
    started_message: tuple[object, ...] | None = None

    def duplicate_started(*args: object, **kwargs: object) -> tuple[object, ...]:
        nonlocal receive_count, started_message
        receive_count += 1
        message = receive(*args, **kwargs)  # type: ignore[arg-type]
        if receive_count == 2:
            started_message = message
        elif receive_count == 3:
            assert started_message is not None
            return started_message
        return message

    monkeypatch.setattr(provider, "_receive", duplicate_started)

    with pytest.raises(RuntimeError, match="response is invalid"):
        provider(("600000.SH",), timeout_seconds=2, on_started=lambda _value: None)

    assert provider.last_worker_pid is not None
    _assert_pids_reaped((provider.last_worker_pid,))


def test_worker_killed_after_started_is_reaped_and_reports_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "RQUANT_WATCHLIST_QUOTE_TEST_PID_PATH",
        str(tmp_path / "killed-after-started-pid"),
    )
    provider = AkshareSinaWatchlistQuoteProvider(
        snapshot_loader=_permanently_blocked_snapshot,
        process_start_method="spawn",
        termination_grace_seconds=0.05,
    )
    actual_starts: list[datetime] = []

    def kill_worker(actual_requested_at: datetime) -> None:
        actual_starts.append(actual_requested_at)
        assert provider.last_worker_pid is not None
        os.kill(provider.last_worker_pid, signal.SIGKILL)

    with pytest.raises(RuntimeError, match="exited without a response"):
        provider(("600000.SH",), timeout_seconds=2, on_started=kill_worker)

    assert len(actual_starts) == 1
    assert provider.last_worker_pid is not None
    _assert_pids_reaped((provider.last_worker_pid,))


def test_started_callback_failure_kills_descendants_and_reaps_worker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pid_path = tmp_path / "started-callback-descendants"
    monkeypatch.setenv("RQUANT_WATCHLIST_QUOTE_TEST_PID_PATH", str(pid_path))
    provider = AkshareSinaWatchlistQuoteProvider(
        snapshot_loader=_blocked_snapshot_with_descendant,
        process_start_method="spawn",
        termination_grace_seconds=0.05,
    )
    descendant_pids: tuple[int, ...] = ()

    def fail_started(_actual_requested_at: datetime) -> None:
        nonlocal descendant_pids
        descendant_pids = _wait_for_pids(pid_path, count=2)
        raise RuntimeError("gateway state persistence failed")

    with pytest.raises(RuntimeError, match="state persistence failed"):
        provider(("600000.SH",), timeout_seconds=2, on_started=fail_started)

    assert provider.last_worker_pid is not None
    assert descendant_pids
    _assert_pids_reaped(descendant_pids)


def test_provider_reaps_every_worker_after_repeated_permanent_timeouts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pid_path = tmp_path / "worker-pids"
    monkeypatch.setenv("RQUANT_WATCHLIST_QUOTE_TEST_PID_PATH", str(pid_path))
    provider = AkshareSinaWatchlistQuoteProvider(
        snapshot_loader=_permanently_blocked_snapshot,
        process_start_method="spawn",
        termination_grace_seconds=0.05,
    )
    pids: list[int] = []

    for _ in range(3):
        with pytest.raises(TimeoutError, match="exceeded"):
            provider(("600000.SH",), timeout_seconds=0.2)
        assert provider.last_worker_pid is not None
        pids.append(provider.last_worker_pid)

    assert len(set(pids)) == 3
    _assert_pids_reaped(tuple(pids))
    assert not any(process.pid in pids for process in multiprocessing.active_children())
    assert not any(
        thread.name.startswith("rquant-watchlist-quote") for thread in threading.enumerate()
    )


def test_timeout_terminates_before_waiting_the_cleanup_grace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pid_path = tmp_path / "terminate-first-pid"
    monkeypatch.setenv("RQUANT_WATCHLIST_QUOTE_TEST_PID_PATH", str(pid_path))
    provider = AkshareSinaWatchlistQuoteProvider(
        snapshot_loader=_terminable_blocked_snapshot,
        process_start_method="spawn",
        termination_grace_seconds=1,
    )

    started = time.monotonic()
    with pytest.raises(TimeoutError, match="exceeded"):
        provider(("600000.SH",), timeout_seconds=1)
    elapsed = time.monotonic() - started

    assert elapsed < 1.5
    _assert_pids_reaped(_wait_for_pids(pid_path, count=1))


def test_timeout_while_ready_is_unconsumed_never_starts_loader_and_reaps_worker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pid_path = tmp_path / "loader-must-not-run"
    monkeypatch.setenv("RQUANT_WATCHLIST_QUOTE_TEST_PID_PATH", str(pid_path))
    provider = AkshareSinaWatchlistQuoteProvider(
        snapshot_loader=_snapshot_that_must_not_start_before_go,
        process_start_method="spawn",
        termination_grace_seconds=0.05,
    )
    receive = provider._receive
    first_receive = True

    def delay_ready_consumption(
        receiver: Connection,
        *args: object,
        **kwargs: object,
    ) -> tuple[object, ...]:
        nonlocal first_receive
        if first_receive:
            first_receive = False
            assert receiver.poll(2)
            time.sleep(0.15)
        return receive(receiver, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(provider, "_receive", delay_ready_consumption)

    with pytest.raises(TimeoutError, match="exceeded"):
        provider(("600000.SH",), timeout_seconds=0.1)

    assert provider.last_worker_pid is not None
    assert provider.last_worker_process_group_id == provider.last_worker_pid
    _assert_pids_reaped((provider.last_worker_pid,))
    assert not pid_path.exists()


def test_timeout_kills_provider_descendants_and_reaps_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pid_path = tmp_path / "descendant-pids"
    monkeypatch.setenv("RQUANT_WATCHLIST_QUOTE_TEST_PID_PATH", str(pid_path))
    provider = AkshareSinaWatchlistQuoteProvider(
        snapshot_loader=_blocked_snapshot_with_descendant,
        process_start_method="spawn",
        termination_grace_seconds=0.05,
    )

    # The worker is spawned and then forks a descendant; both have to register
    # before the provider's deadline kills the group, or the pid file only ever
    # gets one entry. Half a second is a fast developer machine's budget for two
    # CPython start-ups; the case is about reaping the group, not about how small
    # the deadline that trips it is.
    with pytest.raises(TimeoutError, match="exceeded"):
        provider(("600000.SH",), timeout_seconds=10)

    _assert_pids_reaped(_wait_for_pids(pid_path, count=2, timeout=15))


def test_delayed_ready_consumption_then_go_still_reaps_worker_descendants(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pid_path = tmp_path / "delayed-ready-descendant-pids"
    monkeypatch.setenv("RQUANT_WATCHLIST_QUOTE_TEST_PID_PATH", str(pid_path))
    provider = AkshareSinaWatchlistQuoteProvider(
        snapshot_loader=_blocked_snapshot_with_descendant,
        process_start_method="spawn",
        termination_grace_seconds=0.05,
    )
    receive = provider._receive
    first_receive = True

    def delay_ready_consumption(
        receiver: Connection,
        *args: object,
        **kwargs: object,
    ) -> tuple[object, ...]:
        nonlocal first_receive
        if first_receive:
            first_receive = False
            assert receiver.poll(2)
            time.sleep(0.15)
        return receive(receiver, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(provider, "_receive", delay_ready_consumption)

    # The worker is spawned and then forks a descendant; both have to register
    # before the provider's deadline kills the group, or the pid file only ever
    # gets one entry. Half a second is a fast developer machine's budget for two
    # CPython start-ups; the case is about reaping the group, not about how small
    # the deadline that trips it is.
    with pytest.raises(TimeoutError, match="exceeded"):
        provider(("600000.SH",), timeout_seconds=10)

    _assert_pids_reaped(_wait_for_pids(pid_path, count=2, timeout=15))


def test_sigterm_interrupts_blocked_provider_and_leaves_no_descendants(tmp_path: Path) -> None:
    pid_path = tmp_path / "signal-pids"
    context = multiprocessing.get_context("spawn")
    parent = context.Process(target=_run_blocked_provider, args=(str(pid_path),))
    parent.start()
    child_pids = _wait_for_pids(pid_path, count=2)

    os.kill(parent.pid, signal.SIGTERM)
    parent.join(timeout=5)
    if parent.is_alive():
        parent.kill()
        parent.join(timeout=2)

    assert parent.exitcode == -signal.SIGTERM
    _assert_pids_reaped(child_pids)
    parent.close()


def test_sigterm_in_unconsumed_ready_window_reaps_group_before_loader_starts(
    tmp_path: Path,
) -> None:
    worker_pid_path = tmp_path / "ready-worker-pid"
    loader_pid_path = tmp_path / "loader-must-not-start"
    context = multiprocessing.get_context("spawn")
    parent = context.Process(
        target=_run_provider_with_unconsumed_ready,
        args=(str(worker_pid_path), str(loader_pid_path)),
    )
    parent.start()
    deadline = time.monotonic() + 5
    while not worker_pid_path.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert worker_pid_path.exists()
    worker_pid = int(worker_pid_path.read_text(encoding="utf-8"))

    os.kill(parent.pid, signal.SIGTERM)
    parent.join(timeout=5)
    if parent.is_alive():
        parent.kill()
        parent.join(timeout=2)

    assert parent.exitcode == -signal.SIGTERM
    _assert_pids_reaped((worker_pid,))
    assert not loader_pid_path.exists()
    parent.close()


def test_sigterm_replays_runtime_handler_after_provider_cleanup(tmp_path: Path) -> None:
    pid_path = tmp_path / "graceful-signal-pids"
    context = multiprocessing.get_context("spawn")
    receiver, sender = context.Pipe(duplex=False)
    parent = context.Process(
        target=_run_blocked_provider_with_runtime_handler,
        args=(str(pid_path), sender),
    )
    parent.start()
    sender.close()
    child_pids = _wait_for_pids(pid_path, count=2)

    os.kill(parent.pid, signal.SIGTERM)
    assert receiver.poll(5)
    assert receiver.recv() == ("stopped", True)
    receiver.close()
    parent.join(timeout=5)

    assert parent.exitcode == 0
    _assert_pids_reaped(child_pids)
    parent.close()
