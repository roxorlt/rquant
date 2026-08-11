"""Live watchlist quote provider backed by AKShare's Sina spot snapshot."""

from __future__ import annotations

import multiprocessing
import os
import signal
import threading
import time
from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, datetime
from multiprocessing.connection import Connection
from multiprocessing.process import BaseProcess
from types import FrameType

import pandas as pd

SnapshotLoader = Callable[[], pd.DataFrame]
ProviderStartedCallback = Callable[[datetime], None]
_WorkerMessage = tuple[object, ...]


def _load_akshare_spot_snapshot() -> pd.DataFrame:
    import akshare as ak

    return ak.stock_zh_a_spot()


def _select_watchlist_rows(
    snapshot: pd.DataFrame,
    codes: tuple[str, ...],
) -> list[dict[str, object]]:
    required = {"代码", "最新价", "今开", "最高", "最低", "成交量", "成交额"}
    missing = sorted(required - set(snapshot.columns))
    if missing:
        raise RuntimeError(f"AKShare Sina quote snapshot missing columns: {missing}")
    code_map = {code.split(".")[0]: code for code in codes}
    rows: list[dict[str, object]] = []
    for _, row in snapshot.iterrows():
        raw_code = str(row["代码"])
        ts_code = code_map.get(raw_code[-6:])
        if ts_code is None:
            continue
        rows.append(
            {
                "ts_code": ts_code,
                "price": float(row["最新价"]),
                "open": float(row["今开"]),
                "high": float(row["最高"]),
                "low": float(row["最低"]),
                "volume": float(row["成交量"]),
                "amount": float(row["成交额"]),
            }
        )
    return rows


def _watchlist_quote_worker(
    codes: tuple[str, ...],
    snapshot_loader: SnapshotLoader,
    control: Connection,
) -> None:
    """Spawn-safe child entrypoint; no provider work runs while importing this module."""

    try:
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        os.setsid()
        pid = os.getpid()
        control.send(("ready", pid))
        command = control.recv()
        if command != ("go", pid):
            raise RuntimeError("watchlist quote provider parent handshake is invalid")
        actual_requested_at = datetime.now(UTC)
        control.send(("started", actual_requested_at))
        control.send(("ok", _select_watchlist_rows(snapshot_loader(), codes)))
    except BaseException as exc:
        with suppress(BrokenPipeError, EOFError, OSError):
            control.send(("error", type(exc).__name__, str(exc)[:1000]))
    finally:
        control.close()


class _ProviderSignal(BaseException):
    def __init__(self, signum: int, frame: FrameType | None) -> None:
        self.signum = signum
        self.frame = frame


def _signal_process_group(pid: int, signum: int) -> None:
    with suppress(PermissionError, ProcessLookupError):
        os.killpg(pid, signum)


def _replay_signal(
    signum: int,
    frame: FrameType | None,
    previous_handler: object,
) -> None:
    if callable(previous_handler):
        previous_handler(signum, frame)
        raise InterruptedError(f"watchlist quote provider interrupted by signal {signum}")
    if previous_handler is signal.SIG_IGN:
        raise InterruptedError(f"watchlist quote provider interrupted by signal {signum}")
    signal.signal(signum, signal.SIG_DFL)
    os.kill(os.getpid(), signum)
    raise SystemExit(128 + signum)


class AkshareSinaWatchlistQuoteProvider:
    """Run one public real-time spot snapshot in a bounded, reaped process tree."""

    def __init__(
        self,
        *,
        snapshot_loader: SnapshotLoader = _load_akshare_spot_snapshot,
        process_start_method: str = "spawn",
        termination_grace_seconds: float = 0.25,
    ) -> None:
        if process_start_method not in multiprocessing.get_all_start_methods():
            raise ValueError(f"unsupported multiprocessing start method: {process_start_method}")
        if termination_grace_seconds <= 0:
            raise ValueError("termination grace must be positive")
        self._snapshot_loader = snapshot_loader
        self._context = multiprocessing.get_context(process_start_method)
        self._termination_grace_seconds = termination_grace_seconds
        self.last_worker_pid: int | None = None
        self.last_worker_process_group_id: int | None = None
        self.last_actual_requested_at: datetime | None = None

    def __call__(
        self,
        codes: tuple[str, ...],
        *,
        timeout_seconds: float,
        on_started: ProviderStartedCallback | None = None,
    ) -> pd.DataFrame:
        if timeout_seconds <= 0:
            raise ValueError("provider timeout must be positive")
        self.last_actual_requested_at = None
        parent_control, child_control = self._context.Pipe(duplex=True)
        process = self._context.Process(
            target=_watchlist_quote_worker,
            args=(codes, self._snapshot_loader, child_control),
            daemon=True,
            name="rquant-watchlist-quote-provider",
        )
        deadline = time.monotonic() + timeout_seconds
        process_group_owned = False
        terminate_first = False
        interruption: _ProviderSignal | None = None
        previous_handlers: dict[int, object] = {}

        def interrupt(signum: int, frame: FrameType | None) -> None:
            raise _ProviderSignal(signum, frame)

        manages_signals = threading.current_thread() is threading.main_thread()
        if manages_signals:
            previous_handlers = {
                signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)
            }
            for signum in previous_handlers:
                signal.signal(signum, interrupt)
        try:
            try:
                process.start()
                if process.pid is None:
                    raise RuntimeError("watchlist quote provider worker has no pid")
                self.last_worker_pid = process.pid
                child_control.close()
                message = self._receive(parent_control, process, deadline, timeout_seconds)
                if message[:1] != ("ready",) or message[1:] != (process.pid,):
                    raise RuntimeError("watchlist quote provider worker handshake is invalid")
                self.last_worker_process_group_id = process.pid
                process_group_owned = True
                parent_control.send(("go", process.pid))
                message = self._receive(parent_control, process, deadline, timeout_seconds)
                actual_requested_at = self._started_at(message)
                self.last_actual_requested_at = actual_requested_at
                if on_started is not None:
                    on_started(actual_requested_at)
                message = self._receive(parent_control, process, deadline, timeout_seconds)
                if message[:1] == ("error",):
                    error_type = str(message[1]) if len(message) > 1 else "Error"
                    detail = str(message[2]) if len(message) > 2 else "unknown provider error"
                    raise RuntimeError(f"AKShare Sina quote worker {error_type}: {detail}")
                if len(message) != 2 or message[0] != "ok" or not isinstance(message[1], list):
                    raise RuntimeError("watchlist quote provider worker response is invalid")
                return pd.DataFrame(message[1])
            except _ProviderSignal as exc:
                interruption = exc
                terminate_first = True
            except BaseException:
                terminate_first = True
                raise
        finally:
            self._reap(
                process,
                control=parent_control,
                process_group_owned=process_group_owned,
                terminate_first=terminate_first,
            )
            parent_control.close()
            child_control.close()
            if manages_signals:
                for signum, handler in previous_handlers.items():
                    if signal.getsignal(signum) is interrupt:
                        signal.signal(signum, handler)

        assert interruption is not None
        _replay_signal(
            interruption.signum,
            interruption.frame,
            previous_handlers[interruption.signum],
        )

    @staticmethod
    def _started_at(message: _WorkerMessage) -> datetime:
        if len(message) != 2 or message[0] != "started":
            raise RuntimeError("watchlist quote provider started handshake is invalid")
        value = message[1]
        if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
            raise RuntimeError("watchlist quote provider started handshake is invalid")
        return value.astimezone(UTC)

    @staticmethod
    def _receive(
        receiver: Connection,
        process: BaseProcess,
        deadline: float,
        timeout_seconds: float,
    ) -> _WorkerMessage:
        remaining = deadline - time.monotonic()
        if not receiver.poll(max(0, remaining)):
            raise TimeoutError(f"AKShare Sina quote request exceeded {timeout_seconds}s")
        try:
            message = receiver.recv()
        except EOFError as exc:
            raise RuntimeError(
                f"AKShare Sina quote worker exited without a response ({process.exitcode})"
            ) from exc
        if not isinstance(message, tuple):
            raise RuntimeError("watchlist quote provider worker response is invalid")
        return message

    def _reap(
        self,
        process: BaseProcess,
        *,
        control: Connection,
        process_group_owned: bool,
        terminate_first: bool,
    ) -> bool:
        pid = process.pid
        if pid is None:
            process.close()
            return process_group_owned
        if not process_group_owned:
            process_group_owned = self._claim_queued_ready(
                control,
                expected_pid=pid,
            )
            if process_group_owned:
                self.last_worker_process_group_id = pid
        if terminate_first and process.is_alive():
            process.terminate()
            if process_group_owned:
                _signal_process_group(pid, signal.SIGTERM)
        process.join(timeout=self._termination_grace_seconds)
        if process.is_alive():
            process.terminate()
            if process_group_owned:
                _signal_process_group(pid, signal.SIGTERM)
            process.join(timeout=self._termination_grace_seconds)
        if process.is_alive():
            if process_group_owned:
                _signal_process_group(pid, signal.SIGKILL)
            process.kill()
            process.join(timeout=self._termination_grace_seconds)
        if process_group_owned:
            _signal_process_group(pid, signal.SIGKILL)
        if process.exitcode is None:
            raise RuntimeError("watchlist quote provider worker could not be reaped")
        process.close()
        return process_group_owned

    @staticmethod
    def _claim_queued_ready(control: Connection, *, expected_pid: int) -> bool:
        try:
            if not control.poll(0):
                return False
            message = control.recv()
        except (EOFError, OSError):
            return False
        return message == ("ready", expected_pid)


__all__ = ["AkshareSinaWatchlistQuoteProvider", "ProviderStartedCallback"]
