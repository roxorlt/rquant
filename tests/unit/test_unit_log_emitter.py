"""Closed lifecycle events sent by the existing daily and backup processes."""

from __future__ import annotations

import socket
from collections.abc import Callable
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pytest

from rquant.unit_log_reader import LIFECYCLE_MESSAGE, LIFECYCLE_MESSAGE_ID


def _fields(payload: bytes) -> dict[str, str]:
    lines = payload.decode("ascii").removesuffix("\n").split("\n")
    assert all("=" in line for line in lines)
    return dict(line.split("=", 1) for line in lines)


@pytest.mark.parametrize(
    ("event", "duration_ms", "exit_code", "priority"),
    [
        ("run_started", None, None, "6"),
        ("run_succeeded", 1234, 0, "6"),
        ("run_failed", 4567, 2, "3"),
    ],
)
def test_emitter_sends_only_closed_lifecycle_fields(
    event: str, duration_ms: int | None, exit_code: int | None, priority: str
) -> None:
    from rquant.unit_log_emitter import emit_lifecycle_event

    sent: list[bytes] = []
    emit_lifecycle_event(event, duration_ms=duration_ms, exit_code=exit_code, sender=sent.append)

    assert len(sent) == 1
    fields = _fields(sent[0])
    assert fields == {
        "MESSAGE": LIFECYCLE_MESSAGE,
        "MESSAGE_ID": LIFECYCLE_MESSAGE_ID,
        "PRIORITY": priority,
        "RQUANT_EVENT": event,
        **({"RQUANT_DURATION_MS": str(duration_ms)} if duration_ms is not None else {}),
        **({"RQUANT_EXIT_CODE": str(exit_code)} if exit_code is not None else {}),
    }
    assert b"password" not in sent[0]
    assert b"/home/" not in sent[0]


@pytest.mark.parametrize(
    ("event", "duration_ms", "exit_code"),
    [
        ("other", None, None),
        ("run_started", 1, None),
        ("run_succeeded", -1, 0),
        ("run_succeeded", 1, 1),
        ("run_failed", 1, 0),
        ("run_failed", 604800001, 1),
        ("run_failed", 1, 256),
    ],
)
def test_emitter_rejects_unregistered_or_unbounded_fields(
    event: str, duration_ms: int | None, exit_code: int | None
) -> None:
    from rquant.unit_log_emitter import emit_lifecycle_event

    sent: list[bytes] = []
    with pytest.raises(ValueError):
        emit_lifecycle_event(
            event, duration_ms=duration_ms, exit_code=exit_code, sender=sent.append
        )
    assert sent == []


@pytest.mark.parametrize("result", [0, 2])
def test_daily_runner_records_started_and_terminal_result(result: int) -> None:
    from rquant.unit_log_emitter import run_with_lifecycle

    events: list[tuple[str, int | None, int | None]] = []
    ticks = iter((100_000_000, 2_600_000_000))

    def emit(event: str, *, duration_ms: int | None = None, exit_code: int | None = None) -> None:
        events.append((event, duration_ms, exit_code))

    assert run_with_lifecycle(lambda: result, emit=emit, clock=lambda: next(ticks)) == result
    assert events == [
        ("run_started", None, None),
        ("run_succeeded" if result == 0 else "run_failed", 2500, result),
    ]


def test_daily_runner_records_failure_without_exception_text() -> None:
    from rquant.unit_log_emitter import run_with_lifecycle

    events: list[tuple[str, int | None, int | None]] = []
    ticks = iter((100_000_000, 2_600_000_000))

    def emit(event: str, *, duration_ms: int | None = None, exit_code: int | None = None) -> None:
        events.append((event, duration_ms, exit_code))

    def fail() -> int:
        raise RuntimeError("private-token=/home/user/secret")

    with pytest.raises(RuntimeError, match="private-token"):
        run_with_lifecycle(fail, emit=emit, clock=lambda: next(ticks))
    assert events == [("run_started", None, None), ("run_failed", 2500, 1)]


@pytest.mark.parametrize("result", [0, 2])
def test_failed_journal_send_never_changes_daily_result(result: int) -> None:
    from rquant.unit_log_emitter import run_with_lifecycle

    calls = 0

    def broken_emit(
        _event: str, *, duration_ms: int | None = None, exit_code: int | None = None
    ) -> None:
        nonlocal calls
        calls += 1
        raise OSError("journald unavailable")

    assert run_with_lifecycle(lambda: result, emit=broken_emit) == result
    assert calls == 2


def test_failed_journal_send_never_masks_daily_exception() -> None:
    from rquant.unit_log_emitter import run_with_lifecycle

    def broken_emit(
        _event: str, *, duration_ms: int | None = None, exit_code: int | None = None
    ) -> None:
        raise RuntimeError("journald unavailable")

    def fail() -> int:
        raise ValueError("daily failed")

    with pytest.raises(ValueError, match="daily failed"):
        run_with_lifecycle(fail, emit=broken_emit)


def test_emitter_uses_local_journal_datagram_boundary(monkeypatch: pytest.MonkeyPatch) -> None:
    import rquant.unit_log_emitter as module

    # macOS AF_UNIX rejects pytest's deep default tmp path.
    with TemporaryDirectory(prefix="rquant-journal-", dir="/private/tmp") as directory:
        destination = Path(directory) / "journal.sock"
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as receiver:
            receiver.bind(str(destination))
            receiver.settimeout(1)
            monkeypatch.setattr(module, "_JOURNAL_SOCKET", str(destination))

            module.emit_lifecycle_event("run_failed", duration_ms=20, exit_code=1)

            payload = receiver.recv(4096)
    assert _fields(payload)["MESSAGE_ID"] == LIFECYCLE_MESSAGE_ID
    assert _fields(payload)["RQUANT_EVENT"] == "run_failed"


@pytest.mark.parametrize("exit_code", [0, 2])
def test_internal_cli_argument_contract(exit_code: int, monkeypatch: pytest.MonkeyPatch) -> None:
    import rquant.unit_log_emitter as module

    events: list[tuple[str, int, int]] = []

    def fake_emit(
        event: str,
        *,
        duration_ms: int | None = None,
        exit_code: int | None = None,
        sender: Callable[[bytes], None] | None = None,
    ) -> None:
        del sender
        assert duration_ms is not None and exit_code is not None
        events.append((event, duration_ms, exit_code))

    monkeypatch.setattr(module, "emit_lifecycle_event", fake_emit)
    event = "run_succeeded" if exit_code == 0 else "run_failed"
    assert module.main([event, "2500", str(exit_code)]) == 0
    assert events == [(event, 2500, exit_code)]


def test_run_daily_cli_uses_lifecycle_boundary(monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant import cli, unit_log_emitter

    observed: list[str] = []
    monkeypatch.setattr("rquant.config.get_settings", lambda: object())
    monkeypatch.setattr(cli, "cmd_run_daily", lambda _args: 2)

    def capture(run: Callable[[], int]) -> int:
        observed.append("wrapped")
        return run()

    monkeypatch.setattr(unit_log_emitter, "run_with_lifecycle", capture)
    with patch("sys.argv", ["rquant", "run-daily", "--no-ingest"]):
        assert cli.main() == 2
    assert observed == ["wrapped"]
