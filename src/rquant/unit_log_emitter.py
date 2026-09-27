"""Best-effort, closed lifecycle events from scheduled service processes."""

from __future__ import annotations

import re
import socket
import sys
import time
from collections.abc import Callable
from contextlib import suppress

from rquant.unit_log_reader import LIFECYCLE_MESSAGE, LIFECYCLE_MESSAGE_ID

_JOURNAL_SOCKET = "/run/systemd/journal/socket"
_MAX_DURATION_MS = 7 * 24 * 60 * 60 * 1000
_NUMBER = re.compile(r"(?:0|[1-9][0-9]*)\Z")


def _send_journal(payload: bytes) -> None:
    with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as connection:
        connection.settimeout(0.2)
        connection.sendto(payload, _JOURNAL_SOCKET)


def _payload(event: str, duration_ms: int | None, exit_code: int | None) -> bytes:
    if event not in {"run_started", "run_succeeded", "run_failed"}:
        raise ValueError("invalid lifecycle event")
    if event == "run_started":
        if duration_ms is not None or exit_code is not None:
            raise ValueError("start event has no measurements")
    elif (
        type(duration_ms) is not int
        or not 0 <= duration_ms <= _MAX_DURATION_MS
        or type(exit_code) is not int
        or not 0 <= exit_code <= 255
        or (event == "run_succeeded" and exit_code != 0)
        or (event == "run_failed" and exit_code == 0)
    ):
        raise ValueError("invalid lifecycle measurements")

    fields = [
        f"MESSAGE={LIFECYCLE_MESSAGE}",
        f"MESSAGE_ID={LIFECYCLE_MESSAGE_ID}",
        f"PRIORITY={3 if event == 'run_failed' else 6}",
        f"RQUANT_EVENT={event}",
    ]
    if duration_ms is not None and exit_code is not None:
        fields.extend((f"RQUANT_DURATION_MS={duration_ms}", f"RQUANT_EXIT_CODE={exit_code}"))
    return ("\n".join(fields) + "\n").encode("ascii")


def emit_lifecycle_event(
    event: str,
    *,
    duration_ms: int | None = None,
    exit_code: int | None = None,
    sender: Callable[[bytes], None] | None = None,
) -> None:
    """Send only validated fields; journald loss cannot change the scheduled work result."""
    payload = _payload(event, duration_ms, exit_code)
    with suppress(Exception):
        (sender or _send_journal)(payload)


def run_with_lifecycle(
    run: Callable[[], int],
    *,
    emit: Callable[..., None] | None = None,
    clock: Callable[[], int] = time.monotonic_ns,
) -> int:
    """Wrap one CLI run while preserving its return value and exception behavior."""
    emitter = emit or emit_lifecycle_event
    started_ns = clock()

    def best_effort(event: str, *, exit_code: int | None = None) -> None:
        try:
            if event == "run_started":
                emitter(event)
                return
            elapsed_ms = min(max(0, (clock() - started_ns) // 1_000_000), _MAX_DURATION_MS)
            emitter(event, duration_ms=elapsed_ms, exit_code=exit_code)
        except Exception:
            pass

    best_effort("run_started")
    try:
        result = run()
    except BaseException:
        best_effort("run_failed", exit_code=1)
        raise
    bounded_code = result if type(result) is int and 0 <= result <= 255 else 1
    best_effort("run_succeeded" if result == 0 else "run_failed", exit_code=bounded_code)
    return result


def main(argv: list[str] | None = None) -> int:
    """Internal fixed-argument entry for the existing backup shell script."""
    arguments = sys.argv[1:] if argv is None else argv
    if arguments == ["run_started"]:
        emit_lifecycle_event("run_started")
        return 0
    if (
        len(arguments) != 3
        or arguments[0] not in {"run_succeeded", "run_failed"}
        or _NUMBER.fullmatch(arguments[1]) is None
        or _NUMBER.fullmatch(arguments[2]) is None
    ):
        return 2
    try:
        emit_lifecycle_event(
            arguments[0], duration_ms=int(arguments[1]), exit_code=int(arguments[2])
        )
    except ValueError:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
