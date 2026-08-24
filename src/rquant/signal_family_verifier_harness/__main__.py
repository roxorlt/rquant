"""The child entry point: read one request, emit one bounded response, exit.

`authority.md` L1445 is unforgiving about what leaves this process: *it emits exactly one
bounded canonical IPC result and exits; extra output, timeout, signal death, nonzero status,
open pipe, or inherited-descriptor mismatch rejects*. So the failure path here writes
nothing at all — not to the result pipe, not to stdout, not to stderr. A rejected run is a
silent nonzero exit, and the root names the reason from its own side.

The two descriptor numbers arrive in the sanitized environment the root froze; the request
itself only ever travels on the pipe.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Final

from ._request import MAX_REQUEST_BYTES, build_child_response, parse_child_request
from ._surfaces import exercise_vector

REQUEST_FD_ENV_KEY: Final[str] = "RQUANT_SIGNAL_FAMILY_REQUEST_FD"
RESULT_FD_ENV_KEY: Final[str] = "RQUANT_SIGNAL_FAMILY_RESULT_FD"

_READ_CHUNK: Final[int] = 65_536

EXIT_OK: Final[int] = 0
EXIT_REQUEST_REJECTED: Final[int] = 2
EXIT_EXERCISE_FAILED: Final[int] = 3
EXIT_WRITE_FAILED: Final[int] = 4


def _descriptor(name: str) -> int:
    raw = os.environ.get(name, "")
    if not raw.isdigit():
        raise ValueError(f"{name} does not carry a descriptor number")
    return int(raw)


def read_request(descriptor: int) -> bytes:
    """Drain the request pipe under the same bound the root wrote it with."""

    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = os.read(descriptor, _READ_CHUNK)
        if not chunk:
            break
        total += len(chunk)
        if total > MAX_REQUEST_BYTES:
            raise ValueError("the request exceeds its bounded size")
        chunks.append(chunk)
    return b"".join(chunks)


def run_child(request_bytes: bytes, *, workspace_root: Path) -> bytes:
    """Turn the request bytes into the one canonical response, or raise."""

    request = parse_child_request(request_bytes)
    results: dict[str, Any] = {}
    for vector in request.vectors:
        results[vector.vector_id] = exercise_vector(vector, workspace_root)
    return build_child_response(request, results)


def main() -> int:
    try:
        request_fd = _descriptor(REQUEST_FD_ENV_KEY)
        result_fd = _descriptor(RESULT_FD_ENV_KEY)
        request_bytes = read_request(request_fd)
        os.close(request_fd)
    except (OSError, ValueError):
        return EXIT_REQUEST_REJECTED
    try:
        response = run_child(request_bytes, workspace_root=Path(os.getcwd()))
    except BaseException:  # noqa: BLE001 - a rejected run must stay silent, see the docstring
        return EXIT_EXERCISE_FAILED
    try:
        written = 0
        while written < len(response):
            written += os.write(result_fd, response[written:])
        os.close(result_fd)
    except OSError:
        return EXIT_WRITE_FAILED
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())


__all__ = [
    "EXIT_EXERCISE_FAILED",
    "EXIT_OK",
    "EXIT_REQUEST_REJECTED",
    "EXIT_WRITE_FAILED",
    "REQUEST_FD_ENV_KEY",
    "RESULT_FD_ENV_KEY",
    "main",
    "read_request",
    "run_child",
]
