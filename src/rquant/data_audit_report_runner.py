"""Run previously queued read-only data audit reports with explicit local paths."""

from __future__ import annotations

import argparse
import json
import os
import signal
import stat
import sys
from collections.abc import Sequence
from pathlib import Path
from threading import Event

from rquant.data_audit_report_jobs import DataAuditReportJobStore, DataAuditReportJobWorker


def _absolute_canonical_path(value: str) -> Path:
    path = Path(value)
    if (
        not path.is_absolute()
        or path != Path(os.path.abspath(path))
        or path.parent.resolve(strict=False) != path.parent
    ):
        raise argparse.ArgumentTypeError("path must be absolute and canonical")
    return path


def _state_path(value: str) -> Path:
    path = _absolute_canonical_path(value)
    if path.suffix != ".sqlite" or path.is_symlink():
        raise argparse.ArgumentTypeError("job state must be a non-symlink .sqlite path")
    return path


def _poll_interval(value: str) -> float:
    try:
        interval = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("poll interval must be a number") from exc
    if not 1 <= interval <= 60:
        raise argparse.ArgumentTypeError("poll interval must be between 1 and 60 seconds")
    return interval


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run queued read-only data audit report jobs")
    parser.add_argument("--state-path", required=True, type=_state_path)
    parser.add_argument("--report-directory", required=True, type=_absolute_canonical_path)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--once", action="store_true", help="claim at most one job, then exit")
    mode.add_argument("--poll", action="store_true", help="wait for queued jobs until stopped")
    parser.add_argument("--poll-interval", type=_poll_interval)
    return parser


def _require_safe_paths(state_path: Path, report_directory: Path) -> None:
    if (
        report_directory == state_path
        or report_directory.is_symlink()
        or (report_directory.exists() and not report_directory.is_dir())
    ):
        raise ValueError("report directory must be a real directory distinct from job state")
    if state_path.is_symlink():
        raise ValueError("job state cannot be a symlink")
    if not state_path.exists():
        return
    descriptor = os.open(state_path, os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(descriptor, "rb") as handle:
        observed = os.fstat(handle.fileno())
        if not stat.S_ISREG(observed.st_mode):
            raise ValueError("job state must be a regular SQLite file")
        if observed.st_size and handle.read(16) != b"SQLite format 3\x00":
            raise ValueError("job state is not a SQLite file")


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if args.once and args.poll_interval is not None:
        parser.error("--poll-interval requires --poll")
    poll_interval = args.poll_interval if args.poll_interval is not None else 5.0

    stopped = Event()
    previous_handlers = {}

    def request_stop(_signum: int, _frame: object) -> None:
        stopped.set()

    for signum in (signal.SIGINT, signal.SIGTERM):
        previous_handlers[signum] = signal.signal(signum, request_stop)

    try:
        _require_safe_paths(args.state_path, args.report_directory)
        store = DataAuditReportJobStore(
            state_path=args.state_path,
            report_directory=args.report_directory,
        )
        worker = DataAuditReportJobWorker(store)
        idle_reported = False
        while not stopped.is_set():
            receipt = worker.run_one()
            if receipt is None:
                if args.once:
                    print('{"status":"idle"}', flush=True)
                    return 0
                if not idle_reported:
                    print('{"status":"idle"}', flush=True)
                    idle_reported = True
            else:
                idle_reported = False
                print(
                    json.dumps(
                        {
                            "task_id": receipt.task_id,
                            "status": receipt.status,
                            "error_code": receipt.error_code,
                        },
                        separators=(",", ":"),
                    ),
                    flush=True,
                )
                if args.once:
                    return 0 if receipt.status == "succeeded" else 1
            stopped.wait(poll_interval)
        print('{"status":"stopped"}', flush=True)
        return 0
    except Exception as exc:
        print(f"data audit report runner failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    finally:
        for signum, previous in previous_handlers.items():
            signal.signal(signum, previous)


if __name__ == "__main__":
    raise SystemExit(main())
