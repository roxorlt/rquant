"""Run already queued read-only backfill plan jobs from an explicit Lab outbox."""

from __future__ import annotations

import argparse
import json
import signal
import sys
from collections.abc import Sequence
from pathlib import Path
from threading import Event

from rquant.backfill_plan_jobs import BackfillPlanJobStore, BackfillPlanJobWorker


def _absolute_path(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise argparse.ArgumentTypeError("path must be absolute")
    return path


def _state_path(value: str) -> Path:
    path = _absolute_path(value)
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
    parser = argparse.ArgumentParser(description="Run queued read-only backfill plan jobs")
    parser.add_argument("--state-path", required=True, type=_state_path)
    parser.add_argument("--plan-directory", required=True, type=_absolute_path)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--once", action="store_true", help="claim at most one job, then exit")
    mode.add_argument("--poll", action="store_true", help="wait for queued jobs until stopped")
    parser.add_argument("--poll-interval", type=_poll_interval)
    return parser


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
        store = BackfillPlanJobStore(
            state_path=args.state_path,
            plan_directory=args.plan_directory,
        )
        worker = BackfillPlanJobWorker(store)
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
        print(f"backfill plan runner failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    finally:
        for signum, previous in previous_handlers.items():
            signal.signal(signum, previous)


if __name__ == "__main__":
    raise SystemExit(main())
