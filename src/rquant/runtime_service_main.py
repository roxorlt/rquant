"""Process entrypoint for one allow-listed isolated runtime service."""

from __future__ import annotations

import argparse
import re
import signal
from collections.abc import Sequence
from pathlib import Path
from threading import Event
from types import FrameType

from rquant.runtime_service_entrypoint import (
    RuntimeServiceKind,
    RuntimeServiceRegistry,
    load_runtime_service_manifest,
    run_runtime_service_manifest,
)


def _absolute_path(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise argparse.ArgumentTypeError("runtime paths must be absolute")
    return path


def _commit_sha(value: str) -> str:
    if re.fullmatch(r"[0-9a-f]{40}", value) is None:
        raise argparse.ArgumentTypeError("expected commit must be a full lowercase Git SHA")
    return value


def build_builtin_registry() -> RuntimeServiceRegistry:
    from rquant.runtime_service_builtin import build_builtin_registry as factory

    return factory()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one isolated rQuant service")
    parser.add_argument("--manifest", required=True, type=_absolute_path)
    parser.add_argument("--control-root", required=True, type=_absolute_path)
    parser.add_argument("--expected-commit", required=True, type=_commit_sha)
    parser.add_argument(
        "--expected-kind",
        type=RuntimeServiceKind,
        choices=tuple(RuntimeServiceKind),
        help="Reject manifests not admitted by this systemd unit template",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one service step and stop; intended for validation only",
    )
    return parser


def run(args: argparse.Namespace) -> int:
    manifest = load_runtime_service_manifest(
        args.manifest,
        expected_commit=args.expected_commit,
    )
    if args.expected_kind is not None and manifest.service_kind is not args.expected_kind:
        raise ValueError("runtime manifest kind is not admitted by this systemd unit")
    stop_event = Event()

    def request_stop(_signum: int, _frame: FrameType | None) -> None:
        stop_event.set()

    previous_handlers = {
        signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)
    }
    try:
        for signum in previous_handlers:
            signal.signal(signum, request_stop)
        run_runtime_service_manifest(
            manifest,
            registry=build_builtin_registry(),
            control_root=args.control_root,
            stop_event=stop_event,
            max_iterations=1 if args.once else None,
        )
    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    return run(build_parser().parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["build_builtin_registry", "build_parser", "main", "run"]
