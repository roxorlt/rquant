"""Process entrypoint for one allow-listed isolated runtime service."""

from __future__ import annotations

import argparse
import re
import signal
import subprocess
from collections.abc import Sequence
from pathlib import Path
from threading import Event
from types import FrameType

from rquant.runtime_capabilities import load_systemd_runtime_capabilities
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


def _generation_hash(value: str) -> str:
    if re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise argparse.ArgumentTypeError("expected generation must be a full lowercase SHA-256")
    return value


def build_builtin_registry() -> RuntimeServiceRegistry:
    from rquant.runtime_service_builtin import build_builtin_registry as factory

    return factory()


def resolve_checkout_commit(root: Path | None = None) -> str:
    checkout = (root or Path.cwd()).resolve()
    try:
        revision = subprocess.run(
            [
                "/usr/bin/git",
                "-C",
                str(checkout),
                "rev-parse",
                "--verify",
                "HEAD^{commit}",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError as exc:
        raise RuntimeError("runtime checkout commit cannot be verified") from exc
    commit = revision.stdout.strip()
    if revision.returncode != 0 or re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        raise RuntimeError("runtime checkout commit cannot be verified")
    try:
        status = subprocess.run(
            [
                "/usr/bin/git",
                "-C",
                str(checkout),
                "status",
                "--porcelain=v1",
                "--untracked-files=all",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError as exc:
        raise RuntimeError("runtime checkout cleanliness cannot be verified") from exc
    if status.returncode != 0:
        raise RuntimeError("runtime checkout cleanliness cannot be verified")
    if status.stdout.strip():
        raise RuntimeError("runtime checkout source tree must be clean")
    return commit


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one isolated rQuant service")
    parser.add_argument("--manifest", required=True, type=_absolute_path)
    parser.add_argument("--control-root", required=True, type=_absolute_path)
    parser.add_argument("--expected-commit", required=True, type=_commit_sha)
    parser.add_argument("--expected-generation", required=True, type=_generation_hash)
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
    actual_commit = resolve_checkout_commit()
    if actual_commit != args.expected_commit:
        raise RuntimeError("runtime checkout commit does not match expected commit")
    manifest = load_runtime_service_manifest(
        args.manifest,
        expected_commit=args.expected_commit,
        expected_generation=args.expected_generation,
    )
    if args.expected_kind is not None and manifest.service_kind is not args.expected_kind:
        raise ValueError("runtime manifest kind is not admitted by this systemd unit")
    load_systemd_runtime_capabilities(
        manifest.service_kind,
        expected_generation=args.expected_generation,
    )
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


__all__ = [
    "build_builtin_registry",
    "build_parser",
    "main",
    "resolve_checkout_commit",
    "run",
]
