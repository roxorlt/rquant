"""Explicit local collector entry; cached HTTP routes cannot invoke it."""

from __future__ import annotations

import argparse
import json
import math
import signal
import threading
from collections.abc import Sequence
from pathlib import Path
from types import FrameType

from rquant.l2.cache import CacheError, RawL2Cache
from rquant.l2.frame_tap import FrameTapConfig, FrameTapError, RawFrameTap, tap_directory
from rquant.l2.subscriptions import SubscriptionError, load_capacity_policy
from rquant.l2.symbols import MAX_SYMBOLS, normalize_symbols


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Seed/backfill once, then passively receive L2 candidates")
    parser.add_argument("--cache-root", type=Path, required=True)
    parser.add_argument("--bootstrap-file", type=Path, required=True)
    parser.add_argument("--order-template", type=Path, required=True)
    parser.add_argument("--trade-template", type=Path, required=True)
    targets = parser.add_mutually_exclusive_group()
    targets.add_argument('--control-root', type=Path)
    targets.add_argument('--symbols', nargs='+')
    capacity = parser.add_mutually_exclusive_group()
    capacity.add_argument('--max-symbols', type=int)
    capacity.add_argument('--capacity-file', type=Path)
    runtime = parser.add_mutually_exclusive_group()
    runtime.add_argument("--duration-seconds", type=float, default=3600)
    runtime.add_argument("--resident", action="store_true")
    parser.add_argument("--trade-pages", type=int, choices=(1, 2, 3), default=1)
    parser.add_argument("--order-pages", type=int, choices=(1, 2, 3), default=1)
    parser.add_argument("--max-reconnects", type=int, choices=(0, 1, 2), default=0)
    parser.add_argument("--reconnect-delay-seconds", type=float, default=60)
    parser.add_argument("--connections", type=int, choices=tuple(range(1, 17)), default=1,
                        help="batched upstream connections (contiguous stock chunks); "
                             "1 keeps the legacy single-connection behaviour")
    parser.add_argument("--subscribe-batch-stocks", type=int, default=0,
                        help="subscription batch size in stocks (0 disables the batch barrier); "
                             "a non-zero value also paces against the observed seed latency")
    parser.add_argument("--seed-retry-limit", type=int, choices=(0, 1, 2, 3), default=0,
                        help="deferred seed retries after the 15s guard (0 keeps legacy)")
    parser.add_argument("--seed-retry-cooldown-seconds", type=float, default=20.0)
    parser.add_argument("--receive-pump", action="store_true",
                        help="decouple socket framing from decode/persist with a reader thread; "
                             "requires the wall clock, do not use with the virtual replay clock")
    parser.add_argument("--pump-queue-frames", type=int, default=65536)
    parser.add_argument("--pump-priority-frames", type=int, default=4096)
    parser.add_argument("--frame-tap-dir", type=Path,
                        help="record raw frames into a new child directory (also enabled by "
                             "RQUANT_L2_FRAME_TAP=1 + RQUANT_L2_FRAME_TAP_DIR)")
    parser.add_argument("--frame-tap-segment-bytes", type=int,
                        help="tap segment size override; default 256 MiB")
    args = parser.parse_args(argv)
    if (not math.isfinite(args.duration_seconds) or not 0 < args.duration_seconds <= 21600
            or not math.isfinite(args.reconnect_delay_seconds) or args.reconnect_delay_seconds < 60):
        parser.error("invalid duration or reconnect delay")
    if args.max_symbols is not None and not 1 <= args.max_symbols <= MAX_SYMBOLS:
        parser.error('invalid symbol capacity')
    if args.symbols is not None:
        try:
            args.symbols = normalize_symbols(args.symbols)
        except ValueError:
            parser.error('invalid symbols')
    try:
        if args.frame_tap_dir is not None:
            tap_options = ({"segment_bytes": args.frame_tap_segment_bytes}
                           if args.frame_tap_segment_bytes is not None else {})
            tap_config = FrameTapConfig(directory=tap_directory(args.frame_tap_dir),
                                        **tap_options)
        else:
            tap_config = FrameTapConfig.from_env()
            if tap_config is not None:
                tap_config = FrameTapConfig(
                    directory=tap_directory(tap_config.directory),
                    segment_bytes=(args.frame_tap_segment_bytes
                                   if args.frame_tap_segment_bytes is not None
                                   else tap_config.segment_bytes),
                    queue_bytes=tap_config.queue_bytes,
                    queue_records=tap_config.queue_records,
                    fsync_interval_seconds=tap_config.fsync_interval_seconds)
        frame_tap = RawFrameTap(tap_config) if tap_config is not None else None
    except FrameTapError:
        parser.error("invalid frame tap configuration")
    stop = threading.Event()

    def request_stop(signum: int, frame: FrameType | None) -> None:
        stop.set()

    handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        max_symbols = (load_capacity_policy(args.capacity_file).max_symbols if args.capacity_file
                       else args.max_symbols if args.max_symbols is not None else 20)
        for sig in handlers:
            signal.signal(sig, request_stop)
        from rquant.l2.feed import FeedCollector

        collector = FeedCollector(
            RawL2Cache(args.cache_root), bootstrap_path=args.bootstrap_file,
            order_template_path=args.order_template, trade_template_path=args.trade_template,
            duration_seconds=None if args.resident else args.duration_seconds, trade_pages=args.trade_pages,
            order_pages=args.order_pages, max_reconnects=args.max_reconnects,
            reconnect_delay_seconds=args.reconnect_delay_seconds, stop_event=stop,
            control_root=args.control_root, max_symbols=max_symbols, symbols=args.symbols,
            frame_tap=frame_tap,
            connection_count=args.connections,
            subscribe_batch_stocks=args.subscribe_batch_stocks,
            seed_retry_limit=args.seed_retry_limit,
            seed_retry_cooldown_seconds=args.seed_retry_cooldown_seconds,
            receive_pump=args.receive_pump,
            pump_queue_frames=args.pump_queue_frames,
            pump_priority_frames=args.pump_priority_frames,
        )
        status = collector.run()
        print(status.model_dump_json())
        return 1 if status.state in {"blocked", "disconnected"} else 0
    except (CacheError, SubscriptionError, FrameTapError) as error:
        print(json.dumps({"error_code": error.code}))
        return 1
    except ValueError:
        print(json.dumps({"error_code": "input_error"}))
        return 1
    except Exception:
        print(json.dumps({"error_code": "collector_error"}))
        return 1
    finally:
        for sig, handler in handlers.items():
            signal.signal(sig, handler)
        if frame_tap is not None:
            frame_tap.close()


if __name__ == "__main__":
    raise SystemExit(main())
