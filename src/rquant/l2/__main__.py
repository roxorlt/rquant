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
        )
        status = collector.run()
        print(status.model_dump_json())
        return 1 if status.state in {"blocked", "disconnected"} else 0
    except (CacheError, SubscriptionError) as error:
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


if __name__ == "__main__":
    raise SystemExit(main())
