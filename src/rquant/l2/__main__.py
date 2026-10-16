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


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Seed/backfill once, then passively receive L2 candidates")
    parser.add_argument("--cache-root", type=Path, required=True)
    parser.add_argument("--bootstrap-file", type=Path, required=True)
    parser.add_argument("--order-template", type=Path, required=True)
    parser.add_argument("--trade-template", type=Path, required=True)
    parser.add_argument("--duration-seconds", type=float, default=3600)
    parser.add_argument("--trade-pages", type=int, choices=(1, 2, 3), default=1)
    parser.add_argument("--order-pages", type=int, choices=(1, 2, 3), default=1)
    parser.add_argument("--max-reconnects", type=int, choices=(0, 1, 2), default=0)
    parser.add_argument("--reconnect-delay-seconds", type=float, default=60)
    args = parser.parse_args(argv)
    if (not math.isfinite(args.duration_seconds) or not 0 < args.duration_seconds <= 21600
            or not math.isfinite(args.reconnect_delay_seconds) or args.reconnect_delay_seconds < 60):
        parser.error("invalid duration or reconnect delay")
    stop = threading.Event()

    def request_stop(signum: int, frame: FrameType | None) -> None:
        stop.set()

    handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        for sig in handlers:
            signal.signal(sig, request_stop)
        from rquant.l2.feed import FeedCollector

        collector = FeedCollector(
            RawL2Cache(args.cache_root), bootstrap_path=args.bootstrap_file,
            order_template_path=args.order_template, trade_template_path=args.trade_template,
            duration_seconds=args.duration_seconds, trade_pages=args.trade_pages,
            order_pages=args.order_pages, max_reconnects=args.max_reconnects,
            reconnect_delay_seconds=args.reconnect_delay_seconds, stop_event=stop,
        )
        status = collector.run()
        print(status.model_dump_json())
        return 1 if status.state in {"blocked", "disconnected"} else 0
    except CacheError as error:
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
