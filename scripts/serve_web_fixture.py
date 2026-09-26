"""Serve the web API over a Serving root with a pinned clock (browser tests, screenshots).

The browser tests read a synthetic generation dated 2026-09-24, or a copy of a replayed
production generation; what the pages show depends on "now" (the trading day, the
session phase, data age). ``--now`` starts the API's clock at that instant and lets it
run from there, so every run sees the same day while newly published generations still
become current. Production never uses this script: ``rquant web-serve`` reads the real
clock.

    uv run python scripts/serve_web_fixture.py --root /tmp/serving \\
        --now 2026-09-24T07:36:00Z --bind 127.0.0.1:18768
"""

from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))


def running_clock(start: datetime, monotonic: Callable[[], float] = time.monotonic):
    """A clock that reads ``start`` now and advances in real time."""

    origin = monotonic()

    def now() -> datetime:
        return start + timedelta(seconds=monotonic() - origin)

    return now


def _instant(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("--now needs a time zone, e.g. 2026-09-24T07:36:00Z")
    return parsed.astimezone(UTC)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--root", required=True, type=Path, help="Serving root to read")
    parser.add_argument("--now", type=_instant, help="start the clock here (ISO 8601 with zone)")
    parser.add_argument("--bind", default="127.0.0.1:18768", help="loopback host:port")
    parser.add_argument("--stale-after", type=float, default=None, help="freshness budget (s)")
    args = parser.parse_args(argv)

    import uvicorn

    from rquant.web.app import create_app
    from rquant.web.settings import WebSettings

    values: dict[str, object] = {"serving_root": args.root, "bind": args.bind}
    if args.stale_after is not None:
        values["stale_after_seconds"] = args.stale_after
    settings = WebSettings.model_validate(values)
    clock = running_clock(args.now) if args.now is not None else (lambda: datetime.now(UTC))
    uvicorn.run(
        create_app(settings, clock=clock),
        host=settings.bind_host,
        port=settings.bind_port,
        workers=1,
        proxy_headers=False,
        server_header=False,
        access_log=False,
        log_level="warning",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
