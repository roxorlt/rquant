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
import os
import secrets
import signal
import sys
import tempfile
import time
from collections.abc import Callable, Sequence
from contextlib import ExitStack
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import FrameType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rquant.page_control import PageControlService
    from rquant.web.models.screen_history import ScreenQueryAction, ScreenQueryReadData

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))


def running_clock(
    start: datetime, monotonic: Callable[[], float] = time.monotonic
) -> Callable[[], datetime]:
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


def _prepare_proxy_proof(path: Path) -> None:
    """Give the local browser proxy and API one fresh, private synthetic proof."""

    if not path.is_absolute() or ".." in path.parts:
        raise ValueError("fixture proxy proof path must be absolute and canonical")
    path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{secrets.token_hex(8)}")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o400)
    try:
        with os.fdopen(descriptor, "w", encoding="ascii") as output:
            output.write(secrets.token_hex(32))
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


class _FixtureScreenClient:
    """Original dispatcher with the explicit local browser test actor."""

    def __init__(self, control: PageControlService) -> None:
        self.control = control

    def request(
        self, action: ScreenQueryAction, *, authenticated_actor_id: str
    ) -> ScreenQueryReadData:
        from rquant.screen.query_admission import dispatch_screen_query_action

        return dispatch_screen_query_action(
            self.control,
            authenticated_actor_id=authenticated_actor_id,
            allowed_users=frozenset({"e2e"}),
            action=action,
        )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--root", required=True, type=Path, help="Serving root to read")
    parser.add_argument("--now", type=_instant, help="start the clock here (ISO 8601 with zone)")
    parser.add_argument("--bind", default="127.0.0.1:18768", help="loopback host:port")
    parser.add_argument("--stale-after", type=float, default=None, help="freshness budget (s)")
    parser.add_argument(
        "--native-health-fixture",
        action="store_true",
        help="publish the original complete synthetic health owners at the fixed clock",
    )
    parser.add_argument(
        "--private-fixture",
        action="store_true",
        help="simulate private ingress for local browser tests only",
    )
    parser.add_argument(
        "--proxy-proof-file",
        type=Path,
        help="private synthetic proxy proof shared with the local browser test proxy",
    )
    args = parser.parse_args(argv)

    if args.private_fixture:
        if args.now is None or not args.bind.startswith("127.0.0.1:"):
            parser.error("--private-fixture requires a pinned clock and IPv4 loopback bind")
        if args.proxy_proof_file is None:
            parser.error("--private-fixture requires --proxy-proof-file")
    elif args.proxy_proof_file is not None:
        parser.error("--proxy-proof-file requires --private-fixture")
    if args.native_health_fixture and not args.private_fixture:
        parser.error("--native-health-fixture requires --private-fixture and --now")

    def stop(_signal: int, _frame: FrameType | None) -> None:
        raise SystemExit(0)

    previous = signal.signal(signal.SIGTERM, stop)
    try:
        with ExitStack() as owner:
            os.environ["RQUANT_DISABLE_DOTENV"] = "1"
            private_root = None
            if args.private_fixture:
                _prepare_proxy_proof(args.proxy_proof_file)
                temporary_parent = (
                    args.proxy_proof_file.parent
                    if args.native_health_fixture
                    else Path("/tmp").resolve()
                )
                private_root = Path(
                    owner.enter_context(
                        tempfile.TemporaryDirectory(prefix="rq-web-", dir=temporary_parent)
                    )
                )
                os.environ.update(
                    {
                        "TUSHARE_TOKEN_MAIN": "synthetic-test-token-0000000000000000",
                        "DATA_DIR": str(private_root / "data"),
                        "DUCKDB_PATH": str(private_root / "unused-primary.duckdb"),
                        "PARQUET_DIR": str(private_root / "parquet"),
                        "LOG_DIR": str(private_root / "logs"),
                    }
                )

            import uvicorn

            from rquant.web.app import create_app
            from rquant.web.settings import DEFAULT_BIND, WebSettings

            values: dict[str, object] = {"serving_root": args.root, "bind": args.bind}
            clock = (
                (lambda: args.now)
                if args.private_fixture
                else (
                    running_clock(args.now) if args.now is not None else (lambda: datetime.now(UTC))
                )
            )
            screen_client = None
            if args.private_fixture:
                # The proxy supplies the original private ingress proof and identity.
                values["bind"] = DEFAULT_BIND
                values["ingress_socket_path"] = (
                    args.root.resolve().parent / "web-private-fixture.sock"
                )
                values["proxy_proof_file"] = args.proxy_proof_file

                if args.native_health_fixture:
                    from tests.fixtures.react_platform_health.native_health_fixture import (
                        publish_native_health_fixture,
                    )

                    publish_native_health_fixture(args.root, private_root / "health", at=args.now)

                    # Health observations and page.clock use the same explicit synthetic instant.
                    def health_clock() -> datetime:
                        return args.now

                    clock = health_clock
                else:
                    from rquant.page_control import (
                        PageControlConsumer,
                        PageControlOutbox,
                        PageControlService,
                    )
                    from rquant.screen.query_admission import (
                        ScreenQueryExecutor,
                        ScreenQueryPrivateConfig,
                    )
                    from rquant.screen.query_history import (
                        ScreenQueryHistory,
                        prepare_private_screen_outbox,
                    )

                    socket_path = private_root / "screen-query.sock"
                    executor = ScreenQueryExecutor(
                        ScreenQueryPrivateConfig(
                            socket_path=socket_path,
                            trusted_web_uid=os.geteuid() + 1,
                            shared_gid=os.getegid(),
                            allowed_users=frozenset({"e2e"}),
                            serving_root=args.root,
                        ),
                        clock=clock,
                    )
                    owner.callback(executor.tracker.close)
                    outbox_path = private_root / "private" / "page-control.sqlite3"
                    prepare_private_screen_outbox(outbox_path)
                    outbox = PageControlOutbox(outbox_path)
                    history = ScreenQueryHistory(outbox, cursor_key=executor.cursor_key)
                    control = PageControlService(
                        outbox=outbox,
                        consumer=PageControlConsumer(
                            outbox=outbox,
                            data_dir=private_root / "data",
                            log_dir=private_root / "logs",
                            screen_query_history=history,
                            screen_query_executor=executor,
                            daily_writer_capability=executor.daily_writer_capability,
                            daily_run_evidence=executor.daily_run_evidence,
                            clock=lambda: datetime.now(UTC),
                        ),
                    )
                    # Same accepted in-process test transport as DirectScreenGateway;
                    # these endpoint IDs do not claim a physical distinct-UID test.
                    screen_client = _FixtureScreenClient(control)
                    values.update(
                        screen_query_users=frozenset({"e2e"}),
                        screen_query_socket_path=socket_path,
                        screen_query_service_uid=os.geteuid() + 1,
                        screen_query_shared_gid=os.getegid(),
                    )
            if args.stale_after is not None:
                values["stale_after_seconds"] = args.stale_after
            settings = WebSettings.model_validate(values)
            bind_host, bind_port = args.bind.rsplit(":", 1)
            uvicorn.run(
                create_app(settings, clock=clock, screen_query_client=screen_client),
                host=bind_host,
                port=int(bind_port),
                workers=1,
                proxy_headers=False,
                server_header=False,
                access_log=False,
                log_level="warning",
            )
    finally:
        signal.signal(signal.SIGTERM, previous)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
