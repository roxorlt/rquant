"""Configuration-free offline producer, private executor and separate save bridge."""

from __future__ import annotations

import argparse
import os
import re
import signal
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path


def main(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(prog="rquant research-query")
    commands = parser.add_subparsers(dest="action", required=True)
    build = commands.add_parser("build", help="从核验来源发布三表公开快照")
    build.add_argument("--source", type=Path, required=True)
    build.add_argument("--source-sha256", required=True)
    build.add_argument("--source-at", type=datetime.fromisoformat, required=True)
    build.add_argument("--out", type=Path, required=True)
    for action in ("serve", "save-serve"):
        command = commands.add_parser(
            action, help="私有查询服务" if action == "serve" else "独立 PageControl 保存桥"
        )
        command.add_argument("--socket", type=Path, required=True)
        command.add_argument("--web-uid", type=int, required=True)
        command.add_argument("--shared-gid", type=int, required=True)
        command.add_argument(
            "--users", required=True, help="已授权 researcher/admin 的精确账号，逗号分隔"
        )
        if action == "serve":
            command.add_argument("--snapshot-root", type=Path, required=True)
            command.add_argument("--scratch-root", type=Path, required=True)
        else:
            command.add_argument("--outbox", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.action == "build":
        from .snapshot import build_query_snapshot

        manifest = build_query_snapshot(
            args.source, args.out, source_sha256=args.source_sha256, source_at=args.source_at
        )
        print(manifest.model_dump_json())
        return 0
    users = tuple(part.strip() for part in args.users.split(","))
    if (
        not 1 <= len(users) <= 16
        or len(set(users)) != len(users)
        or any(re.fullmatch(r"[A-Za-z0-9._@-]{1,64}", user) is None for user in users)
    ):
        parser.error("users must contain exact distinct trusted account names")
    if args.web_uid == os.geteuid() or args.web_uid < 0 or args.shared_gid < 0:
        parser.error("Web and private service must have distinct identities")
    from .service import QueryPrivateServer

    if args.action == "serve":
        from .executor import QueryExecutor
        from .snapshot import VerifiedQuerySnapshot

        server = QueryPrivateServer(
            args.socket,
            executor=QueryExecutor(VerifiedQuerySnapshot(args.snapshot_root), args.scratch_root),
            allowed_users=frozenset(users),
            trusted_web_uid=args.web_uid,
            shared_gid=args.shared_gid,
        )
    else:
        from rquant.page_control import PageControlConsumer, PageControlOutbox, PageControlService

        if not args.outbox.is_absolute() or args.outbox != args.outbox.resolve():
            parser.error("outbox path must be absolute and canonical")
        outbox = PageControlOutbox(args.outbox)
        os.chmod(outbox.path, 0o600)
        control = PageControlService(
            outbox=outbox,
            consumer=PageControlConsumer(
                outbox=outbox,
                data_dir=args.outbox.parent / "query-state",
                log_dir=args.outbox.parent / "query-events",
            ),
        )
        server = QueryPrivateServer(
            args.socket,
            control=control,
            allowed_users=frozenset(users),
            trusted_web_uid=args.web_uid,
            shared_gid=args.shared_gid,
        )

    def stop_service(signum: int, frame: object) -> None:
        raise KeyboardInterrupt

    previous_handler = signal.signal(signal.SIGTERM, stop_service)
    try:
        server.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        signal.signal(signal.SIGTERM, previous_handler)
    return 0
