"""Explicit offline history, fixed local due scheduler, and trusted private admission."""

from __future__ import annotations

import argparse
import os
from datetime import UTC, date, datetime
from pathlib import Path

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from rquant.factor.result_artifact import _open_private_root, _root_path
from rquant.factor.run_configuration import FactorRunFileReference, open_factor_run_configuration
from rquant.factor.tracking import FactorTrackingIdentity, FactorTrackingStore
from rquant.factor.tracking_runner import FactorTrackingRunner
from rquant.strict_json import canonical_json_bytes


def build_factor_tracking_scheduler(runner: FactorTrackingRunner) -> BlockingScheduler:
    scheduler = BlockingScheduler(timezone="Asia/Shanghai")
    scheduler.add_job(
        runner.due_tick,
        trigger=CronTrigger(day_of_week="mon-fri", hour=18, minute=40, timezone="Asia/Shanghai"),
        id="factor-tracking-due-v1",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=3600,
    )
    return scheduler


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="显式私有配置的因子持续跟踪（不联网、不安装生产定时器）"
    )
    parser.add_argument(
        "action", choices=("initialize", "run-history", "due", "schedule", "snapshot", "serve")
    )
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--reference", help="完整sealed FactorRunFileReference JSON")
    parser.add_argument("--tracking-identity", help="完整FactorTrackingIdentity JSON")
    parser.add_argument("--factor-id")
    parser.add_argument("--target-end", type=date.fromisoformat)
    parser.add_argument("--control-root", type=Path)
    parser.add_argument("--socket", type=Path)
    parser.add_argument("--web-uid", type=int)
    parser.add_argument("--shared-gid", type=int)
    parser.add_argument("--tracking-users", default="")
    parser.add_argument("--enabled", action="store_true")
    args = parser.parse_args(argv)
    root = _root_path(args.root)
    descriptor = _open_private_root(root)
    os.close(descriptor)
    if args.action == "initialize":
        result = FactorTrackingStore(root / "factor-tracking.sqlite").initialize()
        print(result.model_dump_json())
        return 0
    if args.reference is None or args.tracking_identity is None:
        parser.error("必须提供实际sealed配置引用及固定跟踪库身份")
    reference = FactorRunFileReference.model_validate_json(args.reference)
    identity = FactorTrackingIdentity.model_validate_json(args.tracking_identity)
    runner = FactorTrackingRunner(root, reference, identity)
    if args.action == "run-history":
        if args.factor_id is None:
            parser.error("历史运行必须显式指定因子ID")
        result = runner.run_history(args.factor_id, target_end=args.target_end)
        print(result.model_dump_json())
        return 0 if result.status != "paused" else 2
    if args.action == "due":
        results = runner.due_tick()
        print(canonical_json_bytes([r.model_dump(mode="json") for r in results]).decode())
        return 0
    if args.action == "schedule":
        scheduler = build_factor_tracking_scheduler(runner)
        try:
            scheduler.start()
        finally:
            if scheduler.running:
                scheduler.shutdown(wait=True)
        return 0
    if args.action == "snapshot":
        from rquant.factor.tracking_serving import (
            project_factor_tracking_projections,
            project_factor_tracking_snapshot,
        )

        with open_factor_run_configuration(root, reference) as loaded:
            snapshot = project_factor_tracking_snapshot(
                identity,
                registry_identity=loaded.configuration.registry_identity,
                available_at=datetime.now(UTC),
            )
        print(
            canonical_json_bytes(
                [p.model_dump(mode="json") for p in project_factor_tracking_projections(snapshot)]
            ).decode()
        )
        return 0
    if args.control_root is None or args.socket is None:
        parser.error("私有监听必须提供显式控制根及socket")
    from rquant.factor.tracking_backend import FactorTrackingPageControlBackend
    from rquant.factor_tracking_admission import (
        FactorTrackingAdmission,
        build_factor_tracking_admission_server,
    )
    from rquant.page_control import PageControlConsumer, PageControlOutbox, PageControlService

    control = _root_path(args.control_root)
    descriptor = _open_private_root(control)
    os.close(descriptor)
    users = frozenset(args.tracking_users.split(",")) if args.tracking_users else frozenset()
    backend = FactorTrackingPageControlBackend(
        root, reference, identity, enabled=args.enabled, tracking_users=users
    )
    outbox = PageControlOutbox(control / "page-control.sqlite")
    service = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox, data_dir=control, log_dir=control, factor_tracking_backend=backend
        ),
    )
    admission = FactorTrackingAdmission(service, enabled=args.enabled, tracking_users=users)
    server = build_factor_tracking_admission_server(
        admission, socket_path=args.socket, trusted_web_uid=args.web_uid, shared_gid=args.shared_gid
    )
    if server is None:
        raise PermissionError("跟踪入口尚未开放")
    try:
        server.serve_forever(poll_interval=0.2)
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
