"""Explicit local file configuration and one worker/private listener entry.

This command neither loads dotenv nor prepares a live source or initializes a
factor registry/ledger. Their captured ready identities are required inputs.
"""

from __future__ import annotations

import argparse
import os
from datetime import UTC, datetime
from pathlib import Path

from pydantic import BaseModel

from rquant.factor.member_archive import _read_file
from rquant.factor.result_artifact import _open_private_root, _root_path
from rquant.factor.run_configuration import (
    FactorRunConfiguration,
    FactorRunFileReference,
    open_factor_run_configuration,
    run_configured_factor_worker,
    save_factor_prepared_source,
    save_factor_run_configuration,
)
from rquant.factor.source_prepare import FactorPreparedStreamSource
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads


def _input(path: Path, model: type[BaseModel], limit: int) -> BaseModel:
    path = _root_path(path)
    descriptor = _open_private_root(path.parent)
    try:
        data, _ = _read_file(descriptor, path.name, limit)
        strict_canonical_json_loads(data)
        return model.model_validate_json(data)
    finally:
        os.close(descriptor)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="使用显式冻结配置的离线因子入口")
    parser.add_argument("action", choices=("save-source", "save-configuration", "worker", "serve"))
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--input", type=Path)
    parser.add_argument("--reference", help="保存入口返回的完整配置引用 JSON")
    parser.add_argument("--control-root", type=Path)
    parser.add_argument("--socket", type=Path)
    parser.add_argument("--web-uid", type=int)
    parser.add_argument("--shared-gid", type=int)
    args = parser.parse_args(argv)
    if args.action in ("save-source", "save-configuration"):
        if args.input is None:
            parser.error("保存必须提供实际输入文件")
        if args.action == "save-source":
            source = _input(args.input, FactorPreparedStreamSource, 16 * 1024 * 1024)
            result = save_factor_prepared_source(args.root, source)
        else:
            configuration = _input(args.input, FactorRunConfiguration, 256 * 1024)
            result = save_factor_run_configuration(args.root, configuration)
            with open_factor_run_configuration(args.root, result) as loaded:
                from rquant.factor.registry import FactorDefinitionRegistry

                config = loaded.configuration
                FactorDefinitionRegistry(Path(config.registry_identity.path)).get_head(
                    "configuration_probe",
                    expected_identity=config.registry_identity,
                )
                loaded.open_ledger(clock=lambda: datetime.now(UTC))
        print(canonical_json_bytes(result.model_dump(mode="json")).decode())
        return 0
    if args.reference is None:
        parser.error("必须提供保存入口返回的完整配置引用")
    reference = FactorRunFileReference.model_validate_json(args.reference)
    if args.action == "worker":
        result = run_configured_factor_worker(args.root, reference)
        print(result.model_dump_json())
        return 0
    if args.control_root is None or args.socket is None:
        parser.error("私有监听必须提供显式控制根及 socket")
    from rquant.factor.run_backend import FactorRunPageControlBackend
    from rquant.factor_run_admission import FactorRunAdmission, build_factor_run_admission_server
    from rquant.page_control import PageControlConsumer, PageControlOutbox, PageControlService

    root = _root_path(args.control_root)
    descriptor = _open_private_root(root)
    os.close(descriptor)
    backend = FactorRunPageControlBackend(args.root, reference)
    outbox = PageControlOutbox(root / "page-control.sqlite")
    consumer = PageControlConsumer(
        outbox=outbox, data_dir=root, log_dir=root, factor_run_backend=backend
    )
    admission = FactorRunAdmission(
        PageControlService(outbox=outbox, consumer=consumer),
        run_users=frozenset(backend.run_users),
        enabled=backend.enabled,
    )
    server = build_factor_run_admission_server(
        admission, socket_path=args.socket, trusted_web_uid=args.web_uid, shared_gid=args.shared_gid
    )
    if server is None:
        raise PermissionError("运行入口尚未开启")
    try:
        server.serve_forever(poll_interval=0.2)
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
